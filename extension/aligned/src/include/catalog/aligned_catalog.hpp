#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class AlignedTableEntry;
struct AlignedTableBindData;

//! A logical table of the aligned attached database. Reads go straight to the
//! parquet column groups via the aligned_table scan 鈥?nothing is materialized.
class AlignedTableEntry : public TableCatalogEntry {
public:
	AlignedTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, string root_p);

	//! Scan function = aligned_table(root, table); bind data is pre-built.
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	const string &GetRoot() const {
		return root;
	}

private:
	string root;
};

//! The single schema ("main") of an aligned attached database.
class AlignedSchemaEntry : public SchemaCatalogEntry {
public:
	AlignedSchemaEntry(Catalog &catalog, const string &name);

	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(ClientContext &context, CatalogType type,
	          const std::function<void(CatalogEntry &)> &callback) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction,
	                                       const EntryLookupInfo &lookup_info) override;

	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
	//! Builds (or returns cached) table entries from the data root layout.
	void EnsureTablesLoaded(ClientContext &context);
	case_insensitive_map_t<unique_ptr<CatalogEntry>> tables;
	bool tables_loaded = false;
};

//! DuckLake-style catalog over a parquet column-group data root.
class AlignedCatalog : public Catalog {
public:
	explicit AlignedCatalog(AttachedDatabase &db, string root_p);

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override;
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
	                                              const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    LogicalCreateTable &op, PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	const string &GetRoot() const {
		return root;
	}

private:
	string root;
	unique_ptr<AlignedSchemaEntry> main_schema;
};

//! Registers the storage extension so `ATTACH '<root>' AS name (TYPE ALIGNED)`
//! creates an AlignedCatalog. Reads hit the parquet column groups directly.
void RegisterAlignedStorageExtension(DatabaseInstance &db);

} // namespace duckdb

