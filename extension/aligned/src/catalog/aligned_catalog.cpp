//! DuckLake-style catalog integration (Phase 8).
//!
//! `ATTACH '<root>' AS name (TYPE ALIGNED)` creates an AlignedCatalog over the
//! parquet column-group data root. Tables are LOGICAL: reads go straight to
//! the parquet files via the aligned_table scan; nothing is materialized.
//! Standard DML routes through the catalog's PlanInsert/PlanDelete/PlanUpdate
//! hooks (the same mechanism DuckLake uses).

#include "catalog/aligned_catalog.hpp"

#include "catalog/manifest.hpp"
#include "execution/aligned_dml.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/storage/database_size.hpp"
#include "scan/aligned_scan.hpp"
#include "transaction/aligned_transaction.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// AlignedTableEntry
//===----------------------------------------------------------------------===//
AlignedTableEntry::AlignedTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     string root_p)
    : TableCatalogEntry(catalog, schema, info), root(std::move(root_p)) {
}

TableFunction AlignedTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	vector<LogicalType> types;
	vector<string> names;
	bind_data = AlignedBindForCatalog(context, root, name, types, names);

	// Same shape/flags as the registered aligned_table function: projection &
	// filter pushdown reach the scan through these flags.
	TableFunction fn("aligned_table", {LogicalType::VARCHAR}, AlignedScanFunction, nullptr, AlignedInitGlobal,
	                 AlignedInitLocal);
	fn.named_parameters["root"] = LogicalType::VARCHAR;
	fn.cardinality = AlignedCardinality;
	fn.projection_pushdown = true;
	fn.filter_pushdown = true;
	fn.filter_prune = true;
	return fn;
}

unique_ptr<BaseStatistics> AlignedTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableStorageInfo AlignedTableEntry::GetStorageInfo(ClientContext &context) {
	return TableStorageInfo();
}

//===----------------------------------------------------------------------===//
// AlignedSchemaEntry
//===----------------------------------------------------------------------===//
AlignedSchemaEntry::AlignedSchemaEntry(Catalog &catalog, const string &schema_name)
    : SchemaCatalogEntry(catalog, [] {
	      CreateSchemaInfo info;
	      info.schema = "main";
	      return info;
      }()) {
	(void)schema_name;
}

void AlignedSchemaEntry::EnsureTablesLoaded(ClientContext &context) {
	if (tables_loaded) {
		return;
	}
	tables_loaded = true;
	const string &root = catalog.Cast<AlignedCatalog>().GetRoot();

	auto fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(root)) {
		throw IOException("aligned attach: data root does not exist: '%s'", root);
	}
	vector<string> candidates;
	fs->ListFiles(root, [&](const string &fname, bool is_dir) {
		if (!is_dir || fname.empty() || fname[0] == '.' || fname[0] == '_') {
			return;
		}
		candidates.push_back(fname);
	});
	sort(candidates.begin(), candidates.end());

	for (auto &tbl : candidates) {
		try {
			vector<LogicalType> types;
			vector<string> col_names;
			auto bind_data = AlignedBindForCatalog(context, root, tbl, types, col_names);
			(void)bind_data;

			CreateTableInfo info;
			info.schema = name;
			info.table = tbl;
			for (idx_t i = 0; i < col_names.size(); i++) {
				info.columns.AddColumn(ColumnDefinition(col_names[i], types[i]));
			}
			auto entry = make_uniq<AlignedTableEntry>(catalog, *this, info, root);
			tables.insert(make_pair(tbl, std::move(entry)));
		} catch (std::exception &) {
			// Not a valid aligned table layout 鈥?skip it.
		}
	}
}

void AlignedSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::INVALID) {
		return;
	}
	for (auto &entry : tables) {
		callback(*entry.second);
	}
}

void AlignedSchemaEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry &)> &callback) {
	EnsureTablesLoaded(context);
	Scan(type, callback);
}

#define ALIGNED_DDL_UNSUPPORTED(what)                                                                                  \
	throw NotImplementedException("aligned attach: " what " is not supported on logical parquet tables");

optional_ptr<CatalogEntry> AlignedSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                           const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() == CatalogType::TABLE_ENTRY) {
		if (transaction.HasContext()) {
			EnsureTablesLoaded(transaction.GetContext());
		}
		auto it = tables.find(lookup_info.GetEntryName());
		if (it != tables.end()) {
			return optional_ptr<CatalogEntry>(it->second.get());
		}
	}
	throw CatalogException("Table with name %s does not exist!", lookup_info.GetEntryName());
}

optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
	ALIGNED_DDL_UNSUPPORTED("CREATE INDEX")
}

optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                           BoundCreateTableInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("CREATE TABLE")
}
optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                              CreateFunctionInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("CREATE FUNCTION")
}
optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("CREATE VIEW")
}
optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                              CreateSequenceInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("CREATE SEQUENCE")
}
optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                   CreateTableFunctionInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("CREATE TYPE/FUNCTION")
}
optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                  CreateCopyFunctionInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("CREATE COPY FUNCTION")
}
optional_ptr<CatalogEntry> AlignedSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                    CreatePragmaFunctionInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("CREATE PRAGMA FUNCTION")
}
optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                               CreateCollationInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("CREATE COLLATION")
}
optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("CREATE TYPE")
}
void AlignedSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("DROP")
}
void AlignedSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	ALIGNED_DDL_UNSUPPORTED("ALTER")

#undef ALIGNED_DDL_UNSUPPORTED
}

//===----------------------------------------------------------------------===//
// AlignedCatalog
//===----------------------------------------------------------------------===//
AlignedCatalog::AlignedCatalog(AttachedDatabase &db, string root_p)
    : Catalog(db), root(std::move(root_p)), main_schema(make_uniq<AlignedSchemaEntry>(*this, "main")) {
}

void AlignedCatalog::Initialize(bool load_builtin) {
}

string AlignedCatalog::GetCatalogType() {
	return "aligned";
}

optional_ptr<CatalogEntry> AlignedCatalog::CreateSchema(CatalogTransaction transaction,
                                                              CreateSchemaInfo &info) {
	throw NotImplementedException("aligned attach: CREATE SCHEMA is not supported");
}

optional_ptr<SchemaCatalogEntry> AlignedCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
	if (StringUtil::CIEquals(schema_lookup.GetEntryName(), "main") || schema_lookup.GetEntryName().empty()) {
		return optional_ptr<SchemaCatalogEntry>(main_schema.get());
	}
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw CatalogException("Schema with name %s does not exist!", schema_lookup.GetEntryName());
}

void AlignedCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	callback(*main_schema);
}

void AlignedCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("aligned attach: DROP SCHEMA is not supported");
}

PhysicalOperator &AlignedCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("aligned attach: CREATE TABLE AS is not supported");
}

PhysicalOperator &AlignedCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	D_ASSERT(plan);
	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &entry = op.table.Cast<AlignedTableEntry>();
	vector<string> col_names;
	for (auto &col : entry.GetColumns().Physical()) {
		col_names.push_back(col.Name());
	}
	auto row_types = entry.GetTypes();
	auto &insert = planner.Make<PhysicalAlignedInsert>(op.types, std::move(row_types), std::move(col_names),
	                                                   entry.name, entry.GetRoot(), op.estimated_cardinality);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &AlignedCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException("aligned attach: DELETE is not implemented yet");
}

PhysicalOperator &AlignedCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalUpdate &op, PhysicalOperator &plan) {
	throw NotImplementedException("aligned attach: UPDATE is not implemented yet");
}

DatabaseSize AlignedCatalog::GetDatabaseSize(ClientContext &context) {
	return DatabaseSize();
}

bool AlignedCatalog::InMemory() {
	return false;
}

string AlignedCatalog::GetDBPath() {
	return root;
}

//===----------------------------------------------------------------------===//
// Storage extension registration
//===----------------------------------------------------------------------===//
static unique_ptr<Catalog> AlignedAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                         AttachedDatabase &db, const string &name, AttachInfo &info,
                                         AttachOptions &options) {
	string root = info.path;
	// Strip a leading "aligned:" scheme if present.
	if (StringUtil::StartsWith(StringUtil::Lower(root), "aligned:")) {
		root = root.substr(8);
	}
	return make_uniq<AlignedCatalog>(db, root);
}

static unique_ptr<TransactionManager>
AlignedCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                Catalog &catalog) {
	return make_uniq<AlignedTransactionManager>(db);
}

void RegisterAlignedStorageExtension(DatabaseInstance &db) {
	auto extension = make_shared_ptr<StorageExtension>();
	extension->attach = AlignedAttach;
	extension->create_transaction_manager = AlignedCreateTransactionManager;
	StorageExtension::Register(DBConfig::GetConfig(db), "ALIGNED", std::move(extension));
}

} // namespace duckdb



