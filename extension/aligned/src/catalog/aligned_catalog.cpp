//! DuckLake-style catalog integration (Phase 8).
//!
//! `ATTACH '<root>' AS name (TYPE ALIGNED)` creates an AlignedCatalog over the
//! parquet column-group data root. Tables are LOGICAL: reads go straight to
//! the parquet files via the aligned_table scan; nothing is materialized.
//! Standard DML routes through the catalog's PlanInsert/PlanDelete/PlanUpdate
//! hooks (the same mechanism DuckLake uses).

#include "catalog/aligned_catalog.hpp"

#include "catalog/manifest.hpp"
#include "catalog/aligned_create.hpp"
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

//! BindInfo hook for LogicalGet::GetTable(): reports the owning catalog entry
//! so DELETE/UPDATE recognize the scan as a base table.
static BindInfo AlignedScanGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &abd = bind_data->Cast<AlignedTableBindData>();
	return BindInfo(*abd.catalog_entry);
}
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
	// Lets DELETE/UPDATE binders recognize this GET as a base table (LogicalGet::GetTable).
	bind_data->Cast<AlignedTableBindData>().catalog_entry = this;
	fn.get_bind_info = AlignedScanGetBindInfo;
	return fn;
}

void AlignedTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                              LogicalUpdate &update, ClientContext &context) {
	// Default: no extra columns. Only the columns the UPDATE statement touches
	// are fetched; the rowid identifies the target rows. The aligned UPDATE
	// pipeline resolves rowid -> (date, symbol) keys and only writes the SET
	// columns back (columns absent from an old part due to schema evolution
	// must NOT be force-updated — that would fail the v7 contract).
	TableCatalogEntry::BindUpdateConstraints(binder, get, proj, update, context);
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
	tables.clear(); // Clear stale entries before re-loading from disk.
	const string &root = catalog.Cast<AlignedCatalog>().GetRoot();

	auto fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(root)) {
		// Empty data root — no tables yet. CREATE TABLE will create the
		// directory structure on demand.
		return;
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
		// Return nullptr instead of throwing — DuckDB calls LookupEntry to check
		// for conflicts during CREATE TABLE and expects nullptr for "not found".
		return nullptr;
	}
	// Non-table entries are not supported.
	return nullptr;
}

optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
	ALIGNED_DDL_UNSUPPORTED("CREATE INDEX")
}

optional_ptr<CatalogEntry> AlignedSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                           BoundCreateTableInfo &info) {
	auto &base = info.Base();
	auto &columns = base.columns;

	// Parse options from WITH (key=value, ...).
	// DuckDB's parser stores each option as a ParsedExpression; for string
	// constants, expr->ToString() returns the value wrapped in single quotes.
	// We strip a single layer of surrounding single quotes (the common case).
	// A more robust approach (ExpressionExecutor::EvaluateScalar) is possible
	// but requires a ClientContext; this is sufficient for DDL string options.
	auto strip_quotes = [](const string &s) -> string {
		if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
			return s.substr(1, s.size() - 2);
		}
		return s;
	};

	string groups_option;
	string partition_template_option;
	string partition_key;

	for (auto &opt : base.options) {
		auto &key = opt.first;
		auto &expr = opt.second;
		if (expr) {
			auto value = strip_quotes(expr->ToString());
			if (StringUtil::CIEquals(key, "groups")) {
				groups_option = value;
			} else if (StringUtil::CIEquals(key, "partition_template")) {
				partition_template_option = value;
			} else if (StringUtil::CIEquals(key, "partition")) {
				partition_key = value;
			}
		}
	}

	const string &root = catalog.Cast<AlignedCatalog>().GetRoot();

	if (!partition_key.empty()) {
		// Partition creation mode: table must already exist
		AlignedCreatePartition(transaction.GetContext(), root, base.table, partition_key);
	} else {
		// Table creation mode
		vector<ColumnDefinition> cols;
		for (auto &col : columns.Logical()) {
			cols.push_back(col.Copy());
		}
		AlignedCreateTable(transaction.GetContext(), root, base.table, cols,
		                   groups_option, partition_template_option);
	}

	// Force re-discovery of tables (the new table is now on disk)
	tables_loaded = false;
	EnsureTablesLoaded(transaction.GetContext());

	auto it = tables.find(base.table);
	if (it == tables.end()) {
		throw CatalogException("aligned CREATE TABLE: table '%s' was created but could not be loaded",
		                        base.table);
	}
	return optional_ptr<CatalogEntry>(it->second.get());
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
}

#undef ALIGNED_DDL_UNSUPPORTED

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

ErrorData AlignedCatalog::SupportsCreateTable(BoundCreateTableInfo &info) {
	// Allow WITH clause (options) for CREATE TABLE — our CreateTable
	// implementation parses "groups", "partition_template", and "partition"
	// options. All other base checks (partition_keys, sort_keys) still apply.
	auto &base = info.Base().Cast<CreateTableInfo>();
	if (!base.partition_keys.empty()) {
		return ErrorData(ExceptionType::CATALOG,
		                 StringUtil::Format("PARTITIONED BY is not supported for tables in a %s catalog",
		                                     GetCatalogType()));
	}
	if (!base.sort_keys.empty()) {
		return ErrorData(ExceptionType::CATALOG,
		                 StringUtil::Format("SORTED BY is not supported for tables in a %s catalog",
		                                     GetCatalogType()));
	}
	return ErrorData();
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
	auto &entry = op.table.Cast<AlignedTableEntry>();
	auto &del = planner.Make<PhysicalAlignedDelete>(op.types, entry.name, entry.GetRoot(),
	                                                op.estimated_cardinality);
	del.children.push_back(plan);
	return del;
}

PhysicalOperator &AlignedCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalUpdate &op, PhysicalOperator &plan) {
	auto &entry = op.table.Cast<AlignedTableEntry>();
	// Per SET column: table column name and the group that owns it (from the
	// table plan's group schemas). Used to stage [date, symbol, set...] and to
	// build the upsert mapping.
	vector<string> set_names;
	vector<string> set_groups;
	vector<LogicalType> ignore_types;
	vector<string> ignore_names;
	auto bind_data = AlignedBindForCatalog(context, entry.GetRoot(), entry.name, ignore_types, ignore_names);
	auto &bind_plan = bind_data->Cast<AlignedTableBindData>().plan;
	for (auto &cidx : op.columns) {
		auto name = entry.GetColumns().GetColumn(cidx).Name();
		set_names.push_back(name);
		string grp;
		for (auto &g : bind_plan.groups) {
			for (auto &cn : g.column_order) {
				if (StringUtil::CIEquals(cn, name)) {
					grp = g.manifest.group;
					break;
				}
			}
			if (!grp.empty()) {
				break;
			}
		}
		set_groups.push_back(grp);
	}
	auto &upd = planner.Make<PhysicalAlignedUpdate>(op.types, std::move(set_names), std::move(set_groups), entry.name,
	                                                entry.GetRoot(), op.estimated_cardinality)
	                 .Cast<PhysicalAlignedUpdate>();
	upd.expressions = std::move(op.expressions);
	upd.children.push_back(plan);
	return upd;
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






