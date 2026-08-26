//! Aligned catalog integration.
//!
//! `ATTACH '<root>' AS name (TYPE ALIGNED)` creates an AlignedCatalog over the
//! parquet column-group data root. Tables are LOGICAL: reads go straight to
//! the parquet files via the aligned_scan scan; nothing is materialized.
//! DML (INSERT/UPDATE/DELETE) is not supported — use COPY TO (FORMAT aligned)
//! for bulk writes and COPY TO (FORMAT aligned, MERGE true) for incremental updates.

#include "catalog/aligned_catalog.hpp"

#include "catalog/manifest.hpp"
#include "catalog/aligned_create.hpp"
#include "catalog/write_lock.hpp"
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
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/storage/database_size.hpp"
#include "scan/aligned_scan.hpp"
#include "resolver/partition_resolver.hpp"
#include "transaction/aligned_transaction.hpp"

namespace duckdb {

//! BindInfo hook for LogicalGet::GetTable(): reports the owning catalog entry
//! so the binder treats the scan as a real base table (used by AddBaseTable,
//! cardinality estimation, and metadata queries like duckdb_tables()).
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

	// Add the virtual partition column to the scan bind data (but NOT to the
	// catalog entry's column schema, which is used for binder column resolution).
	auto &scan_bind = bind_data->Cast<AlignedTableBindData>();
	if (!scan_bind.plan.groups.empty()) {
		auto &index_group = scan_bind.plan.groups[0];
		if (!index_group.manifest.partitioning.empty()) {
			auto &tmpl = index_group.manifest.partitioning[0].template_str;
			string part_col = PartitionColumnName(tmpl);
			if (!part_col.empty()) {
				bool collision = false;
				for (auto &n : scan_bind.names) {
					if (StringUtil::CIEquals(n, part_col)) {
						collision = true;
						break;
					}
				}
				if (!collision) {
					scan_bind.partition_col_name = part_col;
					scan_bind.partition_col_idx = scan_bind.names.size();
					scan_bind.names.push_back(part_col);
					scan_bind.types.push_back(LogicalType::VARCHAR);
					types.push_back(LogicalType::VARCHAR);
					names.push_back(part_col);
				}
			}
		}
	}

	// Same shape/flags as the registered aligned_scan function: projection &
	// filter pushdown reach the scan through these flags.
	TableFunction fn("aligned_scan", {LogicalType::VARCHAR}, AlignedScanFunction, nullptr, AlignedInitGlobal,
	                 AlignedInitLocal);
	fn.named_parameters["root"] = LogicalType::VARCHAR;
	fn.cardinality = AlignedCardinality;
	fn.projection_pushdown = true;
	fn.filter_pushdown = true;
	fn.filter_prune = true;
	// Lets the binder recognize this GET as a real base table (LogicalGet::GetTable).
	bind_data->Cast<AlignedTableBindData>().catalog_entry = this;
	fn.get_bind_info = AlignedScanGetBindInfo;
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
    : SchemaCatalogEntry(catalog, [&] {
	      CreateSchemaInfo info;
	      info.schema = schema_name;
	      return info;
      }()) {
}

void AlignedSchemaEntry::EnsureTablesLoaded(ClientContext &context) {
	std::lock_guard<std::mutex> lock(tables_mutex);
	if (tables_loaded) {
		return;
	}
	tables.clear(); // Clear stale entries before re-loading from disk.
	const string &root = catalog.Cast<AlignedCatalog>().GetRoot();

	auto fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(root)) {
		// Empty data root — no tables yet. CREATE TABLE will create the
		// directory structure on demand.
		tables_loaded = true;
		return;
	}
	vector<string> candidates;
	fs->ListFiles(root, [&](const string &fname, bool is_dir) {
		// Skip non-directories and hidden files (dot-prefixed). Tables
		// named with a leading '_' are valid — the '_' filter only applies
		// to _tmp/ directories inside a table directory, not to table
		// directories at the root level.
		if (!is_dir || fname.empty() || fname[0] == '.') {
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
		} catch (IOException &) {
			// Not a valid aligned table layout — skip it.
			// Only catch IOException (not a valid layout); let
			// InternalException / PermissionException / FatalException
			// propagate so real errors are not silently swallowed.
		}
	}
	// Mark loaded only after the full scan succeeds. If a non-IOException
	// propagates from the loop above, tables_loaded stays false so the next
	// EnsureTablesLoaded call will retry from scratch.
	tables_loaded = true;
}

void AlignedSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::INVALID) {
		return;
	}
	std::lock_guard<std::mutex> lock(tables_mutex);
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
		{
			std::lock_guard<std::mutex> lock(tables_mutex);
			auto it = tables.find(lookup_info.GetEntryName());
			if (it != tables.end()) {
				return optional_ptr<CatalogEntry>(it->second.get());
			}
		}
		// Table not found in the cached catalog. The on-disk state may have
		// changed since ATTACH (aligned_create created a new table, or
		// aligned_drop removed one). Check if the table directory exists
		// on disk and, if so, force a full catalog reload.
		auto &catalog_ref = catalog.Cast<AlignedCatalog>();
		const string &root = catalog_ref.GetRoot();
		auto fs = FileSystem::CreateLocal();
		string table_dir = root + "/" + lookup_info.GetEntryName();
		if (fs->DirectoryExists(table_dir)) {
			// Force a full reload on the next EnsureTablesLoaded call.
			// Set the flag inside the lock to avoid a data race with
			// concurrent EnsureTablesLoaded / Scan calls.
			{
				std::lock_guard<std::mutex> flag_lock(tables_mutex);
				tables_loaded = false;
			}
			if (transaction.HasContext()) {
				EnsureTablesLoaded(transaction.GetContext());
			}
			std::lock_guard<std::mutex> lock(tables_mutex);
			auto it2 = tables.find(lookup_info.GetEntryName());
			if (it2 != tables.end()) {
				return optional_ptr<CatalogEntry>(it2->second.get());
			}
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
	// DuckDB's parser stores each option as a ParsedExpression. For string
	// constants, the expression is a ConstantExpression wrapping a Value.
	// We extract the Value directly, handling quoting/escaping correctly.
	// Fallback to expr->ToString() for non-constant expressions (rare).
	auto get_option_value = [](const unique_ptr<ParsedExpression> &expr) -> string {
		if (!expr) {
			return "";
		}
		if (expr->GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &const_expr = expr->Cast<ConstantExpression>();
			if (const_expr.value.IsNull()) {
				return "";
			}
			return const_expr.value.ToString();
		}
		// Fallback: strip surrounding single quotes from ToString().
		auto s = expr->ToString();
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
			auto value = get_option_value(expr);
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
	auto &fs = FileSystem::GetFileSystem(transaction.GetContext());
	string table_dir = root + "/" + base.table;

	// Acquire write lock for mutual exclusion with concurrent writers.
	// Only lock if the table directory already exists (new table creation
	// has no concurrent writers to exclude — the table doesn't exist yet).
	// Locking a non-existent table creates an empty directory, which breaks
	// EnsureTablesLoaded when CreateTable throws (e.g. invalid columns).
	unique_ptr<TableWriteLock> write_lock;
	if (fs.DirectoryExists(table_dir)) {
		write_lock = make_uniq<TableWriteLock>(fs, table_dir);
	}

	if (!partition_key.empty()) {
		AlignedCreatePartition(transaction.GetContext(), root, base.table, partition_key);
	} else {
		vector<ColumnDefinition> cols;
		for (auto &col : columns.Logical()) {
			cols.push_back(col.Copy());
		}
		AlignedCreateTable(transaction.GetContext(), root, base.table, cols,
		                   groups_option, partition_template_option);
	}
	write_lock.reset(); // release lock before EnsureTablesLoaded

	// Force re-discovery of tables (the new table is now on disk)
	{
		std::lock_guard<std::mutex> lock(tables_mutex);
		tables_loaded = false;
	}
	EnsureTablesLoaded(transaction.GetContext());

	std::lock_guard<std::mutex> lock(tables_mutex);
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
	throw NotImplementedException("aligned attach: INSERT is not supported. Use COPY TO (FORMAT aligned) for writes.");
}

PhysicalOperator &AlignedCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException("aligned attach: DELETE is not supported.");
}

PhysicalOperator &AlignedCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalUpdate &op, PhysicalOperator &plan) {
	throw NotImplementedException("aligned attach: UPDATE is not supported. Use COPY TO (FORMAT aligned, MERGE true) for updates.");
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






