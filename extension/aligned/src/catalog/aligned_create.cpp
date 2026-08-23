#include "catalog/aligned_create.hpp"
#include "catalog/manifest.hpp"
#include "resolver/partition_resolver.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/date.hpp"

#include "parquet_writer.hpp"
#include "io/parquet_io.hpp"


#include <map>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Helpers
//===----------------------------------------------------------------------===//

//! Validates a group name: "index" is valid as-is; all other groups must be
//! a two-level path "lv1/lv2" (exactly one slash, neither segment empty).
static void ValidateGroupName(const string &group_name) {
	if (StringUtil::CIEquals(group_name, "index")) {
		return;
	}
	auto slash = group_name.find('/');
	if (slash == string::npos || group_name.find('/', slash + 1) != string::npos ||
	    slash == 0 || slash + 1 >= group_name.size()) {
		throw BinderException("aligned CREATE TABLE: group name '%s' must be 'index' or a "
		                       "two-level path 'lv1/lv2' (e.g. 'factor/alpha101')", group_name);
	}
}

//! Parses a "group:col1,col2;group2:col3" mapping string (same syntax as
//! aligned_upsert mapping). Returns an ordered map: group_name -> column list.
//! The "index" group is always first if present.
static void ParseGroupsOption(const string &groups_str, case_insensitive_map_t<vector<string>> &out) {
	auto entries = StringUtil::Split(groups_str, ';');
	for (auto &entry : entries) {
		if (entry.empty()) {
			continue;
		}
		auto colon = entry.find(':');
		if (colon == string::npos) {
			throw BinderException("aligned CREATE TABLE: invalid groups entry '%s' "
			                       "(expected 'group:col1,col2')", entry);
		}
		string group_name = entry.substr(0, colon);
		StringUtil::Trim(group_name);
		auto cols_str = entry.substr(colon + 1);
		auto cols = StringUtil::Split(cols_str, ',');
		vector<string> col_names;
		for (auto &c : cols) {
			string trimmed = c;
			StringUtil::Trim(trimmed);
			if (!trimmed.empty()) {
				col_names.push_back(trimmed);
			}
		}
		if (group_name.empty() || col_names.empty()) {
			throw BinderException("aligned CREATE TABLE: invalid groups entry '%s' "
			                       "(expected 'group:col1,col2')", entry);
		}
		if (out.find(group_name) != out.end()) {
			throw BinderException("aligned CREATE TABLE: duplicate group '%s' in groups option", group_name);
		}
		ValidateGroupName(group_name);
		out[group_name] = std::move(col_names);
	}
}

//! Validates the primary key contract: first two columns must be
//! (symbol VARCHAR, date DATE/TIMESTAMP) or (date DATE/TIMESTAMP, symbol VARCHAR).
//! Returns {date_col_index, symbol_col_index}.
static pair<idx_t, idx_t> ValidatePrimaryKey(const vector<ColumnDefinition> &columns) {
	if (columns.size() < 2) {
		throw BinderException("aligned CREATE TABLE: at least 2 columns required "
		                       "(symbol VARCHAR, date DATE/TIMESTAMP as the first two columns)");
	}

	idx_t date_idx = DConstants::INVALID_INDEX;
	idx_t symbol_idx = DConstants::INVALID_INDEX;

	for (idx_t i = 0; i < 2; i++) {
		auto &col = columns[i];
		auto &type = col.Type();
		if (type.id() == LogicalTypeId::DATE || type.id() == LogicalTypeId::TIMESTAMP ||
		    type.id() == LogicalTypeId::TIMESTAMP_SEC || type.id() == LogicalTypeId::TIMESTAMP_MS ||
		    type.id() == LogicalTypeId::TIMESTAMP_NS) {
			if (date_idx != DConstants::INVALID_INDEX) {
				throw BinderException("aligned CREATE TABLE: first two columns must be exactly one "
				                       "DATE/TIMESTAMP and one VARCHAR (symbol) — found two date columns");
			}
			date_idx = i;
		} else if (type.id() == LogicalTypeId::VARCHAR) {
			if (symbol_idx != DConstants::INVALID_INDEX) {
				throw BinderException("aligned CREATE TABLE: first two columns must be exactly one "
				                       "DATE/TIMESTAMP and one VARCHAR (symbol) — found two VARCHAR columns");
			}
			symbol_idx = i;
		} else {
			throw BinderException("aligned CREATE TABLE: first two columns must be (symbol VARCHAR, "
			                       "date DATE/TIMESTAMP) — column '%s' has type %s",
			                       col.Name(), type.ToString());
		}
	}

	if (date_idx == DConstants::INVALID_INDEX) {
		throw BinderException("aligned CREATE TABLE: first two columns must include a DATE/TIMESTAMP "
		                       "column (the partition source)");
	}
	if (symbol_idx == DConstants::INVALID_INDEX) {
		throw BinderException("aligned CREATE TABLE: first two columns must include a VARCHAR column "
		                       "(the symbol column)");
	}

	return {date_idx, symbol_idx};
}

//! Helper: resolve a group's column names to (names, types) using a column
//! name → index map. Throws on unknown column. Shared by both DDL operations.
static void ResolveGroupColumns(const vector<ColumnDefinition> &columns,
                                const case_insensitive_map_t<idx_t> &col_index,
                                const vector<string> &group_cols,
                                vector<string> &out_names,
                                vector<LogicalType> &out_types,
                                const string &group_name_for_error) {
	for (auto &cn : group_cols) {
		auto it = col_index.find(cn);
		if (it == col_index.end()) {
			throw BinderException("aligned CREATE TABLE: groups option references unknown "
			                       "column '%s' in group '%s'", cn, group_name_for_error);
		}
		out_names.push_back(columns[it->second].Name());
		out_types.push_back(columns[it->second].Type());
	}
}

//===----------------------------------------------------------------------===//
// ExtendTableWithGroup — add new column groups to an existing table
//===----------------------------------------------------------------------===//

static void ExtendTableWithGroup(ClientContext &context, FileSystem &fs, const string &root,
                                 const string &table_name, const string &table_dir,
                                 const vector<ColumnDefinition> &columns,
                                 case_insensitive_map_t<vector<string>> &groups_map,
                                 case_insensitive_map_t<idx_t> &col_index) {
	// The groups option must specify at least one new group that is NOT "index".
	bool has_non_index = false;
	for (auto &kv : groups_map) {
		if (!StringUtil::CIEquals(kv.first, "index")) {
			has_non_index = true;
			break;
		}
	}
	if (!has_non_index) {
		throw BinderException("aligned CREATE TABLE: table '%s' already exists at '%s'. "
		                       "To add a column group, specify a non-index group in WITH (groups = '...').",
		                       table_name, table_dir);
	}

	// Discover the existing table's plan (groups + partitions).
	TablePlan plan;
	BuildTablePlan(context, root, table_name, plan);

	// Collect existing group names (case-insensitive).
	case_insensitive_set_t existing_groups;
	for (auto &g : plan.groups) {
		existing_groups.insert(g.manifest.group);
	}

	// Collect existing partitions with their row counts (from the index group).
	struct ExtPartInfo {
		string key;
		idx_t row_count;
	};
	vector<ExtPartInfo> existing_partitions;
	for (auto &p : plan.groups[0].partitions) {
		existing_partitions.push_back({p.key, p.row_count});
	}

	for (auto &kv : groups_map) {
		const string &group_name = kv.first;
		auto &group_cols = kv.second;

		if (existing_groups.count(group_name)) {
			throw BinderException("aligned CREATE TABLE: group '%s' already exists in table '%s'",
			                       group_name, table_name);
		}

		vector<string> col_names;
		vector<LogicalType> col_types;
		ResolveGroupColumns(columns, col_index, group_cols, col_names, col_types, group_name);

		string group_dir = table_dir + "/" + group_name;
		fs.CreateDirectoriesRecursive(group_dir);

		for (auto &part : existing_partitions) {
			string part_dir = group_dir + "/" + part.key;
			fs.CreateDirectoriesRecursive(part_dir);
			string parquet_path = part_dir + "/" + FormatPartName(0, part.row_count);
			WriteNullParquet(context, fs, parquet_path, col_names, col_types, part.row_count);
		}
	}
}

//===----------------------------------------------------------------------===//
// CreateNewTable — create a new table with column groups + default partition
//===----------------------------------------------------------------------===//

static void CreateNewTable(ClientContext &context, FileSystem &fs, const string &table_dir,
                           const vector<ColumnDefinition> &columns,
                           case_insensitive_map_t<vector<string>> &groups_map,
                           case_insensitive_map_t<idx_t> &col_index,
                           const string &partition_template_option) {
	auto pk = ValidatePrimaryKey(columns);
	idx_t date_idx = pk.first;
	idx_t symbol_idx = pk.second;

	string partition_template = partition_template_option;
	if (partition_template.empty()) {
		partition_template = "month=%Y-%m";
	}

	if (!IsKnownTemplate(partition_template)) {
		throw BinderException("aligned CREATE TABLE: invalid partition_template '%s' "
		                       "(expected 'date=%%Y-%%m-%%d', 'month=%%Y-%%m', or 'year=%%Y')",
		                       partition_template);
	}

	// Ensure index group exists.
	if (groups_map.find("index") == groups_map.end()) {
		case_insensitive_set_t assigned_cols;
		for (auto &kv : groups_map) {
			if (StringUtil::CIEquals(kv.first, "index")) {
				continue;
			}
			for (auto &c : kv.second) {
				assigned_cols.insert(c);
			}
		}
		vector<string> index_cols;
		for (idx_t i = 0; i < columns.size(); i++) {
			if (assigned_cols.count(columns[i].Name()) == 0) {
				index_cols.push_back(columns[i].Name());
			}
		}
		groups_map["index"] = std::move(index_cols);
	} else {
		auto &index_cols = groups_map["index"];
		auto has_col = [&](const string &name) {
			for (auto &c : index_cols) {
				if (StringUtil::CIEquals(c, name)) return true;
			}
			return false;
		};
		if (!has_col(columns[date_idx].Name())) {
			index_cols.insert(index_cols.begin(), columns[date_idx].Name());
		}
		if (!has_col(columns[symbol_idx].Name())) {
			index_cols.insert(index_cols.begin(), columns[symbol_idx].Name());
		}
	}

	fs.CreateDirectoriesRecursive(table_dir);

	string default_partition_key = DefaultPartitionKey(partition_template);
	const string placeholder_name = FormatPartName(0, 0);

	for (auto &kv : groups_map) {
		const string &group_name = kv.first;
		auto &group_cols = kv.second;

		vector<string> col_names;
		vector<LogicalType> col_types;
		ResolveGroupColumns(columns, col_index, group_cols, col_names, col_types, group_name);

		string group_dir = table_dir + "/" + group_name;
		string part_dir = group_dir + "/" + default_partition_key;
		fs.CreateDirectoriesRecursive(part_dir);

		string parquet_path = part_dir + "/" + placeholder_name;
		WriteEmptyParquet(context, fs, parquet_path, col_names, col_types);
	}
}

//===----------------------------------------------------------------------===//
// AlignedCreateTable — dispatches to CreateNewTable or ExtendTableWithGroup
//===----------------------------------------------------------------------===//

void AlignedCreateTable(ClientContext &context, const string &root, const string &table_name,
                        const vector<ColumnDefinition> &columns,
                        const string &groups_option, const string &partition_template_option) {
	auto &fs = FileSystem::GetFileSystem(context);
	string table_dir = root + "/" + table_name;

	// Check if the table already exists (has committed parquet files).
	bool table_exists = false;
	if (fs.DirectoryExists(table_dir)) {
		auto parts = fs.GlobFiles(table_dir + "/**/*.parquet", FileGlobOptions::ALLOW_EMPTY);
		for (auto &p : parts) {
			if (p.path.find("/_tmp/") == string::npos) {
				table_exists = true;
				break;
			}
		}
	}

	// Parse groups option first — needed to determine if this is a new-table or
	// an extend-table (add column group) operation.
	case_insensitive_map_t<vector<string>> groups_map;
	if (!groups_option.empty()) {
		ParseGroupsOption(groups_option, groups_map);
	}

	// Build a case-insensitive column name → index map for O(1) lookups.
	case_insensitive_map_t<idx_t> col_index;
	for (idx_t i = 0; i < columns.size(); i++) {
		col_index[columns[i].Name()] = i;
	}

	if (table_exists) {
		ExtendTableWithGroup(context, fs, root, table_name, table_dir, columns, groups_map, col_index);
	} else {
		CreateNewTable(context, fs, table_dir, columns, groups_map, col_index, partition_template_option);
	}
}

//===----------------------------------------------------------------------===//
// AlignedCreatePartition
//===----------------------------------------------------------------------===//

void AlignedCreatePartition(ClientContext &context, const string &root, const string &table_name,
                             const string &partition_key) {
	auto &fs = FileSystem::GetFileSystem(context);
	string table_dir = root + "/" + table_name;

	// Table must exist
	if (!fs.DirectoryExists(table_dir)) {
		throw BinderException("aligned CREATE TABLE (partition): table '%s' does not exist at '%s'",
		                       table_name, table_dir);
	}

	// Discover groups via glob
	auto parts = fs.GlobFiles(table_dir + "/**/*.parquet", FileGlobOptions::DISALLOW_EMPTY);
	if (parts.empty()) {
		throw BinderException("aligned CREATE TABLE (partition): table '%s' has no parts — "
		                       "cannot create a partition on an empty table", table_name);
	}

	// Build the table plan to discover groups + their schemas
	TablePlan plan;
	BuildTablePlan(context, root, table_name, plan);

	// Determine the partition template from the index group
	auto &index_group = plan.groups[0];
	string template_str;
	if (!index_group.manifest.partitioning.empty()) {
		template_str = index_group.manifest.partitioning[0].template_str;
	} else {
		template_str = "month=%Y-%m";
	}

	// Validate the partition key matches the template
	ValidatePartitionKey(partition_key, template_str);

	// Check the partition doesn't already exist. If the index group already
	// has it, the partition was already created (all groups should too, by
	// the partition-aligned contract). Skip groups that already have it.
	bool partition_exists_in_index = false;
	for (auto &part : plan.groups[0].parts) {
		if (part.partition_key == partition_key) {
			partition_exists_in_index = true;
			break;
		}
	}
	if (partition_exists_in_index) {
		throw BinderException("aligned CREATE TABLE (partition): partition '%s' already exists",
		                       partition_key);
	}

	// For each group, write a placeholder parquet in the new partition.
	// Skip groups that somehow already have this partition (defensive — should
	// not happen if the partition-aligned contract holds).
	const string placeholder_name = FormatPartName(0, 0);
	for (auto &group : plan.groups) {
		bool group_has_partition = false;
		for (auto &part : group.parts) {
			if (part.partition_key == partition_key) {
				group_has_partition = true;
				break;
			}
		}
		if (group_has_partition) {
			continue;
		}

		// Use the group's existing column schema
		vector<string> col_names = group.column_order;
		vector<LogicalType> col_types = group.schema_types;

		// Create partition directory
		string part_dir = group.group_path + "/" + partition_key;
		fs.CreateDirectoriesRecursive(part_dir);

		// Write placeholder parquet
		string parquet_path = part_dir + "/" + placeholder_name;
		WriteEmptyParquet(context, fs, parquet_path, col_names, col_types);
	}
}

} // namespace duckdb
