#include "catalog/aligned_create.hpp"
#include "catalog/manifest.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include "parquet_writer.hpp"
#include "parquet_field_id.hpp"
#include "parquet_shredding.hpp"
#include "zstd_file_system.hpp"

#include <map>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Helpers
//===----------------------------------------------------------------------===//

//! Writes a 0-row parquet file at `path` with the given column names + types.
//! The footer carries the schema so the reader can discover it.
static void WriteEmptyParquet(ClientContext &context, FileSystem &fs, const string &path,
                              const vector<string> &col_names, const vector<LogicalType> &col_types) {
	auto writer = make_uniq<ParquetWriter>(
	    context, fs, path, col_types, col_names,
	    duckdb_parquet::CompressionCodec::ZSTD, ChildFieldIDs(), ShreddingType(),
	    vector<pair<string, string>>(), nullptr, optional_idx(),
	    1073741824ULL /* MAX_UNCOMPRESSED_DICT_PAGE_SIZE */, 1, 0.01,
	    ZStdFileSystem::DefaultCompressionLevel(), ParquetVersion::V1, GeoParquetVersion::V1);

	// Flush an empty buffer (0 rows) then finalize. This writes the footer
	// with the full column schema and no data.
	auto buffer = make_uniq<ColumnDataCollection>(context, col_types);
	unique_ptr<ParquetWriteTransformData> transform;
	writer->Flush(*buffer, transform);
	writer->Finalize();
}

//! Writes a parquet file at `path` with `row_count` rows of all-NULL values.
//! Used when adding a new column group to an existing table: each existing
//! partition gets a placeholder with the same row count as the index partition
//! (all NULL for the new columns) to satisfy the partition-aligned contract.
static void WriteNullParquet(ClientContext &context, FileSystem &fs, const string &path,
                             const vector<string> &col_names, const vector<LogicalType> &col_types,
                             idx_t row_count) {
	auto writer = make_uniq<ParquetWriter>(
	    context, fs, path, col_types, col_names,
	    duckdb_parquet::CompressionCodec::ZSTD, ChildFieldIDs(), ShreddingType(),
	    vector<pair<string, string>>(), nullptr, optional_idx(),
	    1073741824ULL /* MAX_UNCOMPRESSED_DICT_PAGE_SIZE */, 1, 0.01,
	    ZStdFileSystem::DefaultCompressionLevel(), ParquetVersion::V1, GeoParquetVersion::V1);

	// Build a buffer of `row_count` all-NULL rows.
	auto buffer = make_uniq<ColumnDataCollection>(context, col_types);
	ColumnDataAppendState append_state;
	buffer->InitializeAppend(append_state);

	DataChunk chunk;
	chunk.Initialize(context, col_types);
	idx_t remaining = row_count;
	while (remaining > 0) {
		idx_t batch = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
		chunk.Reset();
		chunk.SetCardinality(batch);
		// All columns are NULL — set validity mask to all-invalid for each vector.
		for (idx_t c = 0; c < col_types.size(); c++) {
			auto &vec = chunk.data[c];
			FlatVector::Validity(vec).SetAllInvalid(batch);
		}
		buffer->Append(append_state, chunk);
		remaining -= batch;
	}

	unique_ptr<ParquetWriteTransformData> transform;
	writer->Flush(*buffer, transform);
	writer->Finalize();
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

//! Computes the default partition key from a template. For "month=%Y-%m" the
//! default is "month=1970-01" (epoch); for "date=%Y-%m-%d" it's "date=1970-01-01";
//! for "year=%Y" it's "year=1970".
static string DefaultPartitionKey(const string &template_str) {
	if (template_str.rfind("date=", 0) == 0) {
		return "date=1970-01-01";
	} else if (template_str.rfind("month=", 0) == 0) {
		return "month=1970-01";
	} else if (template_str.rfind("year=", 0) == 0) {
		return "year=1970";
	}
	throw BinderException("aligned CREATE TABLE: invalid partition_template '%s' "
	                       "(expected 'date=%%Y-%%m-%%d', 'month=%%Y-%%m', or 'year=%%Y')",
	                       template_str);
}

//! Validates that a partition key matches the given template kind.
static void ValidatePartitionKey(const string &key, const string &template_str) {
	if (template_str.rfind("date=", 0) == 0) {
		// Expect "date=YYYY-MM-DD" (15 chars)
		if (key.rfind("date=", 0) != 0 || key.size() != 15) {
			throw BinderException("aligned CREATE TABLE: partition key '%s' does not match "
			                       "template 'date=%%Y-%%m-%%d' (expected 'date=YYYY-MM-DD')", key);
		}
	} else if (template_str.rfind("month=", 0) == 0) {
		// Expect "month=YYYY-MM" (13 chars)
		if (key.rfind("month=", 0) != 0 || key.size() != 13) {
			throw BinderException("aligned CREATE TABLE: partition key '%s' does not match "
			                       "template 'month=%%Y-%%m' (expected 'month=YYYY-MM')", key);
		}
	} else if (template_str.rfind("year=", 0) == 0) {
		// Expect "year=YYYY" (9 chars)
		if (key.rfind("year=", 0) != 0 || key.size() != 9) {
			throw BinderException("aligned CREATE TABLE: partition key '%s' does not match "
			                       "template 'year=%%Y' (expected 'year=YYYY')", key);
		}
	} else {
		throw BinderException("aligned CREATE TABLE: invalid partition_template '%s'", template_str);
	}
}

//===----------------------------------------------------------------------===//
// AlignedCreateTable
//===----------------------------------------------------------------------===//

void AlignedCreateTable(ClientContext &context, const string &root, const string &table_name,
                        const vector<ColumnDefinition> &columns,
                        const string &groups_option, const string &partition_template_option) {
	auto &fs = FileSystem::GetFileSystem(context);
	string table_dir = root + "/" + table_name;

	// Check if the table already exists (has parquet files).
	bool table_exists = fs.DirectoryExists(table_dir) &&
	                    !fs.GlobFiles(table_dir + "/**/*.parquet", FileGlobOptions::ALLOW_EMPTY).empty();

	// Parse groups option first — needed to determine if this is a new-table or
	// an extend-table (add column group) operation.
	case_insensitive_map_t<vector<string>> groups_map;
	if (!groups_option.empty()) {
		ParseGroupsOption(groups_option, groups_map);
	}

	if (table_exists) {
		// --- Extend mode: add new column groups to an existing table ---
		// The groups option must specify at least one new group that is NOT "index"
		// (the index group already exists). The column definitions in CREATE TABLE
		// define the new group's columns; they do NOT need to include symbol/date.
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

		// Determine the partition template from the existing index group.
		string template_str;
		if (!plan.groups[0].manifest.partitioning.empty()) {
			template_str = plan.groups[0].manifest.partitioning[0].template_str;
		} else {
			template_str = "month=%Y-%m";
		}

		// Collect existing group names (case-insensitive).
		case_insensitive_set_t existing_groups;
		for (auto &g : plan.groups) {
			existing_groups.insert(g.manifest.group);
		}

		// Collect existing partitions with their row counts (from the index group).
		// The new group's placeholder must match the index partition's row count
		// (partition-aligned contract: shared partitions must agree on total rows).
		struct PartInfo {
			string key;
			idx_t row_count;
		};
		vector<PartInfo> existing_partitions;
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

			// Build column types/names for this new group.
			vector<string> col_names;
			vector<LogicalType> col_types;
			for (auto &cn : group_cols) {
				for (auto &col : columns) {
					if (StringUtil::CIEquals(col.Name(), cn)) {
						col_names.push_back(col.Name());
						col_types.push_back(col.Type());
						break;
					}
				}
			}
			if (col_names.empty()) {
				throw BinderException("aligned CREATE TABLE: group '%s' has no matching columns "
				                       "in the CREATE TABLE statement", group_name);
			}

			// Create the new group directory + a placeholder parquet in every
			// existing partition. Each placeholder has the same row count as the
			// index partition (all-NULL values) to satisfy the partition-aligned
			// contract.
			string group_dir = table_dir + "/" + group_name;
			fs.CreateDirectoriesRecursive(group_dir);

			for (auto &part : existing_partitions) {
				string part_dir = group_dir + "/" + part.key;
				fs.CreateDirectoriesRecursive(part_dir);
				// File name: 0000-{rows:10d}.parquet (matches partition row count)
				string part_name = StringUtil::Format("0000-%010llu.parquet",
				                                      (unsigned long long)part.row_count);
				string parquet_path = part_dir + "/" + part_name;
				WriteNullParquet(context, fs, parquet_path, col_names, col_types, part.row_count);
			}
		}
		return;
	}

	// --- New table mode ---
	// Validate primary key contract: first two columns must be
	// (symbol VARCHAR, date DATE/TIMESTAMP) or (date DATE/TIMESTAMP, symbol VARCHAR).
	auto pk = ValidatePrimaryKey(columns);
	idx_t date_idx = pk.first;
	idx_t symbol_idx = pk.second;

	// Determine partition template
	string partition_template = partition_template_option;
	if (partition_template.empty()) {
		partition_template = "month=%Y-%m";
	}

	// Validate template format
	if (partition_template.rfind("date=", 0) != 0 &&
	    partition_template.rfind("month=", 0) != 0 &&
	    partition_template.rfind("year=", 0) != 0) {
		throw BinderException("aligned CREATE TABLE: invalid partition_template '%s' "
		                       "(expected 'date=%%Y-%%m-%%d', 'month=%%Y-%%m', or 'year=%%Y')",
		                       partition_template);
	}

	// Parse groups option (or default: all non-key columns go to index)
	// Note: groups_map was already parsed above (before the table_exists check).
	if (groups_map.empty() && groups_option.empty()) {
		// No groups specified — default all columns to index
	}

	// Ensure index group exists
	if (groups_map.find("index") == groups_map.end()) {
		// Default: index group contains symbol + date + all unassigned columns
		vector<string> index_cols;
		for (idx_t i = 0; i < columns.size(); i++) {
			// Check if this column is assigned to a non-index group
			bool assigned = false;
			for (auto &kv : groups_map) {
				if (StringUtil::CIEquals(kv.first, "index")) {
					continue;
				}
				for (auto &c : kv.second) {
					if (StringUtil::CIEquals(c, columns[i].Name())) {
						assigned = true;
						break;
					}
				}
				if (assigned) break;
			}
			if (!assigned) {
				index_cols.push_back(columns[i].Name());
			}
		}
		groups_map["index"] = std::move(index_cols);
	} else {
		// If index is specified, make sure symbol + date are included
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

	// Validate that all groups reference valid columns
	for (auto &kv : groups_map) {
		for (auto &col_name : kv.second) {
			bool found = false;
			for (auto &col : columns) {
				if (StringUtil::CIEquals(col.Name(), col_name)) {
					found = true;
					break;
				}
			}
			if (!found) {
				throw BinderException("aligned CREATE TABLE: groups option references unknown "
				                       "column '%s' in group '%s'", col_name, kv.first);
			}
		}
	}

	// Create table directory
	fs.CreateDirectoriesRecursive(table_dir);

	// For each group, create the directory + write placeholder parquet
	string default_partition_key = DefaultPartitionKey(partition_template);
	const string placeholder_name = "0000-0000000000.parquet";

	for (auto &kv : groups_map) {
		const string &group_name = kv.first;
		auto &group_cols = kv.second;

		// Build column types/names for this group
		vector<string> col_names;
		vector<LogicalType> col_types;
		for (auto &cn : group_cols) {
			for (auto &col : columns) {
				if (StringUtil::CIEquals(col.Name(), cn)) {
					col_names.push_back(col.Name());
					col_types.push_back(col.Type());
					break;
				}
			}
		}

		// Create group directory
		string group_dir = table_dir + "/" + group_name;
		// Create partition directory
		string part_dir = group_dir + "/" + default_partition_key;
		fs.CreateDirectoriesRecursive(part_dir);

		// Write placeholder parquet
		string parquet_path = part_dir + "/" + placeholder_name;
		WriteEmptyParquet(context, fs, parquet_path, col_names, col_types);
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

	// Check the partition doesn't already exist
	for (auto &group : plan.groups) {
		for (auto &part : group.parts) {
			if (part.partition_key == partition_key) {
				throw BinderException("aligned CREATE TABLE (partition): partition '%s' already "
				                       "exists in group '%s'", partition_key, group.manifest.group);
			}
		}
	}

	// For each group, write a placeholder parquet in the new partition
	const string placeholder_name = "0000-0000000000.parquet";
	for (auto &group : plan.groups) {
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
