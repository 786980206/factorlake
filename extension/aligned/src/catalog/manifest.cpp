#include "catalog/manifest.hpp"
#include "resolver/partition_resolver.hpp"
#include "resolver/row_space.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "parquet_reader.hpp"
#include "yyjson.hpp"

#include <algorithm>

namespace duckdb {

namespace {

string ReadTextFile(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	idx_t size = handle->GetFileSize();
	string result;
	result.resize(size);
	if (size > 0) {
		// NOTE: C++11 standard is enforced (string::data() returns const char*)
		handle->Read(&result[0], size, 0);
	}
	return result;
}

bool GetStringField(duckdb_yyjson::yyjson_val *obj, const char *key, string &result) {
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!val || !duckdb_yyjson::yyjson_is_str(val)) {
		return false;
	}
	result.assign(duckdb_yyjson::yyjson_get_str(val), duckdb_yyjson::yyjson_get_len(val));
	return true;
}

bool GetUIntField(duckdb_yyjson::yyjson_val *obj, const char *key, idx_t &result) {
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!val || !duckdb_yyjson::yyjson_is_uint(val)) {
		return false;
	}
	result = duckdb_yyjson::yyjson_get_uint(val);
	return true;
}

//! Parses a JSON document and invokes parse_body with the root object.
template <class FN>
void WithJsonObject(const string &path, const string &content, FN parse_body) {
	auto doc = duckdb_yyjson::yyjson_read(content.c_str(), content.size(), 0);
	if (!doc) {
		throw IOException("Aligned table: invalid JSON in '%s'", path);
	}
	auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
	if (!root || !duckdb_yyjson::yyjson_is_obj(root)) {
		duckdb_yyjson::yyjson_doc_free(doc);
		throw IOException("Aligned table: expected a JSON object in '%s'", path);
	}
	try {
		parse_body(root);
	} catch (...) {
		duckdb_yyjson::yyjson_doc_free(doc);
		throw;
	}
	duckdb_yyjson::yyjson_doc_free(doc);
}

string GetRequiredString(duckdb_yyjson::yyjson_val *obj, const char *key, const string &path) {
	string result;
	if (!GetStringField(obj, key, result) || result.empty()) {
		throw IOException("Aligned table: missing or invalid required field '%s' in '%s'", key, path);
	}
	return result;
}

vector<string> GetStringArray(duckdb_yyjson::yyjson_val *obj, const char *key, const string &path, bool required) {
	vector<string> result;
	auto val = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!val) {
		if (required) {
			throw IOException("Aligned table: missing required array field '%s' in '%s'", key, path);
		}
		return result;
	}
	if (!duckdb_yyjson::yyjson_is_arr(val)) {
		throw IOException("Aligned table: field '%s' in '%s' must be an array", key, path);
	}
	auto size = duckdb_yyjson::yyjson_arr_size(val);
	result.reserve(size);
	for (size_t i = 0; i < size; i++) {
		auto item = duckdb_yyjson::yyjson_arr_get(val, i);
		if (!duckdb_yyjson::yyjson_is_str(item)) {
			throw IOException("Aligned table: field '%s' in '%s' must be an array of strings", key, path);
		}
		result.emplace_back(duckdb_yyjson::yyjson_get_str(item), duckdb_yyjson::yyjson_get_len(item));
	}
	return result;
}

//! Parses the optional _table.json "partitioning" map (group -> templates).
//! The group manifests are gone; partitioning lives in the table manifest.
void ParsePartitioning(duckdb_yyjson::yyjson_val *obj, case_insensitive_map_t<vector<PartitionTemplate>> &out,
                       const string &path) {
	auto val = duckdb_yyjson::yyjson_obj_get(obj, "partitioning");
	if (!val) {
		return; // optional: derived from the directory layout instead
	}
	if (!duckdb_yyjson::yyjson_is_obj(val)) {
		throw IOException("Aligned table: field 'partitioning' in '%s' must be an object (group -> templates)", path);
	}
	size_t idx;
	size_t max;
	duckdb_yyjson::yyjson_val *key;
	duckdb_yyjson::yyjson_val *entry;
	yyjson_obj_foreach(val, idx, max, key, entry) {
		string group_name(duckdb_yyjson::yyjson_get_str(key), duckdb_yyjson::yyjson_get_len(key));
		if (!duckdb_yyjson::yyjson_is_arr(entry)) {
			throw IOException("Aligned table: partitioning entry '%s' in '%s' must be an array", group_name, path);
		}
		vector<PartitionTemplate> templates;
		auto size = duckdb_yyjson::yyjson_arr_size(entry);
		for (size_t i = 0; i < size; i++) {
			auto item = duckdb_yyjson::yyjson_arr_get(entry, i);
			if (!duckdb_yyjson::yyjson_is_obj(item)) {
				throw IOException("Aligned table: partitioning entries of '%s' in '%s' must be objects", group_name,
				                  path);
			}
			PartitionTemplate tmpl;
			if (!GetStringField(item, "template", tmpl.template_str) || tmpl.template_str.empty()) {
				throw IOException("Aligned table: partitioning entry %zu of '%s' in '%s' is missing 'template'", i,
				                  group_name, path);
			}
			if (!GetStringField(item, "source", tmpl.source) || tmpl.source.empty()) {
				throw IOException("Aligned table: partitioning entry %zu of '%s' in '%s' is missing 'source'", i,
				                  group_name, path);
			}
			templates.push_back(std::move(tmpl));
		}
		out[group_name] = std::move(templates);
	}
}

//! Contract §2.1d: true when any directory segment of the path starts with
//! '.' or '_' (e.g. "_tmp/", ".hidden/"). The file name segment is excluded.
bool HasIgnoredPathSegment(const string &path) {
	auto start = path.find_first_of("/\\");
	while (start != string::npos) {
		auto end = path.find_first_of("/\\", start + 1);
		string segment = path.substr(start + 1, end == string::npos ? string::npos : end - start - 1);
		if (!segment.empty() && (segment[0] == '.' || segment[0] == '_')) {
			return true;
		}
		start = end;
	}
	return false;
}

//! Parses a "part-%06llu" file name; returns false when malformed.
bool ParsePartName(const string &base_name, idx_t &part_id) {
	if (base_name.rfind("part-", 0) != 0) {
		return false;
	}
	string num_str = base_name.substr(5);
	if (num_str.empty() || num_str.size() > 20) {
		return false;
	}
	for (auto c : num_str) {
		if (c < '0' || c > '9') {
			return false;
		}
	}
	try {
		part_id = std::stoull(num_str);
	} catch (...) {
		return false;
	}
	return true;
}

} // namespace

TableManifest ReadTableManifest(FileSystem &fs, const string &manifest_path) {
	string content = ReadTextFile(fs, manifest_path);
	TableManifest manifest;
	WithJsonObject(manifest_path, content, [&](duckdb_yyjson::yyjson_val *root) {
		manifest.name = GetRequiredString(root, "name", manifest_path);
		idx_t version = 1;
		if (GetUIntField(root, "version", version)) {
			manifest.version = version;
		}
		idx_t schema_version = 1;
		if (GetUIntField(root, "schema_version", schema_version)) {
			manifest.schema_version = schema_version;
		}
		manifest.key = GetStringArray(root, "key", manifest_path, true);
		string canonical_order;
		if (GetStringField(root, "canonical_order", canonical_order)) {
			manifest.canonical_order = canonical_order;
		}
		// row_count is bookkeeping only (written by the writer); the reader
		// derives the total row count from the Parquet footers.
		GetUIntField(root, "row_count", manifest.row_count);
		GetUIntField(root, "row_group_size", manifest.row_group_size);
		GetUIntField(root, "part_rows", manifest.part_rows);
		GetUIntField(root, "last_txid", manifest.last_txid);
		manifest.groups = GetStringArray(root, "groups", manifest_path, true);
		ParsePartitioning(root, manifest.partitioning, manifest_path);
	});
	if (manifest.key.empty()) {
		throw IOException("Aligned table: manifest '%s' declares an empty key", manifest_path);
	}
	if (manifest.groups.empty()) {
		throw IOException("Aligned table: manifest '%s' declares no column groups", manifest_path);
	}
	if (manifest.canonical_order != "fixed") {
		throw IOException("Aligned table: manifest '%s' has unsupported canonical_order '%s' (only 'fixed' is supported)",
		                  manifest_path, manifest.canonical_order);
	}
	return manifest;
}

void BuildTablePlan(ClientContext &context, const string &root_path, const string &table_name, TablePlan &plan) {
	auto &fs = FileSystem::GetFileSystem(context);

	// Normalize the root path (strip trailing separators)
	string root = root_path;
	while (!root.empty() && (root.back() == '/' || root.back() == '\\')) {
		root.pop_back();
	}
	plan.table_path = root + "/" + table_name;
	plan.table = ReadTableManifest(fs, plan.table_path + "/_table.json");
	if (!StringUtil::CIEquals(plan.table.name, table_name)) {
		throw IOException("Aligned table: manifest name '%s' does not match requested table '%s'", plan.table.name,
		                  table_name);
	}

	// Contract §2.1b: the 'index' group is mandatory
	bool has_index = false;
	for (auto &g : plan.table.groups) {
		if (StringUtil::CIEquals(g, "index")) {
			has_index = true;
			break;
		}
	}
	if (!has_index) {
		throw IOException("Aligned table '%s': mandatory group 'index' is missing from _table.json", plan.table.name);
	}

	for (auto &group_name : plan.table.groups) {
		GroupPlan group;
		group.manifest.group = group_name;
		// Contract §2.1c: every non-index group is a two-level path 'lv1/lv2'
		if (!StringUtil::CIEquals(group_name, "index")) {
			auto slash = group_name.find('/');
			if (slash == string::npos || group_name.find('/', slash + 1) != string::npos || slash == 0 ||
			    slash + 1 >= group_name.size()) {
				throw IOException("Aligned table '%s': group '%s' must be a two-level path 'lv1/lv2' (except 'index')",
				                  plan.table.name, group_name);
			}
			group.lv1 = group_name.substr(0, slash);
			group.lv2 = group_name.substr(slash + 1);
		}
		group.group_path = plan.table_path + "/" + group_name;

		// Discover part files (any physical partition layout; directories
		// starting with '.' or '_' are ignored — contract §2.1d). Part
		// metadata (row count, columns) comes from the Parquet footer only;
		// there are no sidecars and no commit markers anymore.
		auto files = fs.GlobFiles(group.group_path + "/**/part-*.parquet", FileGlobOptions::ALLOW_EMPTY);
		struct DiscoveredPart {
			string dir_rel; // partition dir relative to the group root ("" = none)
			idx_t part_id;  // numeric id from the file name
			PartInfo part;
		};
		vector<DiscoveredPart> discovered;
		vector<string> part_paths; // for partition-schema derivation
		for (auto &file : files) {
			auto &path = file.path;
			if (HasIgnoredPathSegment(path)) {
				continue; // uncommitted / stray directories are invisible
			}
			auto slash = path.find_last_of("/\\");
			string base_name = slash == string::npos ? path : path.substr(slash + 1);
			if (StringUtil::EndsWith(base_name, ".parquet")) {
				base_name = base_name.substr(0, base_name.size() - 8);
			}
			idx_t part_id = 0;
			if (!ParsePartName(base_name, part_id)) {
				continue; // not a valid part file
			}
			// Open the Parquet footer for row count + column names
			auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(path), ParquetOptions(context));
			DiscoveredPart dp;
			dp.part.path = path;
			dp.part.part_name = base_name;
			dp.part.row_count = reader->NumRows();
			for (auto &col : reader->columns) {
				dp.part.columns.push_back(col.name);
			}
			// Partition dir relative to the group root (normalized to '/')
			string norm = path;
			std::replace(norm.begin(), norm.end(), '\\', '/');
			string prefix = group.group_path;
			std::replace(prefix.begin(), prefix.end(), '\\', '/');
			if (norm.size() > prefix.size() && StringUtil::StartsWith(norm, prefix + "/")) {
				dp.dir_rel = norm.substr(prefix.size() + 1);
			}
			// strip the file name
			auto last = dp.dir_rel.find_last_of('/');
			dp.dir_rel = last == string::npos ? "" : dp.dir_rel.substr(0, last);
			dp.part_id = part_id;
			part_paths.push_back(norm);
			discovered.push_back(std::move(dp));
		}

		// Partitioning: explicit (from _table.json) wins; otherwise derive it
		// from the directory layout (only year=/month=/date= are recognized).
		auto explicit_it = plan.table.partitioning.find(group_name);
		if (explicit_it != plan.table.partitioning.end() && !explicit_it->second.empty()) {
			group.manifest.partitioning = explicit_it->second;
		} else {
			group.manifest.partitioning = DerivePartitioningFromPaths(part_paths, plan.table.name, group_name);
		}

		// Sort into row order: partition dir (string order == chronological
		// order for the fixed year/month/date formats) then part id (write
		// order). start_row accumulates footer row counts in that order.
		std::sort(discovered.begin(), discovered.end(),
		          [](const DiscoveredPart &a, const DiscoveredPart &b) {
			          if (a.dir_rel != b.dir_rel) {
				          return a.dir_rel < b.dir_rel;
			          }
			          return a.part_id < b.part_id;
		          });
		idx_t start = 0;
		group.parts.reserve(discovered.size());
		for (auto &dp : discovered) {
			dp.part.start_row = start;
			start += dp.part.row_count;
			group.parts.push_back(std::move(dp.part));
		}
		// Validate the group's row space: parts tile [0, group_rows) exactly
		// (the alignment contract for this group).
		idx_t group_rows = start;
		ValidateRowSpace(plan.table.name, group_name, group_rows, group.parts);

		// Column order: union of part columns, first-seen order (contract §8)
		for (auto &part : group.parts) {
			for (auto &col : part.columns) {
				if (std::find(group.column_order.begin(), group.column_order.end(), col) == group.column_order.end()) {
					group.column_order.push_back(col);
				}
			}
		}
		plan.groups.push_back(std::move(group));
	}

	// Cross-group alignment: every group must cover the SAME total row count
	// (all groups live on the same Logical Row Space — contract §3).
	idx_t total = plan.groups[0].parts.empty() ? 0 : plan.groups[0].parts.back().start_row +
	                                                  plan.groups[0].parts.back().row_count;
	for (auto &group : plan.groups) {
		idx_t group_rows =
		    group.parts.empty() ? 0 : group.parts.back().start_row + group.parts.back().row_count;
		if (group_rows != total) {
			throw IOException("Aligned table '%s': group '%s' covers %llu rows but the table covers %llu rows "
			                  "(alignment violation)",
			                  plan.table.name, group.manifest.group, group_rows, total);
		}
	}
	plan.table.row_count = total;
}

} // namespace duckdb