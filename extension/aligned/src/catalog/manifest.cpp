#include "catalog/manifest.hpp"
#include "resolver/row_space.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"

#include <unordered_set>

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
		// aligned (bool, default true): whether leaf pruning results can be
		// unified into one alignment-group coordinate via intersection.
		auto aligned_val = duckdb_yyjson::yyjson_obj_get(root, "aligned");
		if (aligned_val) {
			if (duckdb_yyjson::yyjson_is_bool(aligned_val)) {
				manifest.aligned = duckdb_yyjson::yyjson_get_bool(aligned_val);
			} else if (duckdb_yyjson::yyjson_is_true(aligned_val)) {
				manifest.aligned = true;
			} else if (duckdb_yyjson::yyjson_is_false(aligned_val)) {
				manifest.aligned = false;
			} else {
				throw IOException("Aligned table: field 'aligned' in '%s' must be a boolean", manifest_path);
			}
		}
		GetUIntField(root, "row_count", manifest.row_count);
		GetUIntField(root, "row_group_size", manifest.row_group_size);
		GetUIntField(root, "part_rows", manifest.part_rows);
		manifest.groups = GetStringArray(root, "groups", manifest_path, true);
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

static GroupManifest ReadGroupManifest(FileSystem &fs, const string &manifest_path) {
	string content = ReadTextFile(fs, manifest_path);
	GroupManifest manifest;
	WithJsonObject(manifest_path, content, [&](duckdb_yyjson::yyjson_val *root) {
		manifest.group = GetRequiredString(root, "group", manifest_path);
		GetUIntField(root, "row_count", manifest.row_count);
		GetUIntField(root, "row_group_size", manifest.row_group_size);
		auto partitioning = duckdb_yyjson::yyjson_obj_get(root, "partitioning");
		if (partitioning) {
			if (!duckdb_yyjson::yyjson_is_arr(partitioning)) {
				throw IOException("Aligned table: 'partitioning' in '%s' must be an array", manifest_path);
			}
			auto size = duckdb_yyjson::yyjson_arr_size(partitioning);
			for (size_t i = 0; i < size; i++) {
				auto item = duckdb_yyjson::yyjson_arr_get(partitioning, i);
				if (!duckdb_yyjson::yyjson_is_obj(item)) {
					throw IOException("Aligned table: 'partitioning' entries in '%s' must be objects", manifest_path);
				}
				PartitionTemplate tmpl;
				if (!GetStringField(item, "template", tmpl.template_str) || tmpl.template_str.empty()) {
					throw IOException("Aligned table: 'partitioning' entry %zu in '%s' is missing 'template'", i,
					                  manifest_path);
				}
				if (!GetStringField(item, "source", tmpl.source) || tmpl.source.empty()) {
					throw IOException("Aligned table: 'partitioning' entry %zu in '%s' is missing 'source'", i,
					                  manifest_path);
				}
				manifest.partitioning.push_back(std::move(tmpl));
			}
		}
	});
	return manifest;
}

static PartInfo ReadPartSidecar(FileSystem &fs, const string &sidecar_path) {
	string content = ReadTextFile(fs, sidecar_path);
	PartInfo part;
	WithJsonObject(sidecar_path, content, [&](duckdb_yyjson::yyjson_val *root) {
		part.part_name = GetRequiredString(root, "part", sidecar_path);
		// table/group are validated by the caller against the scan context
		idx_t start_row = 0;
		if (!GetUIntField(root, "start_row", start_row)) {
			throw IOException("Aligned table: sidecar '%s' is missing 'start_row'", sidecar_path);
		}
		part.start_row = start_row;
		if (!GetUIntField(root, "row_count", part.row_count)) {
			throw IOException("Aligned table: sidecar '%s' is missing 'row_count'", sidecar_path);
		}
		GetUIntField(root, "row_group_size", part.row_group_size);
		part.columns = GetStringArray(root, "columns", sidecar_path, true);
	});
	if (part.columns.empty()) {
		throw IOException("Aligned table: sidecar '%s' declares no columns", sidecar_path);
	}
	return part;
}

//! Contract §2.1d: true when any directory segment of the path starts with
//! '.' or '_' (e.g. "_tmp/", ".hidden/"). The file name segment is excluded.
static bool HasIgnoredPathSegment(const string &path) {
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

//! Reads the commit marker of a partition directory (contract §9). Returns the
//! set of committed part names (basenames). A missing or invalid marker means
//! the directory is not committed and every part in it is invisible.
static unordered_set<string> ReadCommitMarker(FileSystem &fs, const string &dir, const string &table_name,
                                              const string &group_name) {
	unordered_set<string> result;
	string marker_path = dir + "/.aligned-commit.json";
	if (!fs.FileExists(marker_path)) {
		return result;
	}
	string content;
	try {
		content = ReadTextFile(fs, marker_path);
	} catch (...) {
		throw IOException("Aligned table '%s' group '%s': unreadable commit marker '%s'", table_name, group_name,
		                  marker_path);
	}
	WithJsonObject(marker_path, content, [&](duckdb_yyjson::yyjson_val *root) {
		idx_t txid = 0;
		if (!GetUIntField(root, "txid", txid)) {
			throw IOException("Aligned table '%s' group '%s': commit marker '%s' is missing 'txid'", table_name,
			                  group_name, marker_path);
		}
		auto parts = GetStringArray(root, "parts", marker_path, true);
		for (auto &part : parts) {
			result.insert(part);
		}
	});
	return result;
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
		group.manifest = ReadGroupManifest(fs, group.group_path + "/_group.json");
		if (!StringUtil::CIEquals(group.manifest.group, group_name)) {
			throw IOException("Aligned table '%s': group manifest name '%s' does not match group '%s'",
			                  plan.table.name, group.manifest.group, group_name);
		}
		if (group.manifest.row_count != plan.table.row_count) {
			throw IOException("Aligned table '%s': group '%s' row_count %llu does not match table row_count %llu",
			                  plan.table.name, group_name, group.manifest.row_count, plan.table.row_count);
		}

		// Discover part files (any physical partition layout; uncommitted parts
		// are invisible; directories starting with '.' or '_' are ignored)
		auto files = fs.GlobFiles(group.group_path + "/**/part-*.parquet", FileGlobOptions::ALLOW_EMPTY);
		for (auto &file : files) {
			auto &path = file.path;
			if (HasIgnoredPathSegment(path)) {
				continue; // ignore hidden dirs
			}
			// Extract base name without extension
			auto slash = path.find_last_of("/\\");
			string base_name = slash == string::npos ? path : path.substr(slash + 1);
			if (StringUtil::EndsWith(base_name, ".parquet")) {
				base_name = base_name.substr(0, base_name.size() - 8);
			}
			// Validate part naming
			if (base_name.rfind("part-", 0) != 0) {
				continue; // not a valid part file
			}
			string num_str = base_name.substr(5);
			idx_t part_id = 0;
			try {
				part_id = std::stoull(num_str);
			} catch (...) {
				continue; // malformed part number
			}
			// Open Parquet to read schema and row count
			auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(path), ParquetOptions(context));
			PartInfo part;
			part.path = path;
			part.part_name = base_name;
			idx_t part_rows = plan.table.part_rows ? plan.table.part_rows : 4194304ull;
			part.start_row = part_id * part_rows;
			part.row_count = reader->NumRows();
			part.row_group_size = plan.table.row_group_size;
			for (auto &col : reader->columns) {
				part.columns.push_back(col.name);
			}
			group.parts.push_back(std::move(part));
		}

		// Sort by start_row and validate the row space (contract §3 / §7)
		std::sort(group.parts.begin(), group.parts.end(),
		          [](const PartInfo &a, const PartInfo &b) { return a.start_row < b.start_row; });
		ValidateRowSpace(plan.table.name, group_name, plan.table.row_count, group.parts);

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
}

} // namespace duckdb
