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

//! Derives the group name from a normalized part path: strips the file name,
//! then walks the directory segments (relative to the table root) from the
//! end towards the root, skipping partition segments (containing '='), until
//! the first non-partition segment. The group is the longest suffix of
//! non-partition segments below the table root ("index", "factor/alpha101",
//! ...). Returns "" when the file sits directly in the table root.
string DeriveGroupFromPath(const string &norm_path, const string &table_prefix) {
	string rel = norm_path;
	if (norm_path.size() > table_prefix.size() && StringUtil::StartsWith(norm_path, table_prefix + "/")) {
		rel = norm_path.substr(table_prefix.size() + 1);
	}
	string dir = rel;
	auto slash = dir.find_last_of('/');
	dir = slash == string::npos ? "" : dir.substr(0, slash);
	string group_rel;
	while (!dir.empty()) {
		auto s = dir.find_last_of('/');
		string seg = s == string::npos ? dir : dir.substr(s + 1);
		if (seg.find('=') != string::npos) {
			// partition segment: skipped
		} else if (group_rel.empty()) {
			group_rel = seg;
		} else {
			group_rel = seg + "/" + group_rel;
		}
		dir = s == string::npos ? "" : dir.substr(0, s);
	}
	return group_rel;
}

//! Normalizes a path to '/' separators (absolute path expected).
string NormalizePath(const string &path) {
	string norm = path;
	std::replace(norm.begin(), norm.end(), '\\', '/');
	return norm;
}

} // namespace

bool TryReadTableManifest(FileSystem &fs, const string &manifest_path, TableManifest &manifest) {
	if (!fs.FileExists(manifest_path)) {
		return false;
	}
	string content = ReadTextFile(fs, manifest_path);
	WithJsonObject(manifest_path, content, [&](duckdb_yyjson::yyjson_val *root) {
		GetStringField(root, "name", manifest.name);
		idx_t version = 1;
		if (GetUIntField(root, "version", version)) {
			manifest.version = version;
		}
		// Legacy fields (aligned, key, canonical_order, row_count,
		// row_group_size) are ignored for backwards compatibility: the reader
		// always enforces full alignment.
		GetUIntField(root, "rg_rows", manifest.rg_rows);
		GetUIntField(root, "part_rows", manifest.part_rows);
		GetUIntField(root, "last_txid", manifest.last_txid);
		manifest.groups = GetStringArray(root, "groups", manifest_path, false);
		ParsePartitioning(root, manifest.partitioning, manifest_path);
	});
	return true;
}

//! Opens a part file and reads footer metadata (row count + columns).
static PartInfo ReadPartFooter(ClientContext &context, const string &path) {
	auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(path), ParquetOptions(context));
	PartInfo part;
	part.path = path;
	auto slash = path.find_last_of("/\\");
	part.part_name = slash == string::npos ? path : path.substr(slash + 1);
	if (StringUtil::EndsWith(part.part_name, ".parquet")) {
		part.part_name = part.part_name.substr(0, part.part_name.size() - 8);
	}
	part.row_count = reader->NumRows();
	for (auto &col : reader->columns) {
		part.columns.push_back(col.name);
		part.types.push_back(col.type);
	}
	return part;
}

//! Validates the group-level formula precondition against the ACTUAL footer
//! row counts: every part except the last must hold exactly part_rows rows
//! (part rows were read at plan time — no extra IO). Returns the index of the
//! violating part, or DConstants::INVALID_INDEX when the group is aligned.
static idx_t FindAlignedViolation(const GroupPlan &group, idx_t part_rows) {
	for (idx_t i = 0; i + 1 < group.parts.size(); i++) {
		if (group.parts[i].row_count != part_rows) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

void BuildTablePlan(ClientContext &context, const string &root_path, const string &table_name, TablePlan &plan) {
	auto &fs = FileSystem::GetFileSystem(context);

	// Normalize the root path (strip trailing separators)
	string root = root_path;
	while (!root.empty() && (root.back() == '/' || root.back() == '\\')) {
		root.pop_back();
	}
	plan.table_path = root + "/" + table_name;

	// Optional _table.json; defaults apply when absent
	TableManifest manifest;
	if (TryReadTableManifest(fs, plan.table_path + "/_table.json", manifest)) {
		plan.table = std::move(manifest);
		if (!plan.table.name.empty() && !StringUtil::CIEquals(plan.table.name, table_name)) {
			throw IOException("Aligned table: manifest name '%s' does not match requested table '%s'",
			                  plan.table.name, table_name);
		}
	} else {
		if (!fs.DirectoryExists(plan.table_path)) {
			throw IOException("Aligned table '%s': table directory does not exist at '%s'", table_name,
			                  plan.table_path);
		}
		plan.table = TableManifest(); // defaults: rg_rows=16384, part_rows=4194304
	}
	string table_prefix = NormalizePath(plan.table_path);

	// Group discovery: explicit list from the manifest wins; otherwise every
	// directory that directly contains part files (partition segments
	// "name=value" stripped) is a column group.
	vector<string> group_names = plan.table.groups;
	if (group_names.empty()) {
		auto files = fs.GlobFiles(table_prefix + "/**/*.parquet", FileGlobOptions::ALLOW_EMPTY);
		case_insensitive_set_t seen;
		for (auto &file : files) {
			if (HasIgnoredPathSegment(file.path)) {
				continue;
			}
			string group = DeriveGroupFromPath(NormalizePath(file.path), table_prefix);
			if (group.empty()) {
				throw IOException("Aligned table '%s': part file '%s' sits directly in the table root "
				                  "(no column group directory)",
				                  table_name, file.path);
			}
			if (seen.insert(group).second) {
				group_names.push_back(group);
			}
		}
	}

	// Contract §2.1b: the 'index' group is mandatory
	bool has_index = false;
	for (auto &g : group_names) {
		if (StringUtil::CIEquals(g, "index")) {
			has_index = true;
			break;
		}
	}
	if (!has_index) {
		throw IOException("Aligned table '%s': mandatory group 'index' was not found", plan.table.name.empty() ?
		                                                                               table_name :
		                                                                               plan.table.name);
	}

	for (auto &group_name : group_names) {
		GroupPlan group;
		group.manifest.group = group_name;
		// Contract §2.1c: every non-index group is a two-level path 'lv1/lv2'
		if (!StringUtil::CIEquals(group_name, "index")) {
			auto slash = group_name.find('/');
			if (slash == string::npos || group_name.find('/', slash + 1) != string::npos || slash == 0 ||
			    slash + 1 >= group_name.size()) {
				throw IOException("Aligned table '%s': group '%s' must be a two-level path 'lv1/lv2' (except 'index')",
				                  plan.table.name.empty() ? table_name : plan.table.name, group_name);
			}
			group.lv1 = group_name.substr(0, slash);
			group.lv2 = group_name.substr(slash + 1);
		}
		group.group_path = plan.table_path + "/" + group_name;

		// Discover part files: every .parquet file in the group tree whose path
		// has no '.'/'_' directory segment (uncommitted / stray files are
		// invisible — contract §2.1d). The sorted relative-path order IS the
		// part order; the index in that list IS the part id (no numeric part
		// names required). Metadata comes from the Parquet footer only.
		auto files = fs.GlobFiles(group.group_path + "/**/*.parquet", FileGlobOptions::ALLOW_EMPTY);
		struct DiscoveredPart {
			string rel_path; // normalized path relative to the group root (incl. file name)
			PartInfo part;
		};
		vector<DiscoveredPart> discovered;
		vector<string> part_paths; // for partition-schema derivation
		string group_prefix = NormalizePath(group.group_path);
		for (auto &file : files) {
			auto &path = file.path;
			if (HasIgnoredPathSegment(path)) {
				continue;
			}
			DiscoveredPart dp;
			dp.part = ReadPartFooter(context, path);
			string norm = NormalizePath(path);
			if (norm.size() > group_prefix.size() && StringUtil::StartsWith(norm, group_prefix + "/")) {
				dp.rel_path = norm.substr(group_prefix.size() + 1);
			} else {
				dp.rel_path = norm;
			}
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

		// Sort into row order: the normalized relative path (partition dirs in
		// string order == chronological order for the fixed year/month/date
		// formats, then the file name). The position in this sorted list is
		// the part id.
		std::sort(discovered.begin(), discovered.end(),
		          [](const DiscoveredPart &a, const DiscoveredPart &b) { return a.rel_path < b.rel_path; });
		group.parts.reserve(discovered.size());
		for (auto &dp : discovered) {
			group.parts.push_back(std::move(dp.part));
		}
		plan.groups.push_back(std::move(group));
	}

	// Full alignment is the only supported contract (no modes): every group
	// must have the same part count, part size and last-part size. part_rows
	// is taken from the index group's first part; every group is validated
	// against it fail-fast (no degradation). A table that is not fully
	// aligned is rejected — the reader never silently degrades.
	idx_t index_gi = DConstants::INVALID_INDEX;
	for (idx_t gi = 0; gi < plan.groups.size(); gi++) {
		if (StringUtil::CIEquals(plan.groups[gi].manifest.group, "index")) {
			index_gi = gi;
			break;
		}
	}
	idx_t part_rows = 0;
	idx_t index_part_count = 0;
	if (index_gi != DConstants::INVALID_INDEX && !plan.groups[index_gi].parts.empty()) {
		part_rows = plan.groups[index_gi].parts[0].row_count;
		index_part_count = plan.groups[index_gi].parts.size();
	}
	for (idx_t gi = 0; gi < plan.groups.size(); gi++) {
		auto &g = plan.groups[gi];
		if (g.parts.empty()) {
			continue;
		}
		if (index_part_count == 0) {
			throw IOException("Aligned table '%s': group '%s' has %llu parts but the index group has none "
			                  "(full alignment required)",
			                  plan.table.name.empty() ? table_name : plan.table.name, g.manifest.group,
			                  g.parts.size());
		}
		if (g.parts.size() != index_part_count) {
			throw IOException("Aligned table '%s': group '%s' has %llu parts but the index group has %llu "
			                  "(full alignment required: every group must have the same part count)",
			                  plan.table.name.empty() ? table_name : plan.table.name, g.manifest.group, g.parts.size(),
			                  index_part_count);
		}
		auto violating_pi = FindAlignedViolation(g, part_rows);
		if (violating_pi != DConstants::INVALID_INDEX) {
			throw IOException("Aligned table '%s': group '%s' part '%s' holds %llu rows; every part except the "
			                  "last must hold exactly %llu rows (full alignment required)",
			                  plan.table.name.empty() ? table_name : plan.table.name, g.manifest.group,
			                  g.parts[violating_pi].part_name, g.parts[violating_pi].row_count, part_rows);
		}
		g.part_rows = part_rows;
	}

	// Row intervals: start_row(i) = i * part_rows (identical across groups).
	// The formula preconditions were verified above against the actual footer
	// row counts; the last part may hold fewer rows (the cross-group total
	// check below covers the last-part agreement).
	for (auto &group : plan.groups) {
		idx_t start = 0;
		for (auto &part : group.parts) {
			part.start_row = start;
			start += part_rows;
		}
	}

	// Validate each group's row space and cross-group agreement on the total
	// row count (the alignment contract — required always, because the
	// DataChunk assembly needs the same Logical Row Space).
	idx_t total = 0;
	for (idx_t gi = 0; gi < plan.groups.size(); gi++) {
		auto &group = plan.groups[gi];
		idx_t group_rows = 0;
		for (auto &part : group.parts) {
			group_rows += part.row_count;
		}
		// ValidateRowSpace checks the parts tile [0, group_rows) exactly.
		ValidateRowSpace(plan.table.name.empty() ? table_name : plan.table.name, group.manifest.group, group_rows,
		                 group.parts);
		if (gi == 0) {
			total = group_rows;
		} else if (group_rows != total) {
			throw IOException("Aligned table '%s': group '%s' covers %llu rows but the table covers %llu rows "
			                  "(alignment violation)",
			                  plan.table.name.empty() ? table_name : plan.table.name, group.manifest.group, group_rows,
			                  total);
		}
		// Column order: union of part columns, first-seen order (contract §8)
		for (auto &part : group.parts) {
			for (auto &col : part.columns) {
				if (std::find(group.column_order.begin(), group.column_order.end(), col) == group.column_order.end()) {
					group.column_order.push_back(col);
				}
			}
		}
	}
	plan.row_count = total; // probed total (writer/compactor bookkeeping)
}

} // namespace duckdb