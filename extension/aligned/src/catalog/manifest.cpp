#include "catalog/manifest.hpp"
#include "resolver/partition_resolver.hpp"
#include "resolver/row_space.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "parquet_reader.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <map>

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
		// `groups` is legacy and NEVER read (the group list is always discovered
		// from the file layout by one glob); it is parsed only so it survives a
		// writer/compactor manifest rewrite round-trip.
		manifest.groups = GetStringArray(root, "groups", manifest_path, false);
		ParsePartitioning(root, manifest.partitioning, manifest_path);
	});
	return true;
}

//! Reads footer metadata (row count + schema) of one part file. Under v6 the
//! row count is ALSO read here (it is not stored in the plan), but it is used
//! only for the defensive OpenPart check against the file name; the plan's
//! row bookkeeping comes entirely from the self-describing file names. Only
//! ONE footer read per group is needed at plan time (the group's last part,
//! for the schema + the index's date-field contract).
struct PartFooterInfo {
	idx_t row_count = 0;
	vector<string> columns;
	vector<LogicalType> types;
};

static PartFooterInfo ReadPartFooterInfo(ClientContext &context, const string &path) {
	auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(path), ParquetOptions(context));
	PartFooterInfo info;
	info.row_count = reader->NumRows();
	for (auto &col : reader->columns) {
		info.columns.push_back(col.name);
		info.types.push_back(col.type);
	}
	return info;
}

//! Extracts the single-level partition key from a part path relative to the
//! group root ("month=2026-08/part-000000.parquet" -> "month=2026-08";
//! "part-000000.parquet" -> ""). The v5 contract allows exactly ONE partition
//! directory segment (year=/month=/date=); nested subdirectories or multiple
//! partition levels are rejected. The key is the full "name=value" segment so
//! groups using different partition kinds (e.g. date= vs month=) cannot match.
static bool ExtractPartitionKey(const string &rel_path, string &key, string &error) {
	auto slash = rel_path.find_last_of('/');
	string dir = slash == string::npos ? "" : rel_path.substr(0, slash);
	if (dir.empty()) {
		key = "";
		return true; // unpartitioned group
	}
	auto segs = StringUtil::Split(dir, '/');
	string part_seg;
	for (auto &seg : segs) {
		if (seg.find('=') == string::npos) {
			error = "nested non-partition subdirectory '" + seg + "'";
			return false;
		}
		if (!part_seg.empty()) {
			error = "more than one partition level ('" + part_seg + "/" + seg + "')";
			return false;
		}
		part_seg = seg;
	}
	key = part_seg;
	return true;
}

//! Parses a self-describing part file name ("0002-0000002048.parquet") into its
//! partition-local index (4 digits) and total row count (10 digits). Any other
//! name is a v6 contract violation.
static bool ParsePartFileName(const string &name, idx_t &index, idx_t &rows) {
	if (!StringUtil::EndsWith(name, ".parquet")) {
		return false;
	}
	string base = name.substr(0, name.size() - 8); // strip ".parquet"
	if (base.size() != 15 || base[4] != '-') {
		return false; // 4 digits + '-' + 10 digits
	}
	for (idx_t i = 0; i < 15; i++) {
		if (i == 4) {
			continue;
		}
		if (base[i] < '0' || base[i] > '9') {
			return false;
		}
	}
	index = (idx_t)std::stoull(base.substr(0, 4));
	rows = (idx_t)std::stoull(base.substr(5));
	return true;
}

//! Appends one partition's parts (a contiguous, key-sorted run) to a group's
//! part list. Row counts come from the FILE NAMES (no footer reads); part j's
//! start_row = partition_start + sum of the lower-index parts' row counts. The
//! partition's total row count is the sum of all file-name rows.
static void AppendPartitionParts(GroupPlan &group, const string &key, const vector<string> &part_paths,
                                 idx_t partition_start, const string &table_name) {
	GroupPartition pi;
	pi.key = key;
	pi.start_row = partition_start;
	pi.first_part = group.parts.size();
	pi.part_count = part_paths.size();

	idx_t running = 0;
	for (idx_t j = 0; j < part_paths.size(); j++) {
		PartInfo part;
		part.path = part_paths[j];
		auto slash = part.path.find_last_of("/\\");
		string file_name = slash == string::npos ? part.path : part.path.substr(slash + 1);
		part.part_name = file_name.substr(0, file_name.size() - 8); // strip ".parquet"
		idx_t index = 0;
		if (!ParsePartFileName(file_name, index, part.row_count)) {
			throw IOException("Aligned table '%s' group '%s': part file '%s' does not match the self-describing "
			                  "v6 name '{idx:04d}-{rows:10d}.parquet'",
			                  table_name, group.manifest.group, file_name);
		}
		part.partition_key = key;
		part.partition_index = index;
		part.partition_parts = part_paths.size();
		part.start_row = partition_start + running;
		group.parts.push_back(std::move(part));
		running += part.row_count;
	}
	pi.row_count = running;
	group.partitions.push_back(std::move(pi));
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

	// Group discovery: ONE glob over the whole table; every directory that
	// directly contains part files (partition segment "name=value" stripped) is
	// a column group. The manifest's `groups` field is never read.
	struct PendingPart {
		string path; // absolute, normalized
		string rel;  // relative to the TABLE root
		string rel_group; // relative to the GROUP root
		string key;   // partition key ("" when unpartitioned)
	};
	struct GroupAccum {
		string name;
		vector<PendingPart> pending;
	};
	vector<GroupAccum> group_list;
	case_insensitive_map_t<idx_t> group_idx;
	{
		auto files = fs.GlobFiles(table_prefix + "/**/*.parquet", FileGlobOptions::ALLOW_EMPTY);
		for (auto &file : files) {
			if (HasIgnoredPathSegment(file.path)) {
				continue; // contract §2.1d: '_'/'_' directory segments are invisible
			}
			string norm = NormalizePath(file.path);
			string group = DeriveGroupFromPath(norm, table_prefix);
			if (group.empty()) {
				throw IOException("Aligned table '%s': part file '%s' sits directly in the table root "
				                  "(no column group directory)",
				                  table_name, file.path);
			}
			PendingPart pp;
			pp.path = norm;
			pp.rel = norm.size() > table_prefix.size() && StringUtil::StartsWith(norm, table_prefix + "/")
			             ? norm.substr(table_prefix.size() + 1)
			             : norm;
			string group_prefix = group + "/";
			pp.rel_group = StringUtil::StartsWith(pp.rel, group_prefix) ? pp.rel.substr(group_prefix.size()) : pp.rel;
			string key_error;
			if (!ExtractPartitionKey(pp.rel_group, pp.key, key_error)) {
				throw IOException("Aligned table '%s': group '%s': %s (v5 single-level partition contract)",
				                  plan.table.name.empty() ? table_name : plan.table.name, group, key_error);
			}
			auto it = group_idx.find(group);
			if (it == group_idx.end()) {
				group_idx[group] = group_list.size();
				group_list.push_back({group, {}});
				it = group_idx.find(group);
			}
			group_list[it->second].pending.push_back(std::move(pp));
		}
		// Empty table (no parts anywhere): the group skeleton comes from the
		// manifest's `groups` list — the ONLY case where that field is read
		// (the writer needs the skeleton to lay out the first write; a read of
		// an empty table is degenerate anyway).
		if (group_list.empty() && !plan.table.groups.empty()) {
			for (auto &group_name : plan.table.groups) {
				group_list.push_back({group_name, {}});
			}
		}
	}

	// Contract §2.1b: the 'index' group is mandatory
	idx_t index_gi = DConstants::INVALID_INDEX;
	for (idx_t gi = 0; gi < group_list.size(); gi++) {
		if (StringUtil::CIEquals(group_list[gi].name, "index")) {
			index_gi = gi;
			break;
		}
	}
	if (index_gi == DConstants::INVALID_INDEX) {
		throw IOException("Aligned table '%s': mandatory group 'index' was not found", plan.table.name.empty() ?
		                                                                               table_name :
		                                                                               plan.table.name);
	}

	// Process the index group first: it defines the authoritative partition
	// table (keys, start rows, row counts) that every other group must align
	// to. Its partition indexes must be consecutive from 0000 (no gaps).
	GroupPlan index_group;
	index_group.manifest.group = "index";
	index_group.group_path = plan.table_path + "/index";
	idx_t total_rows = 0;
	vector<GroupPartition> index_partitions;
	case_insensitive_map_t<idx_t> index_part_by_key;
	vector<string> index_paths_for_derive;
	{
		auto &acc = group_list[index_gi];
		std::sort(acc.pending.begin(), acc.pending.end(),
		          [](const PendingPart &a, const PendingPart &b) { return a.rel_group < b.rel_group; });
		// Partition runs (contiguous same-key groups in sorted order)
		idx_t i = 0;
		while (i < acc.pending.size()) {
			string key = acc.pending[i].key;
			idx_t j = i + 1;
			while (j < acc.pending.size() && acc.pending[j].key == key) {
				j++;
			}
			vector<string> part_paths;
			for (idx_t k = i; k < j; k++) {
				part_paths.push_back(acc.pending[k].path);
				index_paths_for_derive.push_back(acc.pending[k].path);
			}
			AppendPartitionParts(index_group, key, part_paths, total_rows, plan.table.name);
			auto &pi = index_group.partitions.back();
			// Index group: indexes must be consecutive from 0000 (no missing,
			// no gaps). The self-describing name is the contract; the footer
			// is NOT read for row counts.
			for (idx_t k = 0; k < pi.part_count; k++) {
				auto &part = index_group.parts[pi.first_part + k];
				if (part.partition_index != k) {
					throw IOException("Aligned table '%s' group 'index' partition '%s': part indexes must be "
					                  "consecutive from 0000 (found '%s' at position %llu; the index group may not "
					                  "skip or repeat indexes)",
					                  plan.table.name.empty() ? table_name : plan.table.name, key,
					                  part.part_name, k);
				}
			}
			index_partitions.push_back(pi);
			index_part_by_key[key] = index_partitions.size() - 1;
			total_rows += pi.row_count;
			i = j;
		}
		// Partition templates for pruning: explicit from the manifest, else
		// derived from the layout (single-level only). The source column is
		// bound AFTER the index schema is known (the DATE/TIMESTAMP field
		// among the index schema's first two columns) — see below; this block
		// only records the explicit templates (for empty tables there is no
		// schema yet, so the templates are taken verbatim).
		auto explicit_it = plan.table.partitioning.find("index");
		if (explicit_it != plan.table.partitioning.end() && !explicit_it->second.empty()) {
			index_group.manifest.partitioning = explicit_it->second;
		}
	}
	index_group.full_coverage = true; // the index defines the full row space
	plan.row_count = total_rows;

	// Group schema = the group's LAST part (rel-path order) — ONE footer read
	// per group (row counts never come from footers under v6). For the index
	// this also yields the primary key (v7 contract): the index schema's FIRST
	// TWO columns ARE the key (date_col, symbol_col) — exactly one of them is a
	// DATE or TIMESTAMP field (the partition source column, filter pushdown
	// relies on it), the other is the symbol column (row ordering within a
	// partition). Empty tables (no parts) skip the schema read and the
	// primary-key contract.
	if (!index_group.parts.empty()) {
		auto &last_part = index_group.parts.back();
		auto last_footer = ReadPartFooterInfo(context, last_part.path);
		index_group.column_order = last_footer.columns;
		index_group.schema_types = last_footer.types;
		if (last_footer.columns.size() < 2) {
			throw IOException("Aligned table '%s': the index schema must have at least two columns "
			                  "(primary key: date, symbol); got %zu",
			                  plan.table.name.empty() ? table_name : plan.table.name, last_footer.columns.size());
		}
		// v7 primary-key contract: among the first two columns exactly one must
		// be DATE/TIMESTAMP (the partition source); the other is the symbol
		// column. Two date columns or no date column is a contract violation.
		string date_col;
		string symbol_col;
		idx_t date_fields = 0;
		for (idx_t c = 0; c < 2; c++) {
			if (last_footer.types[c].id() == LogicalTypeId::DATE ||
			    last_footer.types[c].id() == LogicalTypeId::TIMESTAMP) {
				date_fields++;
				date_col = last_footer.columns[c];
			} else {
				symbol_col = last_footer.columns[c];
			}
		}
		if (date_fields != 1 || symbol_col.empty()) {
			throw IOException("Aligned table '%s': the index schema's first two columns must be the primary key "
			                  "'(date, symbol)' — exactly one DATE/TIMESTAMP field (the partition source column) "
			                  "and one symbol column; got '%s' and '%s'",
			                  plan.table.name.empty() ? table_name : plan.table.name,
			                  last_footer.columns[0].c_str(), last_footer.columns[1].c_str());
		}
		index_group.partition_source = date_col;
		index_group.symbol_column = symbol_col;
		// Explicit partitioning templates must source that column (otherwise
		// filter pushdown silently misses); derived templates get it bound.
		auto explicit_it = plan.table.partitioning.find("index");
		if (explicit_it != plan.table.partitioning.end() && !explicit_it->second.empty()) {
			for (auto &t : explicit_it->second) {
				if (!StringUtil::CIEquals(t.source, date_col)) {
					throw IOException("Aligned table '%s': explicit partitioning for 'index' sources column '%s' "
					                  "but the index's partition source column is '%s'",
					                  plan.table.name.empty() ? table_name : plan.table.name, t.source, date_col);
				}
			}
			index_group.manifest.partitioning = explicit_it->second;
		} else {
			index_group.manifest.partitioning =
			    DerivePartitioningFromPaths(index_paths_for_derive, plan.table.name, "index", date_col);
		}
	}

	// Non-index groups: partition keys must be a subset of the index keys,
	// every shared key must agree on the partition's TOTAL row count, and
	// every SHARED part index must agree on its row count (the index may have
	// indexes a group lacks — deletion — but shared indexes must match).
	for (idx_t gi = 0; gi < group_list.size(); gi++) {
		if (gi == index_gi) {
			continue;
		}
		auto &acc = group_list[gi];
		GroupPlan group;
		group.manifest.group = acc.name;
		group.partition_source = index_group.partition_source;
		// Contract §2.1c: every non-index group is a two-level path 'lv1/lv2'
		auto slash = acc.name.find('/');
		if (slash == string::npos || acc.name.find('/', slash + 1) != string::npos || slash == 0 ||
		    slash + 1 >= acc.name.size()) {
			throw IOException("Aligned table '%s': group '%s' must be a two-level path 'lv1/lv2' (except 'index')",
			                  plan.table.name.empty() ? table_name : plan.table.name, acc.name);
		}
		group.lv1 = acc.name.substr(0, slash);
		group.lv2 = acc.name.substr(slash + 1);
		group.group_path = plan.table_path + "/" + acc.name;

		std::sort(acc.pending.begin(), acc.pending.end(),
		          [](const PendingPart &a, const PendingPart &b) { return a.rel_group < b.rel_group; });

		vector<string> part_paths_for_derive;
		idx_t i = 0;
		bool has_parts = false;
		while (i < acc.pending.size()) {
			string key = acc.pending[i].key;
			// Partition key must exist in the index (same partition kind +
			// value). A group may omit partitions but never add its own.
			auto idx_it = index_part_by_key.find(key);
			if (idx_it == index_part_by_key.end()) {
				throw IOException("Aligned table '%s': group '%s' has partition '%s' that the index group does not "
				                  "have (partition-aligned contract: group keys must be a subset of the index keys)",
				                  plan.table.name.empty() ? table_name : plan.table.name, acc.name, key);
			}
			auto &ip = index_partitions[idx_it->second];
			idx_t j = i + 1;
			while (j < acc.pending.size() && acc.pending[j].key == key) {
				j++;
			}
			vector<string> part_paths;
			for (idx_t k = i; k < j; k++) {
				part_paths.push_back(acc.pending[k].path);
				part_paths_for_derive.push_back(acc.pending[k].path);
			}
			AppendPartitionParts(group, key, part_paths, ip.start_row, plan.table.name);
			auto &pi = group.partitions.back();
			// Cross-group agreement (v6): the partition's TOTAL row count must
			// equal the index's (sum of file-name rows), and every index that
			// BOTH sides have must agree on its row count. A group may lack
			// indexes the index has (deletion) — only shared ones are checked.
			if (pi.row_count != ip.row_count) {
				throw IOException("Aligned table '%s': group '%s' partition '%s' covers %llu rows but the index "
				                  "covers %llu rows (partition-aligned contract: shared partitions must agree on the "
				                  "total row count)",
				                  plan.table.name.empty() ? table_name : plan.table.name, acc.name, key, pi.row_count,
				                  ip.row_count);
			}
			// The index group's indexes are consecutive 0..n-1; build a
			// lookup of the index's row count per index for this partition.
			std::map<idx_t, idx_t> index_rows;
			for (idx_t k = 0; k < ip.part_count; k++) {
				auto &ip_part = index_group.parts[ip.first_part + k];
				index_rows[ip_part.partition_index] = ip_part.row_count;
			}
			for (idx_t k = 0; k < pi.part_count; k++) {
				auto &part = group.parts[pi.first_part + k];
				auto it = index_rows.find(part.partition_index);
				if (it != index_rows.end() && it->second != part.row_count) {
					throw IOException("Aligned table '%s': group '%s' partition '%s' part index %llu ('%s') holds "
					                  "%llu rows but the index holds %llu rows for the same index (shared indexes "
					                  "must agree on row counts)",
					                  plan.table.name.empty() ? table_name : plan.table.name, acc.name, key,
					                  part.partition_index, part.part_name, part.row_count, it->second);
				}
			}
			has_parts = true;
			i = j;
		}
		// Full coverage = the group's partition keys equal the index's keys
		// (only such groups participate in active-interval intersection).
		group.full_coverage = has_parts && group.partitions.size() == index_partitions.size();
		if (group.full_coverage) {
			for (auto &p : group.partitions) {
				auto it = index_part_by_key.find(p.key);
				if (it == index_part_by_key.end()) {
					group.full_coverage = false;
					break;
				}
			}
		}

		// Group schema = the group's LAST part (rel-path order) — ONE footer
		// read per group (v6: row counts never come from footers).
		if (has_parts) {
			auto &last_part = group.parts.back();
			auto last_footer = ReadPartFooterInfo(context, last_part.path);
			group.column_order = last_footer.columns;
			group.schema_types = last_footer.types;
		}

		// Partition templates for pruning: explicit from the manifest, else
		// derived from the layout (single-level only). Non-index groups
		// partition on the same source column as the index.
		auto explicit_it = plan.table.partitioning.find(acc.name);
		if (explicit_it != plan.table.partitioning.end() && !explicit_it->second.empty()) {
			group.manifest.partitioning = explicit_it->second;
		} else {
			group.manifest.partitioning =
			    DerivePartitioningFromPaths(part_paths_for_derive, plan.table.name, acc.name,
			                                group.partition_source);
		}
		plan.groups.push_back(std::move(group));
	}
	plan.groups.insert(plan.groups.begin(), std::move(index_group));
	plan.table.groups.clear();
	for (auto &g : group_list) {
		plan.table.groups.push_back(g.name);
	}
}

} // namespace duckdb