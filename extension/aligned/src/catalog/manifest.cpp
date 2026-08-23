#include "catalog/manifest.hpp"
#include "resolver/partition_resolver.hpp"
#include "io/parquet_io.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "parquet_reader.hpp"

#include <algorithm>
#include <map>

namespace duckdb {

namespace {

//! Contract §2.1d: true when any directory segment **below the table root**
//! starts with '.' or '_' (e.g. "_tmp/", ".hidden/"). Segments ABOVE the table
//! root (e.g. a data root like "/home/user/.config/factorlake") are NOT checked
//! — only segments within the table directory are invisible.
//! The file name segment is excluded.
//! Both `path` and `table_prefix` must be normalized to '/' separators.
bool HasIgnoredPathSegment(const string &path, const string &table_prefix) {
	// Extract the relative path below the table root.
	// table_prefix = ".../table_name" (the table directory). The file path
	// is ".../table_name/index/month=2026-01/0000-...parquet". We strip
	// everything up to and including the table directory name.
	// Since table_prefix may be relative while path is absolute (or vice
	// versa), we anchor on the last segment of table_prefix (the table name).
	auto prefix_slash = table_prefix.find_last_of('/');
	string table_name_seg =
	    prefix_slash == string::npos ? table_prefix : table_prefix.substr(prefix_slash + 1);
	if (table_name_seg.empty()) {
		return false;
	}
	// Find "/table_name/" as a full directory segment in the path.
	string needle = "/" + table_name_seg + "/";
	auto pos = path.rfind(needle);
	if (pos == string::npos) {
		return false; // can't locate table dir — don't filter
	}
	// rel is everything after "/table_name/"
	string rel = path.substr(pos + needle.size());
	// Check each directory segment of the relative path (the file name,
	// i.e. the last segment after the final '/', is excluded).
	// Walk from the beginning: the first segment (before the first '/') is
	// a directory segment that must be checked too.
	idx_t scan = 0;
	while (scan < rel.size()) {
		auto slash = rel.find_first_of('/', scan);
		if (slash == string::npos) {
			break; // last segment = file name, excluded
		}
		string segment = rel.substr(scan, slash - scan);
		if (!segment.empty() && (segment[0] == '.' || segment[0] == '_')) {
			return true;
		}
		scan = slash + 1;
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
		if (!ParsePartName(file_name, index, part.row_count)) {
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
	plan.table_name = table_name;

	// An empty table (no parts) is not a valid table — the index group is
	// mandatory and discovered via glob. No manifest file is used.
	if (!fs.DirectoryExists(plan.table_path)) {
		throw IOException("Aligned table '%s': table directory does not exist at '%s'", table_name,
		                  plan.table_path);
	}
	string table_prefix = NormalizePath(plan.table_path);

	// Group discovery: ONE glob over the whole table; every directory that
	// directly contains part files (partition segment "name=value" stripped) is
	// a column group.
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
			// Normalize the glob result to '/' separators before any path
			// comparison — GlobFiles may return '\' on Windows, while
			// table_prefix uses '/' (NormalizePath is applied later but
			// HasIgnoredPathSegment needs a normalized path).
			string norm = NormalizePath(file.path);
			if (HasIgnoredPathSegment(norm, table_prefix)) {
				continue; // contract §2.1d: '.'/'_' directory segments below table root are invisible
			}
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
				                  table_name, group, key_error);
			}
			auto it = group_idx.find(group);
			if (it == group_idx.end()) {
				group_idx[group] = group_list.size();
				group_list.push_back({group, {}});
				it = group_idx.find(group);
			}
			group_list[it->second].pending.push_back(std::move(pp));
		}
	}

	// Contract §2.1b: the 'index' group is mandatory. An empty table (no
	// parts at all) returns an empty plan — the caller (mutator) handles
	// the first-write case by creating groups from the mapping.
	idx_t index_gi = DConstants::INVALID_INDEX;
	for (idx_t gi = 0; gi < group_list.size(); gi++) {
		if (StringUtil::CIEquals(group_list[gi].name, "index")) {
			index_gi = gi;
			break;
		}
	}
	if (group_list.empty()) {
		// Empty table: no parts anywhere. Return an empty plan.
		return;
	}
	if (index_gi == DConstants::INVALID_INDEX) {
		throw IOException("Aligned table '%s': mandatory group 'index' was not found", table_name);
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
			AppendPartitionParts(index_group, key, part_paths, total_rows, table_name);
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
					                  table_name, key, part.part_name, k);
				}
			}
			index_partitions.push_back(pi);
			index_part_by_key[key] = index_partitions.size() - 1;
			total_rows += pi.row_count;
			i = j;
		}
	}
	index_group.full_coverage = true; // the index defines the full row space
	plan.row_count = total_rows;

	// Group schema = the group's LAST part (rel-path order) — ONE footer read
	// per group (row counts never come from footers under v6). For the index
	// this also yields the primary key (v8 contract): the index schema's FIRST
	// TWO columns ARE the key (symbol, date) — column 0 is the symbol (string),
	// column 1 is the DATE/TIMESTAMP partition source column. The table is
	// partitioned by date; within a partition rows are sorted by (symbol, date)
	// ascending. Empty tables (no parts) skip the schema read and the
	// primary-key contract.
	if (!index_group.parts.empty()) {
		auto &last_part = index_group.parts.back();
		auto last_footer = ReadPartFooterInfo(context, last_part.path);
		index_group.column_order = last_footer.columns;
		index_group.schema_types = last_footer.types;
		if (last_footer.columns.size() < 2) {
			throw IOException("Aligned table '%s': the index schema must have at least two columns "
			                  "(primary key: symbol, date); got %zu",
			                  table_name, last_footer.columns.size());
		}
		// v8 primary-key contract: column 0 = symbol, column 1 = DATE/TIMESTAMP
		// (the partition source). The first column must NOT be DATE/TIMESTAMP;
		// the second column MUST be DATE/TIMESTAMP.
		auto t1 = last_footer.types[1].id();
		if (t1 != LogicalTypeId::DATE && t1 != LogicalTypeId::TIMESTAMP) {
			throw IOException("Aligned table '%s': the index schema's second column must be DATE or "
			                  "TIMESTAMP (the partition source column); got '%s' of type %s",
			                  table_name, last_footer.columns[1].c_str(),
			                  EnumUtil::ToChars(t1));
		}
		string symbol_col = last_footer.columns[0];
		string date_col = last_footer.columns[1];
		index_group.partition_source = date_col;
		index_group.symbol_column = symbol_col;
		// Partitioning is always derived from the directory layout.
		index_group.manifest.partitioning =
		    DerivePartitioningFromPaths(index_paths_for_derive, table_name, "index", date_col);
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
			                  table_name, acc.name);
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
				                  table_name, acc.name, key);
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
			AppendPartitionParts(group, key, part_paths, ip.start_row, table_name);
			auto &pi = group.partitions.back();
			// Cross-group agreement (v6): the partition's TOTAL row count must
			// equal the index's (sum of file-name rows), and every index that
			// BOTH sides have must agree on its row count. A group may lack
			// indexes the index has (deletion) — only shared ones are checked.
			if (pi.row_count != ip.row_count) {
				throw IOException("Aligned table '%s': group '%s' partition '%s' covers %llu rows but the index "
				                  "covers %llu rows (partition-aligned contract: shared partitions must agree on the "
				                  "total row count)",
				                  table_name, acc.name, key, pi.row_count,
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
					                  table_name, acc.name, key,
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
		// Partitioning is always derived from the directory layout.
		group.manifest.partitioning =
		    DerivePartitioningFromPaths(part_paths_for_derive, table_name, acc.name,
		                                group.partition_source);
		plan.groups.push_back(std::move(group));
	}
	plan.groups.insert(plan.groups.begin(), std::move(index_group));
}

string ResolveDataRoot(ClientContext &context, const Value *root_param, const string &fn_name) {
	if (root_param && !root_param->IsNull()) {
		return StringValue::Get(*root_param);
	}
	Value setting_value;
	if (!context.TryGetCurrentSetting("aligned_data_root", setting_value)) {
		throw BinderException(
		    "%s: no data root configured. Pass root='...' or SET aligned_data_root = '...'", fn_name);
	}
	return StringValue::Get(setting_value);
}

const GroupPlan &IndexGroup(const TablePlan &plan) {
	D_ASSERT(!plan.groups.empty());
	return plan.groups[0];
}

idx_t NextPartIndexForPartition(const TablePlan &plan, const string &partition_key) {
	idx_t max_index = 0;
	for (auto &group : plan.groups) {
		for (auto &pk : group.partitions) {
			if (pk.key != partition_key) {
				continue;
			}
			for (idx_t i = pk.first_part; i < pk.first_part + pk.part_count; i++) {
				if (i < group.parts.size() && group.parts[i].partition_index > max_index) {
					max_index = group.parts[i].partition_index;
				}
			}
		}
	}
	return max_index + 1;
}

} // namespace duckdb