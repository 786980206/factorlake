#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// One entry of _table.json "partitioning" (or derived from the directory
// layout): a directory template (e.g. "date=%Y-%m-%d") sourced from the
// logical "date" column. Only three partition kinds are supported:
//   year=%Y, month=%Y-%m, date=%Y-%m-%d.
struct PartitionTemplate {
	string template_str;
	string source;
};

// Column-group metadata. The group name is the directory path relative to the
// table root ("index", "factor/alpha101", ...). The partitioning templates
// come from _table.json (explicit, required for empty tables) or are derived
// from the partition directory structure at plan time.
struct GroupManifest {
	string group;
	vector<PartitionTemplate> partitioning;
};

// _table.json — the optional, table-level manifest of a logical table.
// When the file is absent, the defaults below apply (auto-detected layout).
// All fields are optional; unknown/legacy fields (key, row_count,
// canonical_order, row_group_size, ...) are ignored for backwards
// compatibility.
struct TableManifest {
	string name; // optional, informational
	int64_t version = 1;
	// Alignment contract level:
	//   "all"   - every group has the same part count, part size and last
	//             part size (fully aligned; part rows derived by formula)
	//   "group" - each group is internally aligned (1st == 2nd part size),
	//             groups may differ from each other
	//   "none"  - no alignment: part rows are accumulated per file
	// When "all" is declared but the data does not satisfy it, the plan
	// probes and degrades to "group", then to "none".
	string aligned_mode = "all";
	bool aligned_declared = false; // true when "aligned" was present in _table.json
	idx_t rg_rows = 16384;   // target row-group size (writer flush boundary)
	idx_t part_rows = 4194304; // target part size (writer/compactor hint)
	idx_t last_txid = 0;     // last transaction id (writer/compactor bookkeeping)
	vector<string> groups;   // optional; discovered from the file layout when empty
	// Optional explicit partition templates per group (group -> templates).
	// Empty when partitioning must be derived from the directory layout.
	case_insensitive_map_t<vector<PartitionTemplate>> partitioning;
};

// A part file. The part order is the lexicographic order of the file's
// normalized relative path (directory segments then file name) — the index in
// that sorted list IS the part id (no numeric part names required). In the
// aligned modes start_row is derived by formula (i * part_rows), in "none"
// mode by accumulating footer row counts.
struct PartInfo {
	string path; // absolute path to the parquet file
	string part_name; // file base name (without directory)
	idx_t start_row = 0;
	idx_t row_count = 0;
	vector<string> columns; // column names in FILE schema order
};

// A resolved, validated Column Group.
struct GroupPlan {
	string group_path; // absolute path of the group directory
	GroupManifest manifest;
	vector<PartInfo> parts; // sorted in row order, validated to tile [0, table row_count)
	vector<string> column_order; // union of part columns (physical names), first-seen order
	vector<idx_t> output_positions; // table output position per column_order entry
	string lv1; // first path level of the group ("factor"); empty for "index"
	string lv2; // second path level ("alpha101"); empty for "index"
	// Probe results (aligned modes): rows per part / rows of the last part /
	// first row-group size of the first part. 0 when not applicable.
	idx_t part_rows = 0;
	idx_t last_rows = 0;
	idx_t rg_rows = 0;
};

// The fully resolved scan plan of one logical table.
struct TablePlan {
	string table_path; // absolute path
	TableManifest table;
	vector<GroupPlan> groups;
	string aligned_mode; // effective mode after probing ("all"/"group"/"none")
	idx_t row_count = 0; // total row count (probed, not a manifest field)
};

//! Resolves a logical table: reads the optional _table.json (defaults when
//! absent), discovers the column groups (explicit from the manifest or derived
//! from the file layout), globs the part files, reads part metadata (row count
//! + columns) from the Parquet footers, derives the partitioning, sorts the
//! parts into row order (relative-path order == part id), probes the alignment
//! mode ("all" -> "group" -> "none" degradation chain), assigns start_row by
//! formula (aligned modes) or footer accumulation ("none"), validates every
//! group tiles [0, row_count) and that all groups agree on the total row count.
//! Throws IOException on any contract violation.
void BuildTablePlan(ClientContext &context, const string &root_path, const string &table_name, TablePlan &plan);

//! Reads + validates _table.json only (used for lightweight lookups). Returns
//! false when the file does not exist (the caller falls back to defaults).
bool TryReadTableManifest(FileSystem &fs, const string &manifest_path, TableManifest &manifest);

} // namespace duckdb