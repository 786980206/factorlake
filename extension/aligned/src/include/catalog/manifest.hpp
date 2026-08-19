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
// canonical_order, row_group_size, aligned, ...) are ignored for backwards
// compatibility.
struct TableManifest {
	string name; // optional, informational
	int64_t version = 1;
	// v4 (all-only): the reader always enforces FULL alignment — every group
	// must have the same part count, part size and last-part size. There is no
	// "aligned" mode field anymore; the multi-mode (all/group/none) probing
	// chain has been removed. A table that is not fully aligned is rejected
	// fail-fast.
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
// that sorted list IS the part id (no numeric part names required). With full
// alignment, start_row is derived by formula (i * part_rows).
struct PartInfo {
	string path; // absolute path to the parquet file
	string part_name; // file base name (without directory)
	idx_t start_row = 0;
	idx_t row_count = 0;
	vector<string> columns; // column names in FILE schema order
	vector<LogicalType> types; // column types in FILE schema order (from the footer, plan time)
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
	idx_t part_rows = 0; // rows per part (identical across groups under full alignment)
};

// The fully resolved scan plan of one logical table.
struct TablePlan {
	string table_path; // absolute path
	TableManifest table;
	vector<GroupPlan> groups;
	idx_t row_count = 0; // total row count (derived from footer + alignment formula)
};

//! Resolves a logical table: reads the optional _table.json (defaults when
//! absent), discovers the column groups (explicit from the manifest or derived
//! from the file layout), globs the part files, reads part metadata (row count
//! + columns) from the Parquet footers, derives the partitioning, sorts the
//! parts into row order (relative-path order == part id), enforces FULL
//! alignment (every group: same part count, every part except the last holds
//! exactly part_rows rows, same last-part size), assigns start_row by formula
//! (i * part_rows), validates every group tiles [0, row_count) and that all
//! groups agree on the total row count.
//! Throws IOException on any contract violation.
void BuildTablePlan(ClientContext &context, const string &root_path, const string &table_name, TablePlan &plan);

//! Reads + validates _table.json only (used for lightweight lookups). Returns
//! false when the file does not exist (the caller falls back to defaults).
bool TryReadTableManifest(FileSystem &fs, const string &manifest_path, TableManifest &manifest);

} // namespace duckdb