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

// _table.json — the only manifest file of a logical table.
// NOTE: row_count is written by the writer for bookkeeping but is IGNORED by
// the reader: the total row count is derived from the Parquet footers
// (Σ part rows, identical across all groups — the alignment contract).
struct TableManifest {
	string name;
	int64_t version = 1;
	int64_t schema_version = 1;
	vector<string> key;
	string canonical_order = "fixed";
	idx_t row_count = 0;
	idx_t row_group_size = 131072;
	idx_t part_rows = 0; // optional hint for the target part size (default 4194304)
	idx_t last_txid = 0; // last transaction id (writer/compactor bookkeeping; markers are gone)
	vector<string> groups;
	// Optional explicit partition templates per group (group -> templates).
	// Empty when partitioning must be derived from the directory layout
	// (only possible when the table already has parts).
	case_insensitive_map_t<vector<PartitionTemplate>> partitioning;
};

// A part file. Metadata (row count, columns) is read from the Parquet footer
// at plan time — there is no sidecar anymore. start_row is derived by
// accumulating footer row counts over parts sorted by (partition dir, part id).
struct PartInfo {
	string path; // absolute path to the parquet file
	string part_name;
	idx_t start_row = 0;
	idx_t row_count = 0;
	vector<string> columns; // column names in FILE schema order
};

// A resolved, validated Column Group.
struct GroupPlan {
	string group_path; // absolute path of the group directory
	GroupManifest manifest;
	vector<PartInfo> parts; // sorted by row order, validated to tile [0, table row_count)
	vector<string> column_order; // union of part columns (physical names), first-seen order
	vector<idx_t> output_positions; // table output position per column_order entry
	string lv1; // first path level of the group ("factor"); empty for "index"
	string lv2; // second path level ("alpha101"); empty for "index"
};

// The fully resolved scan plan of one logical table.
struct TablePlan {
	string table_path; // absolute path
	TableManifest table;
	vector<GroupPlan> groups;
};

//! Resolves a logical table: reads _table.json, discovers the column groups,
//! globs the part files, reads part metadata (row count + columns) from the
//! Parquet footers, derives the partitioning (explicit from _table.json, or
//! from the directory layout), sorts the parts into row order, accumulates
//! start_row, validates every group tiles [0, row_count) and that all groups
//! agree on the total row count (the alignment contract). Throws IOException
//! on any contract violation.
void BuildTablePlan(ClientContext &context, const string &root_path, const string &table_name, TablePlan &plan);

//! Reads + validates _table.json only (used for lightweight lookups).
TableManifest ReadTableManifest(FileSystem &fs, const string &manifest_path);

} // namespace duckdb