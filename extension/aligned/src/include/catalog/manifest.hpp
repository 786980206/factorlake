#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Default Row Group size for Parquet writes (compile-time constant).
constexpr idx_t ALIGNED_DEFAULT_RG_ROWS = 131072;

//! Default soft target rows per Parquet part file (compile-time constant).
//! When an upsert appends rows to an existing partition, the mutator prefers
//! growing the partition's last existing part in-place while its row count is
//! below this threshold; once a part reaches it, the next append creates a
//! new part instead (the part is NOT hard-truncated — a single batch may
//! overshoot by a small amount). The value is ~8 Row Groups (8 * 131072),
//! yielding ~256MB–1GB files for typical column widths.
constexpr idx_t ALIGNED_DEFAULT_PART_ROWS = 1048576;

// One entry of the partitioning config (derived from the directory layout):
// a directory template (e.g. "date=%Y-%m-%d") sourced from the logical "date"
// column. Only three partition kinds are supported:
//   year=%Y, month=%Y-%m, date=%Y-%m-%d.
struct PartitionTemplate {
	string template_str;
	string source;
};

// Column-group metadata. The group name is the directory path relative to the
// table root ("index", "factor/alpha101", ...). The partitioning templates are
// derived from the partition directory structure at plan time.
struct GroupManifest {
	string group;
	vector<PartitionTemplate> partitioning;
};

// A part file. The part order is the lexicographic order of the file's
// normalized relative path (partition directory then file name) — the index in
// that sorted list IS the part id. Part file names are self-describing
// (v6): "{idx:04d}-{rows:10d}.parquet" — the first 4 digits are the part's
// index within its partition (0000-based; non-index groups may skip indexes),
// the last 10 digits are the file's TOTAL row count. Row intervals are derived
// from file names only (no footer reads at plan time):
//   start_row = partition.start_row + sum(rows of lower-index parts)
//   row_count = file-name rows
// The index group's indexes must be consecutive from 0000; groups sharing a
// partition must agree on the partition total AND on every shared index's row
// count. At scan time the open reader's footer row count is verified against
// the file-name value (defensive). Per-part column metadata is NOT stored: the
// group schema comes from the group's last part and per-file columns are read
// from the footer at scan time.
struct PartInfo {
	string path; // absolute path to the parquet file
	string part_name; // file base name (without directory)
	idx_t start_row = 0;
	idx_t row_count = 0;
	string partition_key; // partition value ("2026-08"); "" when unpartitioned
	idx_t partition_index = 0; // index parsed from the file name (partition-local)
	idx_t partition_parts = 0; // total parts in the partition (1 => last part)
};

// One partition of a group: a contiguous run of parts sharing the same key.
struct GroupPartition {
	string key;        // partition value; "" when unpartitioned
	idx_t start_row = 0; // logical row where the partition begins (index-defined)
	idx_t row_count = 0; // sum of the partition's file-name row counts
	idx_t first_part = 0; // index into GroupPlan.parts
	idx_t part_count = 0; // number of parts in this partition
};

// A resolved, validated Column Group.
struct GroupPlan {
	string group_path; // absolute path of the group directory
	GroupManifest manifest;
	vector<PartInfo> parts; // sorted in row order; a partition-subset group may
	                        // NOT tile [0, table row_count) (missing ranges are
	                        // NULL-filled at scan time)
	vector<GroupPartition> partitions; // sorted by row order (key order)
	vector<string> column_order; // GROUP schema = the group's last part's columns
	vector<LogicalType> schema_types; // aligned with column_order
	vector<idx_t> output_positions; // table output position per column_order entry
	string lv1; // first path level of the group ("factor"); empty for "index"
	string lv2; // second path level ("alpha101"); empty for "index"
	string partition_source; // the index schema's DATE/TIMESTAMP column among the
	                         // first two fields — the partition source column
	string symbol_column; // v7: the index schema's OTHER column among the first
	                      // two fields — the primary-key symbol column. The
	                      // index schema's first two columns ARE the primary
	                      // key (date_col, symbol_col): exactly one of them is
	                      // DATE/TIMESTAMP (the partition source), the other is
	                      // the symbol column. Rows are ordered by partition
	                      // (date ascending) then by symbol ascending within a
	                      // partition (writer-side sort contract; the reader
	                      // never validates ordering, only alignment).
	bool full_coverage = false; // partition keys == index keys (participates in
	                            // active-interval intersection at scan time)
};

// The fully resolved scan plan of one logical table.
struct TablePlan {
	string table_path; // absolute path
	string table_name; // the logical table name (directory name)
	vector<GroupPlan> groups;
	idx_t row_count = 0; // total row count (index partitions, from file names)
};

//! Resolves a logical table: discovers the column groups from the file layout
//! (ONE glob), groups the parts by partition key (single-level year=/month=/date=),
//! parses each part file name ("{idx:04d}-{rows:10d}.parquet") for its index and
//! row count, enforces the partition-aligned contract (group keys ⊆ index keys,
//! equal per-key partition totals, equal per-shared-index row counts, index
//! indexes consecutive from 0000, index schema's first two columns contain a
//! DATE/TIMESTAMP field), assigns start_row by cumulative file-name rows, and
//! validates every partition's parts tile its row range. Only ONE footer read
//! per group (the group's last part, for the schema). Throws IOException on
//! any contract violation. A table with no part files (empty table) is not a
//! valid table — the index group is mandatory and discovered via glob.
void BuildTablePlan(ClientContext &context, const string &root_path, const string &table_name, TablePlan &plan);

} // namespace duckdb