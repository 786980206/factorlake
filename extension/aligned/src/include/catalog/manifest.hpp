#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// One entry of _group.json "partitioning": a directory template
// (e.g. "year=%Y" sourced from logical column "date").
struct PartitionTemplate {
	string template_str;
	string source;
};

// _group.json
struct GroupManifest {
	string group;
	idx_t row_count = 0;
	idx_t row_group_size = 131072;
	vector<PartitionTemplate> partitioning;
};

// _table.json
struct TableManifest {
	string name;
	int64_t version = 1;
	int64_t schema_version = 1;
	vector<string> key;
	string canonical_order = "fixed";
	// Whether the column groups are guaranteed position-aligned on the same
	// Logical Row Space (contract §3). When true (default), partition/row-group
	// pruning results from the different leaves map to a unified physical-group
	// coordinate and are intersected into one global scan range. When false,
	// each leaf prunes and plans independently and pruning must NOT be
	// propagated across leaves via intersection.
	bool aligned = true;
	idx_t row_count = 0;
	idx_t row_group_size = 131072;
	vector<string> groups;
};

// <part>.aligned.json sidecar
struct PartInfo {
	string path; // absolute path to the parquet file (filled by the resolver)
	string part_name;
	idx_t start_row = 0;
	idx_t row_count = 0;
	idx_t row_group_size = 131072;
	vector<string> columns; // column names in FILE schema order
};

// A resolved, validated Column Group.
struct GroupPlan {
	string group_path; // absolute path of the group directory
	GroupManifest manifest;
	vector<PartInfo> parts; // sorted by start_row, validated to tile [0, row_count)
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

//! Reads + validates _table.json, _group.json and all part sidecars, globs
//! part files, checks commit markers and validates the row space (contract §10.1).
//! Throws IOException on any contract violation.
void BuildTablePlan(ClientContext &context, const string &root_path, const string &table_name, TablePlan &plan);

//! Reads + validates _table.json only (used for lightweight lookups).
TableManifest ReadTableManifest(FileSystem &fs, const string &manifest_path);

} // namespace duckdb
