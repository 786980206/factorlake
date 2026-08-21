#pragma once

#include "duckdb.hpp"
#include "catalog/manifest.hpp"

namespace duckdb {

//! The input of one part rewrite (v7 read-modify-write).
//!
//! The merge model:
//!  - old rows stream in file order (part schema = the part's own footer);
//!  - inserts: rows to insert, each with a part-local POSITION (the first old
//!    row whose symbol >= the key's symbol; == part row count to append). The
//!    value columns hold the mapped columns of the group (in mapping order);
//!    part columns without a mapped value are written NULL. An empty value
//!    column list = NULL-row insert (a group without mapped columns).
//!  - updates: existing part-local ROW numbers with their new mapped values
//!    (merge semantics: only the mapped columns are overwritten, everything
//!    else keeps the old value; mapping columns not present in the part's own
//!    schema are a fail-fast violation);
//!  - deletes: part-local row numbers, ascending (rows are removed; the
//!    remaining rows keep their relative order).
//! All positions/rows refer to the OLD part's row numbering.
struct PartMergeInput {
	const PartInfo *part = nullptr; // existing part to rewrite; nullptr = fresh
	                                // part (all rows come from inserts)
	string staged_path; // staged output path (_tmp/...)
	vector<string> col_names; // output schema: part file columns (fresh part:
	                          // the mapping columns)
	vector<LogicalType> col_types; // aligned with col_names
	// Insert rows: ColumnDataCollection of [BIGINT pos, ...mapped columns].
	// Positions strictly ascending (the mutator sorts by symbol).
	ColumnDataCollection *inserts = nullptr;
	vector<string> insert_cols; // mapped column names per insert value column
	// Update rows: ColumnDataCollection of [BIGINT row, ...mapped columns],
	// rows ascending.
	ColumnDataCollection *updates = nullptr;
	vector<string> update_cols;
	// Deleted part-local rows, ascending.
	const vector<idx_t> *deletes = nullptr;
	idx_t rgs = 131072; // row group size for flushing
};

//! Rewrites one part (or builds a fresh part) applying the merge operations;
//! writes the result to input.staged_path. Returns the new part's row count
//! (0 = the part was deleted empty — the caller decides what to do).
//! Throws IOException on contract violations.
idx_t RewritePart(ClientContext &context, const PartMergeInput &input);

} // namespace duckdb