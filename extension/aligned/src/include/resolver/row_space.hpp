#pragma once

#include "duckdb.hpp"
#include "catalog/manifest.hpp"

namespace duckdb {

//! Validates that parts tile [0, row_count) exactly, with no gaps and no
//! overlaps (contract §7 invariant). Throws IOException on violation.
//! Input parts must already be sorted by start_row.
void ValidateRowSpace(const string &table_name, const string &group_name, idx_t row_count,
                      const vector<PartInfo> &parts);

} // namespace duckdb
