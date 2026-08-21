#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "catalog/manifest.hpp"

namespace duckdb {

//! Evaluates a partition template ("date=%Y-%m-%d") against a date value.
//! Supports %Y, %m, %d (zero-padded). Returns false when the template uses
//! unsupported specifiers or has no specifiers.
bool EvaluatePartitionTemplate(const string &template_str, date_t value, string &result);

//! Derives the partition schema of a group from its part file paths. Only the
//! fixed segment kinds are recognized: year=YYYY, month=YYYY-MM and
//! date=YYYY-MM-DD (segment name -> template). Other name=value segments are
//! ignored. The partition source column (v6) is the index schema's
//! DATE/TIMESTAMP field among its first two columns — bound by the caller
//! (falls back to "date" when empty). Throws IOException when parts of the
//! same group disagree on a segment's format. Returns an empty list when no
//! recognized segment exists (an unpartitioned group).
vector<PartitionTemplate> DerivePartitioningFromPaths(const vector<string> &paths, const string &table_name,
                                                      const string &group_name, const string &source_column);

//! Prunes a part list by a constant comparison filter on a partition source
//! column (contract §4). The templates must be the prefix of the group's
//! partitioning list that is sourced from the filtered column.
//!  - equality: keeps parts whose directory path contains the exact dir path
//!  - ranges:   keeps parts whose partition date (reconstructed from the dir
//!              values) satisfies the comparison; when the date cannot be
//!              reconstructed (e.g. month-level partitioning), no pruning
//! Returns the input list unchanged when no pruning is possible.
vector<PartInfo> PrunePartsByFilter(const vector<PartInfo> &parts, const vector<PartitionTemplate> &templates,
                                    const ConstantFilter &filter);

} // namespace duckdb
