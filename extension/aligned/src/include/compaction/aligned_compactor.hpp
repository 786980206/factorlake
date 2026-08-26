#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// aligned_compact(table_name, group_name, root=...)
// Compacts a column group's parts: for every partition directory with more
// than one part, merges them into a normalized set of parts (1M rows per part,
// last part ≤ 1M). Same partition value, same row range, same column set —
// schema evolution within a directory is rejected. Two-phase commit:
//  1. Stage all merged parts under <table>/_tmp/transaction-<txid>/
//  2. If all groups succeed: move staged parts into place + delete old files
// If any group fails: clean up _tmp, table state unchanged (old parts remain).
// Phase 1 (per-partition merge) is parallelized across partition directories;
// Phase 2 (move + delete) is serial. Returns (dirs_compacted, parts_before,
// parts_after).
unique_ptr<FunctionData> AlignedCompactBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedCompactInitGlobal(ClientContext &context, TableFunctionInitInput &input);

void AlignedCompactFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
