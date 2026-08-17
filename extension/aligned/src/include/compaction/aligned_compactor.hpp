#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// aligned_compact(table_name, group_name, root=...)
// Compacts a column group's parts (Phase 7): for every partition directory
// with more than one part, merges them into a single part (same partition
// value, same row range, same column set — schema evolution within a
// directory is rejected). The new part is staged under
// <table>/_tmp/transaction-<txid>/, then committed atomically:
//  1. write the new part + sidecar
//  2. replace the directory's commit marker with the new part name
//  3. delete the old part files + sidecars
// A crash before step 2 leaves the old parts visible (the staged part is
// discarded); after step 2 the new part is authoritative and the old files
// are simply orphaned (invisible, cleaned on the next compaction).
// Returns one row: (dirs_compacted, parts_before, parts_after).
unique_ptr<FunctionData> AlignedCompactBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedCompactInitGlobal(ClientContext &context, TableFunctionInitInput &input);

void AlignedCompactFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
