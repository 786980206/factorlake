#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// aligned_write(table_name, source_path, mapping, root=..., start_row=...)
// Appends rows to an AlignedTable from a source parquet file (Phase 5):
//  - mapping: "group:col1,col2;group2:col3,..." — which source columns each
//    column group receives (physical column names, in file schema order)
//  - rows are position-aligned: source row i -> logical row start_row + i
//    (the source must already be in canonical order; start_row defaults to
//    the current table end — append only)
//  - partition directories are derived from each group manifest's templates
//    against the source's partition column (DATE; the value may change per
//    row — parts are split at partition changes)
//  - writes staged under <table>/_tmp/transaction-<txid>/, then commits:
//    move part + sidecar into place, update the partition dir's commit
//    marker (contract §9), bump _table.json / _group.json row_count
// Returns one row: (rows_written, parts_written, txid).
unique_ptr<FunctionData> AlignedWriteBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedWriteInitGlobal(ClientContext &context, TableFunctionInitInput &input);

void AlignedWriteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
