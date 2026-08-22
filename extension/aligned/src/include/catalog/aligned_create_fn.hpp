#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// aligned_create(table_name, columns, groups, root=..., partition_template=...)
//
// Creates an AlignedTable on disk via a table function (instead of CREATE TABLE
// DDL). Creates the table directory, column-group subdirectories, and writes
// one empty (0-row) placeholder parquet per group so the reader can discover
// the schema from the footer.
//
// Parameters (positional):
//   table_name    — logical table name (subdirectory of root)
//   columns       — column definition string, e.g. "symbol VARCHAR, date DATE,
//                   close DOUBLE, alpha001 DOUBLE"
//   groups        — column→group mapping (optional), e.g.
//                   "index:close;factor/alpha:alpha001". Columns not listed
//                   default to the index group. May be empty.
//
// Named parameters:
//   root                 — data root directory (default: aligned_data_root)
//   partition_template   — partition template (default: month=%Y-%m)
//
// Rules:
//   - First two columns must be (symbol VARCHAR, date DATE/TIMESTAMP) — v8 PK.
//   - Non-index group names must be lv1/lv2 two-level paths.
//
// Returns one row: (dirs_created BIGINT, files_created BIGINT, txid BIGINT)
unique_ptr<FunctionData> AlignedCreateBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedCreateInitGlobal(ClientContext &context, TableFunctionInitInput &input);

void AlignedCreateFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
