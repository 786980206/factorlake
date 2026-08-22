#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// aligned_create(table_name, group_name, columns, root=..., partition_template=...)
//
// Creates or extends an AlignedTable on disk via a table function.
//
// Parameters (positional):
//   table_name   — logical table name (subdirectory of root)
//   group_name   — column group path: "index" or "lv1/lv2" (e.g. "factor/alpha")
//   columns      — column definition string, e.g. "symbol VARCHAR, date DATE,
//                  close DOUBLE, alpha001 DOUBLE"
//
// Named parameters:
//   root                — data root directory (default: aligned_data_root)
//   partition_template  — partition template (default: month=%Y-%m)
//                         Only used when group_name="index" (new table creation)
//
// Behavior:
//   - group_name="index": create a NEW table. First two columns must be
//     (symbol VARCHAR, date DATE/TIMESTAMP). All columns go into the index
//     group. A placeholder parquet is written so the reader can discover
//     the schema.
//   - group_name!="index": EXTEND an existing table by adding a new column
//     group. The columns define this group's schema. A placeholder parquet
//     (N all-NULL rows, N = index partition row count) is written in every
//     existing partition to satisfy the partition-aligned contract.
//
// Returns one row: (dirs_created BIGINT, files_created BIGINT, txid BIGINT)
unique_ptr<FunctionData> AlignedCreateBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedCreateInitGlobal(ClientContext &context, TableFunctionInitInput &input);

void AlignedCreateFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
