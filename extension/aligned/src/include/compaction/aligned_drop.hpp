#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// aligned_drop(table_name, group_name, root=...)
//
// Drops a column group (or the entire table) from an AlignedTable:
//  - group_name = "index"  → deletes the entire table directory (all groups)
//  - group_name = <other>  → deletes only that column group's directory tree
//
// Acquires the table-level write lock (TableWriteLock) for mutual exclusion
// with concurrent writers. Mints a NextTransactionId for the return value
// (no _tmp/ staging — drop deletes directories directly).
//
// Returns one row: (dirs_removed BIGINT, files_removed BIGINT, txid BIGINT)
unique_ptr<FunctionData> AlignedDropBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedDropInitGlobal(ClientContext &context, TableFunctionInitInput &input);

void AlignedDropFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
