#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "catalog/manifest.hpp"

namespace duckdb {

// aligned_table(name, root=...) / aligned_scan(root, name) table function.
unique_ptr<FunctionData> AlignedBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedInitGlobal(ClientContext &context, TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> AlignedInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *gstate);

OperatorResultType AlignedScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

unique_ptr<NodeStatistics> AlignedCardinality(ClientContext &context, const FunctionData *bind_data);

} // namespace duckdb
