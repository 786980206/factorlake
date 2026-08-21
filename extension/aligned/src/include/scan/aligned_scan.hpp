#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "catalog/manifest.hpp"

namespace duckdb {

//! Bind data of aligned_table()/aligned_scan(): the built table plan plus the
//! resolved output schema. Defined here so catalog integration
//! (AlignedTableEntry::GetScanFunction) can build/cast it too.
struct AlignedTableBindData : public TableFunctionData {
	TablePlan plan;
	vector<string> names;
	vector<LogicalType> types;
	idx_t total_rows = 0;
};

//! Bind without going through TableFunctionBindInput (used by the attached
//! catalog's table entries).
unique_ptr<FunctionData> AlignedBindForCatalog(ClientContext &context, const string &root, const string &table,
                                               vector<LogicalType> &return_types, vector<string> &names);

// aligned_table(name, root=...) / aligned_scan(root, name) table function.
unique_ptr<FunctionData> AlignedBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedInitGlobal(ClientContext &context, TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> AlignedInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *gstate);

// Note: DuckDB 1.5's table_function_t returns void; the end of the scan is
// signaled by setting output.SetCardinality(0).
void AlignedScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

unique_ptr<NodeStatistics> AlignedCardinality(ClientContext &context, const FunctionData *bind_data);

} // namespace duckdb

