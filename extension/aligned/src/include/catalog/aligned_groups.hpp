#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// aligned_groups(table_name, root=...)
//
// Lists all column groups in an AlignedTable. Returns one row per group:
//   (group_name VARCHAR, columns VARCHAR, partition_count BIGINT)
//
// - group_name: the group's directory path relative to the table root
//   ("index", "factor/alpha101", "fieldset/ma", ...).
// - columns: comma-separated column names from the group's schema
//   (discovered from the Parquet footer of the group's last part).
// - partition_count: number of partitions in this group (number of distinct
//   partition keys among the group's part files).
unique_ptr<FunctionData> AlignedGroupsBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedGroupsInitGlobal(ClientContext &context, TableFunctionInitInput &input);

void AlignedGroupsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
