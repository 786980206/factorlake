#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// aligned_meta(table_name, root=...)
//
// Returns a single row of comprehensive metadata about an AlignedTable:
//   (table_name VARCHAR, table_path VARCHAR, partition_template VARCHAR,
//    total_rows BIGINT, group_count BIGINT, partition_count BIGINT,
//    part_count BIGINT, groups VARCHAR, partitions VARCHAR, schema VARCHAR)
//
// - table_name: the logical table name (directory name)
// - table_path: absolute path to the table directory
// - partition_template: the partition template (e.g. "year=%Y")
// - total_rows: total row count across all partitions (from index group)
// - group_count: number of column groups
// - partition_count: number of partitions
// - part_count: total number of parquet files across all groups
// - groups: group_name:column1,column2;group_name:column1,... (all groups)
// - partitions: partition_key1,partition_key2,... (from index group)
// - schema: column_name:type,column_name:type,... (full table schema from
//   index group + all non-index group columns)
unique_ptr<FunctionData> AlignedMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> AlignedMetaInitGlobal(ClientContext &context, TableFunctionInitInput &input);

void AlignedMetaFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
