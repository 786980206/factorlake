#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "catalog/manifest.hpp"

namespace duckdb {

//! Scan state is organized in three tiers (all defined in aligned_scan.cpp):
//!
//! 1. AlignedTableBindData (bind time): the TablePlan, output schema (names/types),
//!    total row count, and the virtual partition column config.
//! 2. AlignedScanGlobalState (global, per-query): row-space intervals after
//!    partition pruning, shared cursor, per-group open parts/readers, and
//!    compiled filter → group mappings.
//! 3. AlignedScanLocalState (local, per-thread): per-group scan cursors
//!    (rg_window, carry buffer) and the scratch DataChunk for filter paths.

//! Bind data of aligned_scan(): the built table plan plus the resolved output
//! schema. Defined here so catalog integration (AlignedTableEntry::GetScanFunction)
//! can build/cast it too.
struct AlignedTableBindData : public TableFunctionData {
	TablePlan plan;
	vector<string> names;
	vector<LogicalType> types;
	idx_t total_rows = 0;
	//! Owning catalog entry when scanned through an attached table;
	//! nullptr for direct aligned_scan() calls. Used by get_bind_info so the
	//! binder treats the scan as a real base table.
	class TableCatalogEntry *catalog_entry = nullptr;
	//! Virtual partition column (e.g. "year" of type VARCHAR). Populated when
	//! the table's partition template prefix does not collide with an existing
	//! column name. When set, the scan materializes the partition key value
	//! (e.g. "2024") for every row and filters on it are used for partition
	//! pruning. Empty name = no virtual partition column.
	string partition_col_name;
	idx_t partition_col_idx = DConstants::INVALID_INDEX; // position in names/types
};

//! Bind without going through TableFunctionBindInput (used by the attached
//! catalog's table entries).
unique_ptr<FunctionData> AlignedBindForCatalog(ClientContext &context, const string &root, const string &table,
                                               vector<LogicalType> &return_types, vector<string> &names);

// aligned_scan(table_name, root=...) table function.
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


