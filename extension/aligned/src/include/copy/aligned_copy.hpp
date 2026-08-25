#pragma once

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

class ParquetWriter;
class ParquetWriteTransformData;

//===----------------------------------------------------------------------===//
// Bind data: resolved at bind time, immutable during execution.
//===----------------------------------------------------------------------===//

struct AlignedCopyBindData : public TableFunctionData {
	string table_name;   // logical table name (e.g. "cnstk_ixday")
	string group_name;   // "index" or "panel/ma"
	string root;         // resolved data root

	// The output schema written to parquet (after column pruning + reordering
	// to group schema order).
	vector<string> column_names;
	vector<LogicalType> sql_types;

	// Partition configuration (from the index group).
	idx_t partition_col_pos = DConstants::INVALID_INDEX;   // position in the *input* chunk
	string partition_template;     // "year=%Y" / "month=%Y-%m" / "date=%Y-%m-%d"
	string partition_col_name;     // e.g. "date"

	// Symbol column position in the *input* chunk.
	idx_t symbol_col_pos = DConstants::INVALID_INDEX;

	// Physical group directory: root/table_name/group_name
	string group_path;
	bool is_new_group = false;

	// The group's full column schema (from footer or inferred).
	vector<string> group_columns;
	vector<LogicalType> group_types;

	// Tuning constants.
	idx_t row_groups_per_file = 8;      // -> 1048576 rows per part

	// Column mapping: output col i -> input col input_col_map[i].
	vector<idx_t> input_col_map;
	vector<string> input_names;
	vector<LogicalType> input_types;  // types of all input columns

	bool is_timestamp = false;  // partition column is TIMESTAMP (not DATE)
};

//===----------------------------------------------------------------------===//
// PartitionWriter: owns one partition's ParquetWriter + part-file rotation.
// Created lazily in Sink. Finalize iterates all writers and finalizes
// them in parallel via a thread pool.
//===----------------------------------------------------------------------===//

struct PartitionWriter {
	// Configuration (set once at creation).
	string partition_key; // e.g. "year=1990"
	string part_dir;      // e.g. ".../index/year=1990"

	// The current part file.
	unique_ptr<ParquetWriter> writer;
	unique_ptr<ParquetWriteTransformData> transform_data;
	idx_t part_index = 0;
	idx_t rows_in_current_part = 0;
	idx_t row_groups_in_current_part = 0;

	// Accounting.
	idx_t received_rows = 0;
	idx_t written_rows = 0;

	bool finalized = false;
};

//===----------------------------------------------------------------------===//
// GlobalState: owns all PartitionWriters. Sink calls Flush directly
// (single-threaded, REGULAR_COPY_TO_FILE). Finalize finalizes in parallel.
//===----------------------------------------------------------------------===//

struct AlignedCopyGlobalState : public GlobalFunctionData {
	ClientContext &context;
	FileSystem &fs;
	const AlignedCopyBindData &bind_data;

	std::mutex lock;
	std::unordered_map<string, unique_ptr<PartitionWriter>> writers;
	std::set<string> cleaned_partitions;

	explicit AlignedCopyGlobalState(ClientContext &ctx, FileSystem &f, const AlignedCopyBindData &bd);

	// Flush a ColumnDataCollection to a partition's ParquetWriter.
	// Called from Sink (single-threaded). Creates the writer + cleans old
	// files on first use. Handles RG rotation.
	void Flush(const string &partition_key, ColumnDataCollection &buffer);

	// Finalize a single partition writer: flush footer, rename file,
	// update written_rows.
	void FinalizePartition(PartitionWriter &pw);

	// Rename the temp file to the self-describing name.
	void RenamePartFile(PartitionWriter &pw);
};

//===----------------------------------------------------------------------===//
// LocalState: per-thread state. In REGULAR_COPY_TO_FILE mode only one
// thread calls Sink, so this is essentially the single execution context.
//===----------------------------------------------------------------------===//

struct AlignedCopyLocalState : public LocalFunctionData {
	explicit AlignedCopyLocalState(ClientContext &context, const vector<LogicalType> &types);

	// Scratch buffer for projecting input chunk -> group schema.
	DataChunk projected_chunk;

	// Per-partition RG buffer.  Keyed by partition key string.
	// Accumulates projected rows until RG_SIZE is reached, then flushed.
	std::unordered_map<string, unique_ptr<ColumnDataCollection>> rg_buffers;
	std::unordered_map<string, unique_ptr<ColumnDataAppendState>> rg_appends;
};

//===----------------------------------------------------------------------===//

CopyFunction GetAlignedCopyFunction();

} // namespace duckdb
