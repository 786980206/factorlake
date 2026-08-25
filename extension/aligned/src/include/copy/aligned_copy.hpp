#pragma once

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"

#include <atomic>
#include <limits>
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

	// Symbol column position in the *input* chunk (for per-partition sort).
	// -1 = symbol column not present in input (non-index group without key cols).
	idx_t symbol_col_pos = DConstants::INVALID_INDEX;

	// Physical group directory: root/table_name/group_name
	string group_path;
	bool is_new_group = false;

	// The group's full column schema (from footer or inferred).
	vector<string> group_columns;
	vector<LogicalType> group_types;

	// Tuning constants.
	idx_t row_groups_per_file = 8;      // → 1048576 rows per part

	// Column mapping: output col i → input col input_col_map[i].
	vector<idx_t> input_col_map;
	vector<string> input_names;
	vector<LogicalType> input_types;  // types of all input columns
};

//===----------------------------------------------------------------------===//
// PartitionWriter: owns one partition's ParquetWriter + part-file rotation.
// Lives inside GlobalState. Only GlobalState's FlushManager touches it.
//===----------------------------------------------------------------------===//

struct PartitionWriter {
	~PartitionWriter() {
		delete accumulated;
	}

	// Configuration (set once at creation).
	string partition_key; // e.g. "year=1990"
	string part_dir;      // e.g. ".../index/year=1990"

	// Per-partition lock: allows parallel flushing of *different* partitions.
	// Only same-partition writes serialize.
	std::mutex lock;

	// The current part file.
	unique_ptr<ParquetWriter> writer;
	unique_ptr<ParquetWriteTransformData> transform_data;
	idx_t part_index = 0;
	idx_t rows_in_current_part = 0;
	idx_t row_groups_in_current_part = 0;

	// Accounting (atomic for cross-thread visibility).
	std::atomic<idx_t> received_rows {0};   // rows routed to this partition
	std::atomic<idx_t> written_rows {0};    // rows finalized to disk

	bool finalized = false;

	// Per-partition accumulated buffer.  Threads append their sorted
	// partition data here in Combine; Finalize sorts + flushes each to
	// ParquetWriter.  This ensures global sort order across all threads.
	ColumnDataCollection *accumulated = nullptr;  // lazily created
	std::mutex accum_lock;
};

//===----------------------------------------------------------------------===//
// GlobalState: the single owner of all PartitionWriters and the only place
// that calls ParquetWriter methods (Flush / Finalize).  The FlushManager
// inside it is the **sole** write path.
//===----------------------------------------------------------------------===//

struct AlignedCopyGlobalState : public GlobalFunctionData {
	ClientContext &context;
	FileSystem &fs;
	const AlignedCopyBindData &bind_data;

	std::mutex lock;
	std::unordered_map<string, unique_ptr<PartitionWriter>> writers;
	std::set<string> cleaned_partitions;

	explicit AlignedCopyGlobalState(ClientContext &ctx, FileSystem &f, const AlignedCopyBindData &bd);

	// Accumulate sorted partition data from a thread's Combine.
	// Does NOT call ParquetWriter — data is buffered for Finalize.
	void Accumulate(const string &partition_key, ColumnDataCollection &sorted_data,
	                idx_t rows);

	// Finalize a single partition writer: flush footer, rename file,
	// update written_rows.  Called only from Finalize.
	void FinalizePartition(PartitionWriter &pw);

	// Rename the temp file to the self-describing name.
	void RenamePartFile(PartitionWriter &pw);
};

//===----------------------------------------------------------------------===//
// AlignedPartitionedColumnData: custom PartitionedColumnData that computes
// partition indices by evaluating a partition template on a date/timestamp
// column. Replaces manual run detection + per-partition CDC buffers.
//===----------------------------------------------------------------------===//

class AlignedPartitionedColumnData : public PartitionedColumnData {
public:
	AlignedPartitionedColumnData(ClientContext &context, vector<LogicalType> types,
	                              idx_t partition_col_pos, string partition_template,
	                              bool is_timestamp);

	void ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) override;

	//! Get the partition key string for a partition index (for flush lookup).
	const string &GetPartitionKey(idx_t partition_id) const {
		auto it = partition_keys.find(partition_id);
		if (it != partition_keys.end()) {
			return it->second;
		}
		throw InternalException("AlignedPartitionedColumnData: unknown partition id %llu", partition_id);
	}

	//! Get all partition ids that have data.
	std::vector<idx_t> GetActivePartitionIds() const;

private:
	idx_t RegisterPartition(const string &key, PartitionedColumnDataAppendState &state);

	idx_t partition_col_pos;
	string partition_template;
	bool is_timestamp;

	// Partition key string → partition id
	std::unordered_map<string, idx_t> key_to_id;
	// Partition id → partition key string
	std::map<idx_t, string> partition_keys;

	// Per-row partition key cache (date_val → partition key string)
	std::unordered_map<int64_t, string> key_cache;
	int64_t fast_cache_date = std::numeric_limits<int64_t>::min();
	string fast_cache_key;
};

//===----------------------------------------------------------------------===//
// LocalState: per-thread partition buffers using PartitionedColumnData.
//===----------------------------------------------------------------------===//

struct AlignedCopyLocalState : public LocalFunctionData {
	explicit AlignedCopyLocalState(ClientContext &context, const vector<LogicalType> &types);

	// PartitionedColumnData: handles all partitioning internally.
	unique_ptr<AlignedPartitionedColumnData> part_data;
	unique_ptr<PartitionedColumnDataAppendState> part_append_state;
};

//===----------------------------------------------------------------------===//

CopyFunction GetAlignedCopyFunction();

} // namespace duckdb
