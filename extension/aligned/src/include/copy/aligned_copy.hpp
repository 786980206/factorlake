#pragma once

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <thread>
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
// PerPartitionState: owned by exactly one FlushWorker.  No locks needed
// because each partition is assigned to one worker via affinity.
//===----------------------------------------------------------------------===//

struct PerPartitionState {
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
// FlushJob: a buffer to be encoded + written by a background thread.
//===----------------------------------------------------------------------===//

struct FlushJob {
	string partition_key;
	unique_ptr<ColumnDataCollection> buffer;
	bool is_sentinel = false;  // true = stop signal
};

//===----------------------------------------------------------------------===//
// FlushWorker: one background thread + its private state.
//
// Each worker owns:
//  - A FIFO work queue (mutex + condvar for blocking pop)
//  - Per-partition ParquetWriters (unordered_map, thread-local, NO locks)
//  - Error capture (exception_ptr)
//
// Partition affinity: each partition is assigned to exactly one worker
// (round-robin via atomic counter in GlobalState).  This guarantees:
//  - No two threads write to the same partition → no file conflicts
//  - Buffers for a partition are processed in FIFO order → order preserved
//===----------------------------------------------------------------------===//

struct FlushWorker {
	// Work queue (protected by queue_lock + queue_cv)
	std::mutex queue_lock;
	std::condition_variable queue_cv;
	std::deque<FlushJob> queue;

	// Thread-local per-partition state (NO locks — only this thread accesses)
	std::unordered_map<string, unique_ptr<PerPartitionState>> partitions;

	// Per-partition pending buffers: accumulated until all data arrives,
	// then sorted by (symbol, date) and flushed in order.
	std::unordered_map<string, vector<unique_ptr<ColumnDataCollection>>> pending;

	// Error capture
	std::exception_ptr error;
};

//===----------------------------------------------------------------------===//
// GlobalState: owns the thread pool + partition assignment.
//===----------------------------------------------------------------------===//

struct AlignedCopyGlobalState : public GlobalFunctionData {
	ClientContext &context;
	FileSystem &fs;
	const AlignedCopyBindData &bind_data;

	// Thread pool
	std::vector<unique_ptr<FlushWorker>> workers;
	std::vector<std::thread> threads;
	std::atomic<idx_t> next_thread_id {0};
	idx_t num_workers = 0;

	// Partition -> worker assignment (accessed from multiple Sink threads
	// in PARALLEL_COPY_TO_FILE mode, so protected by partition_lock).
	std::unordered_map<string, idx_t> partition_to_thread;
	std::mutex partition_lock;  // protects partition_to_thread

	explicit AlignedCopyGlobalState(ClientContext &ctx, FileSystem &f, const AlignedCopyBindData &bd);
	~AlignedCopyGlobalState();

	// Assign a partition to a worker (round-robin).  Called from Sink thread.
	idx_t AssignThread(const string &partition_key);

	// Push a flush job to the assigned worker's queue.
	void PushJob(FlushJob &&job, idx_t worker_id);

	// Push a sentinel (stop signal) to all workers.  Called from Combine.
	void PushSentinels();

	// Join all background threads.  Called from Finalize.
	void JoinThreads();

	// Background worker loop.
	void WorkerLoop(FlushWorker &worker);

	// Helper: initialize a partition (clean old files, create dir, writer).
	void InitPartition(PerPartitionState &pp);

	// Helper: flush a buffer to the partition's ParquetWriter.
	void FlushToPartition(PerPartitionState &pp, ColumnDataCollection &buffer);

	// Helper: merge all pending buffers for a partition into a single
	// ColumnDataCollection, sort by (symbol, date), and flush.
	void SortAndFlushPartition(PerPartitionState &pp,
	                            vector<unique_ptr<ColumnDataCollection>> &buffers);

	// Helper: rotate part file when RG count reaches threshold.
	void RotatePartition(PerPartitionState &pp);

	// Helper: finalize the last part file (footer + rename).
	void FinalizePartition(PerPartitionState &pp);

	// Helper: rename temp file to self-describing name.
	void RenamePartFile(PerPartitionState &pp);
};

//===----------------------------------------------------------------------===//
// LocalState: per-thread state.  In PARALLEL_COPY_TO_FILE mode, multiple
// threads call Sink concurrently, each with its own LocalState.  Per-thread
// per-partition buffers avoid contention.  The source reader (parquet scan)
// runs in parallel, giving ~3x speedup over single-threaded source reading.
//===----------------------------------------------------------------------===//

struct AlignedCopyLocalState : public LocalFunctionData {
	explicit AlignedCopyLocalState(ClientContext &context, const vector<LogicalType> &types);

	// Scratch buffer for projecting input chunk -> group schema.
	DataChunk projected_chunk;

	// Per-partition RG buffer (thread-local, no locks needed).
	std::unordered_map<string, unique_ptr<ColumnDataCollection>> rg_buffers;
	std::unordered_map<string, unique_ptr<ColumnDataAppendState>> rg_appends;

	// Partition key cache: avoids calling EvaluatePartitionTemplate for
	// every row.  Since input is sorted by date, consecutive rows usually
	// have the same partition key.  Cache hit = integer compare only.
	int64_t cached_date_val = std::numeric_limits<int64_t>::min();
	string cached_pk;
};

//===----------------------------------------------------------------------===//

CopyFunction GetAlignedCopyFunction();

} // namespace duckdb
