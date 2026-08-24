#pragma once

#include "duckdb.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

#include <atomic>
#include <map>
#include <mutex>
#include <string>
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
	idx_t partition_col_pos = 0;   // position in the *input* chunk
	string partition_template;     // "year=%Y" / "month=%Y-%m" / "date=%Y-%m-%d"
	string partition_col_name;     // e.g. "date"

	// Physical group directory: root/table_name/group_name
	string group_path;
	bool is_new_group = false;
	bool write_partition_column = true; // true for index group

	// The group's full column schema (from footer or inferred).
	vector<string> group_columns;
	vector<LogicalType> group_types;

	// Tuning constants.
	idx_t row_group_size = 131072;      // ALIGNED_DEFAULT_RG_ROWS
	idx_t row_groups_per_file = 8;      // → 1048576 rows per part

	// Column mapping: output col i → input col input_col_map[i].
	vector<idx_t> input_col_map;
	vector<string> input_names;
};

//===----------------------------------------------------------------------===//
// PartitionWriter: owns one partition's ParquetWriter + part-file rotation.
// Lives inside GlobalState. Only GlobalState's FlushManager touches it.
//===----------------------------------------------------------------------===//

struct PartitionWriter {
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
	std::atomic<idx_t> flushed_rows {0};    // rows flushed to ParquetWriter
	std::atomic<idx_t> written_rows {0};    // rows finalized to disk

	bool finalized = false;
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
	std::map<string, unique_ptr<PartitionWriter>> writers;
	std::set<string> cleaned_partitions;

	explicit AlignedCopyGlobalState(ClientContext &ctx, FileSystem &f, const AlignedCopyBindData &bd);

	// The **sole** entry point for flushing data to a partition's writer.
	// Called only from Combine (after local buffers are merged) and from
	// Finalize (for the last partial RG).  Sink never calls this directly.
	//
	// Responsibilities:
	//   1. Create the PartitionWriter on first use (cleans old files = OVERWRITE).
	//   2. writer->Flush(buffer) — writes one Row Group.
	//   3. Rotate part file at row_groups_per_file boundary.
	//
	// Thread safety: caller must hold `lock`.
	void Flush(const string &partition_key, ColumnDataCollection &buffer);

	// Finalize a single partition writer: flush footer, rename file,
	// update written_rows.  Called only from Finalize.
	void FinalizePartition(PartitionWriter &pw);

	// Rename the temp file to the self-describing name.
	void RenamePartFile(PartitionWriter &pw);
};

//===----------------------------------------------------------------------===//
// LocalState: per-thread partition buffers.  Sink only appends here; it
// never touches PartitionWriter or ParquetWriter.
//===----------------------------------------------------------------------===//

struct AlignedCopyLocalState : public LocalFunctionData {
	explicit AlignedCopyLocalState(ClientContext &context, const vector<LogicalType> &types);

	struct PartitionBuffer {
		ColumnDataCollection collection;
		ColumnDataAppendState append_state;
	};

	std::map<string, unique_ptr<PartitionBuffer>> buffers;
	const vector<LogicalType> &types;
	idx_t received_rows = 0; // local accounting

	// Get or create the buffer for a partition key.
	PartitionBuffer *GetBuffer(const string &partition_key, ClientContext &context);
};

//===----------------------------------------------------------------------===//

CopyFunction GetAlignedCopyFunction();

} // namespace duckdb
