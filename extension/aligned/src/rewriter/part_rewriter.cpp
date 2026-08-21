#include "rewriter/part_rewriter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "parquet_field_id.hpp"
#include "parquet_shredding.hpp"
#include "zstd_file_system.hpp"

namespace duckdb {

namespace {

constexpr idx_t INF_POS = NumericLimits<idx_t>::Maximum();

//! Sequential row cursor over a ColumnDataCollection. The merge consumes the
//! collection rows in ascending order (inserts/updates are sorted by position
//! by the caller). The FIRST column of every row holds its position (BIGINT).
struct RowCursor {
	ColumnDataCollection *coll = nullptr;
	ColumnDataScanState scan_state;
	DataChunk chunk;
	idx_t chunk_rows = 0;
	idx_t chunk_start = 0;
	idx_t local = 0;
	bool done = true;

	void Init(ClientContext &context, ColumnDataCollection &collection) {
		coll = &collection;
		chunk.Initialize(context, collection.Types());
		coll->InitializeScan(scan_state);
		done = false;
		Fetch(context);
	}

	void Fetch(ClientContext &context) {
		coll->Scan(scan_state, chunk);
		chunk_rows = chunk.size();
		local = 0;
		if (chunk_rows == 0) {
			done = true;
		}
	}

	//! The position (first column) of the next unread row.
	idx_t CurrentPos() const {
		D_ASSERT(!done);
		return chunk.GetValue(0, local).GetValue<uint64_t>();
	}

	//! The value of mapped column i (columns 1..) of the next unread row.
	Value CurrentValue(idx_t i) const {
		D_ASSERT(!done);
		return chunk.GetValue(1 + i, local);
	}

	void Advance(ClientContext &context) {
		if (done) {
			return;
		}
		local++;
		if (local >= chunk_rows) {
			chunk_start += chunk_rows;
			Fetch(context);
		}
	}
};

//! The merge driver: walks the old part's rows (position p = 0..old_count-1)
//! and the insert/update/delete cursors in lockstep, emitting output rows into
//! a scratch chunk (flushed to a buffer collection, then to Parquet).
struct MergeWriter {
	ClientContext &context;
	const PartMergeInput &input;
	idx_t out_cols = 0;
	// mapping column name -> output column position (INVALID when the part
	// lacks the column: inserts write NULL, updates are fail-fast)
	vector<idx_t> insert_col_pos;
	vector<idx_t> update_col_pos;
	// cursors
	RowCursor insert_cursor;
	RowCursor update_cursor;
	idx_t delete_next = 0;
	// old part reader
	unique_ptr<ParquetReader> old_reader;
	ParquetReaderScanState old_scan;
	DataChunk old_chunk;
	idx_t old_chunk_rows = 0;
	idx_t old_chunk_start = 0; // absolute row of the current old chunk
	idx_t old_count = 0;
	idx_t cur = 0; // next old row to process
	// output
	DataChunk scratch;
	idx_t scratch_rows = 0;
	ColumnDataCollection buffer;
	ColumnDataAppendState append_state;
	unique_ptr<ParquetWriter> writer;
	unique_ptr<ParquetWriteTransformData> transform;
	idx_t emitted = 0;

	MergeWriter(ClientContext &context_p, const PartMergeInput &input_p)
	    : context(context_p), input(input_p), buffer(context_p, input_p.col_types) {
		out_cols = input.col_types.size();
		insert_col_pos.resize(input.insert_cols.size(), DConstants::INVALID_INDEX);
		for (idx_t i = 0; i < input.insert_cols.size(); i++) {
			for (idx_t c = 0; c < input.col_names.size(); c++) {
				if (StringUtil::CIEquals(input.col_names[c], input.insert_cols[i])) {
					insert_col_pos[i] = c;
					break;
				}
			}
		}
		update_col_pos.resize(input.update_cols.size(), DConstants::INVALID_INDEX);
		for (idx_t i = 0; i < input.update_cols.size(); i++) {
			for (idx_t c = 0; c < input.col_names.size(); c++) {
				if (StringUtil::CIEquals(input.col_names[c], input.update_cols[i])) {
					update_col_pos[i] = c;
					break;
				}
			}
			if (update_col_pos[i] == DConstants::INVALID_INDEX) {
				throw IOException("Aligned table: cannot update column '%s' — it does not exist in part '%s' "
				                  "(schema evolution: the column was added later)",
				                  input.update_cols[i], input.part ? input.part->part_name : "<new>");
			}
		}
		scratch.Initialize(context, input.col_types);
		buffer.InitializeAppend(append_state);
	}

	//! Casts a source value to its output column type (defensive; bind already
	//! validates the types match).
	Value CastValue(const Value &v, idx_t out_pos) {
		if (v.IsNull() || out_pos == DConstants::INVALID_INDEX) {
			return v;
		}
		if (v.type().id() != input.col_types[out_pos].id()) {
			return v.CastAs(context, input.col_types[out_pos]);
		}
		return v;
	}

	void FlushScratch() {
		if (scratch_rows == 0) {
			return;
		}
		scratch.SetCardinality(scratch_rows);
		buffer.Append(append_state, scratch);
		scratch.Reset();
		scratch_rows = 0;
		if (buffer.Count() >= input.rgs) {
			writer->Flush(buffer, transform);
			buffer.Reset();
			buffer.InitializeAppend(append_state);
		}
	}

	void EmitRow() {
		scratch_rows++;
		emitted++;
		if (scratch_rows >= STANDARD_VECTOR_SIZE) {
			FlushScratch();
		}
	}

	//! Emits a mapped insert row (from the insert cursor): mapped values land
	//! at their output columns, everything else stays NULL.
	void EmitInsertRow() {
		idx_t pos = scratch_rows;
		for (idx_t c = 0; c < out_cols; c++) {
			scratch.SetValue(c, pos, Value(input.col_types[c]));
		}
		for (idx_t i = 0; i < input.insert_cols.size(); i++) {
			Value v = CastValue(insert_cursor.CurrentValue(i), insert_col_pos[i]);
			if (insert_col_pos[i] != DConstants::INVALID_INDEX) {
				scratch.SetValue(insert_col_pos[i], pos, v);
			}
		}
		EmitRow();
		insert_cursor.Advance(context);
	}

	//! Emits the old row at position `cur` (already known not deleted),
	//! overwriting mapped columns when an update targets this row.
	void EmitOldRow() {
		idx_t local = cur - old_chunk_start;
		idx_t pos = scratch_rows;
		for (idx_t c = 0; c < out_cols; c++) {
			VectorOperations::Copy(old_chunk.data[c], scratch.data[c], local + 1, local, pos);
		}
		if (input.updates && !update_cursor.done && update_cursor.CurrentPos() == cur) {
			for (idx_t i = 0; i < input.update_cols.size(); i++) {
				Value v = CastValue(update_cursor.CurrentValue(i), update_col_pos[i]);
				scratch.SetValue(update_col_pos[i], pos, v);
			}
			update_cursor.Advance(context);
		}
		EmitRow();
		cur++;
	}

	//! Bulk-copies old rows [a, b) (within the current old chunk) to the
	//! scratch chunk, splitting by scratch capacity (the scratch holds at most
	//! STANDARD_VECTOR_SIZE rows; a bulk run between two events can be longer).
	void BulkCopyOld(idx_t a, idx_t b) {
		while (a < b) {
			const idx_t avail = STANDARD_VECTOR_SIZE - scratch_rows;
			const idx_t n = MinValue(b - a, avail);
			const idx_t local_a = a - old_chunk_start;
			const idx_t pos = scratch_rows;
			for (idx_t c = 0; c < out_cols; c++) {
				VectorOperations::Copy(old_chunk.data[c], scratch.data[c], local_a + n, local_a, pos);
			}
			a += n;
			scratch_rows += n;
			emitted += n;
			cur = a;
			if (scratch_rows >= STANDARD_VECTOR_SIZE) {
				FlushScratch();
			}
		}
	}

	//! Fetches the next old chunk (skipping the empty setup chunks the parquet
	//! reader returns on row-group switches). Returns false when the stream is
	//! exhausted.
	bool FetchOldChunk() {
		old_chunk_start = cur;
		while (true) {
			auto res = old_reader->Scan(context, old_scan, old_chunk);
			auto async_type = res.GetResultType();
			if (async_type == AsyncResultType::FINISHED || async_type == AsyncResultType::BLOCKED) {
				old_chunk_rows = 0;
				return false;
			}
			if (old_chunk.size() == 0) {
				continue;
			}
			old_chunk_rows = old_chunk.size();
			return true;
		}
	}
};

} // namespace

idx_t RewritePart(ClientContext &context, const PartMergeInput &input) {
	// Validate the buffer shapes ([pos, ...mapped cols]).
	if (input.inserts && input.insert_cols.size() != input.inserts->Types().size() - 1) {
		throw IOException("Aligned table: internal error — insert buffer has %llu value columns, mapping declares "
		                  "%llu",
		                  input.inserts->Types().size() - 1, input.insert_cols.size());
	}
	if (input.updates && input.update_cols.size() != input.updates->Types().size() - 1) {
		throw IOException("Aligned table: internal error — update buffer has %llu value columns, mapping declares "
		                  "%llu",
		                  input.updates->Types().size() - 1, input.update_cols.size());
	}

	MergeWriter mw(context, input);

	// Open the old part (fresh part: no old rows).
	if (input.part) {
		mw.old_reader = make_uniq<ParquetReader>(context, OpenFileInfo(input.part->path), ParquetOptions(context));
		for (idx_t i = 0; i < mw.old_reader->columns.size(); i++) {
			mw.old_reader->column_ids.push_back(MultiFileLocalColumnId(i));
		}
		vector<PartitionStatistics> rg_stats;
		mw.old_reader->GetPartitionStats(rg_stats);
		vector<idx_t> all_rgs;
		for (idx_t i = 0; i < rg_stats.size(); i++) {
			all_rgs.push_back(i);
		}
		mw.old_reader->InitializeScan(context, mw.old_scan, all_rgs);
		mw.old_chunk.Initialize(context, input.col_types);
		mw.old_count = mw.old_reader->NumRows();
		// v6 defensive check: the footer row count must match the
		// self-describing file name (same as the reader side).
		if (mw.old_count != input.part->row_count) {
			throw IOException("Aligned table: part '%s' declares %llu rows in its file name but the footer holds "
			                  "%llu rows",
			                  input.part->part_name, input.part->row_count, mw.old_count);
		}
	}

	// Writer (same parameters as aligned_write / aligned_compact).
	mw.writer = make_uniq<ParquetWriter>(
	    context, FileSystem::GetFileSystem(context), input.staged_path, input.col_types, input.col_names,
	    duckdb_parquet::CompressionCodec::ZSTD, ChildFieldIDs(), ShreddingType(), vector<pair<string, string>>(),
	    nullptr, optional_idx(), 1073741824ULL /* PrimitiveColumnWriter::MAX_UNCOMPRESSED_DICT_PAGE_SIZE */, 1, 0.01,
	    ZStdFileSystem::DefaultCompressionLevel(), ParquetVersion::V1, GeoParquetVersion::V1);

	if (input.inserts) {
		mw.insert_cursor.Init(context, *input.inserts);
	}
	if (input.updates) {
		mw.update_cursor.Init(context, *input.updates);
	}

	// The merge loop: process old rows in order; at each position emit pending
	// inserts (same position) first, then the old row unless deleted/updated.
	while (mw.cur < mw.old_count || (input.inserts && !mw.insert_cursor.done)) {
		if (mw.cur >= mw.old_count) {
			// trailing inserts (appended at the part end)
			while (input.inserts && !mw.insert_cursor.done) {
				mw.EmitInsertRow();
			}
			break;
		}
		// Ensure the old chunk covers `cur`.
		if (mw.cur >= mw.old_chunk_start + mw.old_chunk_rows) {
			if (!mw.FetchOldChunk()) {
				throw IOException("Aligned table: part scan ended early (expected %llu rows)", mw.old_count);
			}
		}
		// Next event position (all cursors >= cur).
		idx_t event = INF_POS;
		if (input.inserts && !mw.insert_cursor.done) {
			event = MinValue<idx_t>(event, mw.insert_cursor.CurrentPos());
		}
		if (input.deletes && mw.delete_next < input.deletes->size()) {
			event = MinValue<idx_t>(event, (*input.deletes)[mw.delete_next]);
		}
		if (input.updates && !mw.update_cursor.done) {
			event = MinValue<idx_t>(event, mw.update_cursor.CurrentPos());
		}
		// Bulk-copy untouched old rows up to the event (within this chunk).
		idx_t chunk_end = MinValue<idx_t>(mw.old_chunk_start + mw.old_chunk_rows, mw.old_count);
		idx_t bulk_end = MinValue<idx_t>(event, chunk_end);
		if (bulk_end > mw.cur) {
			mw.BulkCopyOld(mw.cur, bulk_end);
			continue;
		}
		// Event at `cur`.
		if (input.inserts && !mw.insert_cursor.done && mw.insert_cursor.CurrentPos() == mw.cur) {
			mw.EmitInsertRow();
			continue;
		}
		if (input.deletes && mw.delete_next < input.deletes->size() && (*input.deletes)[mw.delete_next] == mw.cur) {
			mw.delete_next++;
			mw.cur++;
			continue;
		}
		// Plain old row (possibly with an update overwrite).
		mw.EmitOldRow();
	}

	mw.FlushScratch();
	mw.writer->Flush(mw.buffer, mw.transform);
	mw.writer->Finalize();
	return mw.emitted;
}

} // namespace duckdb