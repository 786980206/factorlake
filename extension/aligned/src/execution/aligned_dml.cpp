//! Standard DML physical operators for aligned attached tables (Phase 8).
//!
//! INSERT: buffer rows -> temp parquet under <table>/_tmp/ -> aligned_upsert
//! mutator places them into the column groups atomically. The upsert runs on
//! a dedicated thread with its own Connection (nested queries on the caller's
//! context deadlock 鈥?see aligned_attach.cpp).

#include "execution/aligned_dml.hpp"

#include "catalog/aligned_catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "mutator/aligned_mutator.hpp"
#include "parquet_writer.hpp"
#include "zstd_file_system.hpp"

#include <thread>

namespace duckdb {

//===----------------------------------------------------------------------===//
// States
//===----------------------------------------------------------------------===//
struct AlignedInsertGlobalState : public GlobalSinkState {
	AlignedInsertGlobalState(ClientContext &context, const vector<LogicalType> &types)
	    : collection(context, types) {
	}
	ColumnDataCollection collection;
	idx_t row_count = 0;
	idx_t rows_inserted = 0;
	idx_t rows_updated = 0;
	string error;
};

struct AlignedInsertLocalState : public LocalSinkState {
	explicit AlignedInsertLocalState(ClientContext &context, const vector<LogicalType> &types)
	    : collection(context, types) {
	}
	ColumnDataCollection collection;
	idx_t row_count = 0;
};

struct AlignedInsertSourceState : public GlobalSourceState {
};

//===----------------------------------------------------------------------===//
// PhysicalAlignedInsert
//===----------------------------------------------------------------------===//
PhysicalAlignedInsert::PhysicalAlignedInsert(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                                             vector<LogicalType> row_types_p, vector<string> row_names_p,
                                             const string &table_p, const string &root_p, idx_t est_card)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::INSERT, std::move(types_p), est_card),
      table(table_p), root(root_p), row_types(std::move(row_types_p)), row_names(std::move(row_names_p)) {
}

unique_ptr<GlobalSinkState> PhysicalAlignedInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<AlignedInsertGlobalState>(context, row_types);
}

unique_ptr<LocalSinkState> PhysicalAlignedInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<AlignedInsertLocalState>(context.client, row_types);
}

SinkResultType PhysicalAlignedInsert::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &input) const {
	auto &g = input.global_state.Cast<AlignedInsertGlobalState>();
	auto &l = input.local_state.Cast<AlignedInsertLocalState>();
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	l.collection.Append(chunk);
	l.row_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalAlignedInsert::Combine(ExecutionContext &context,
                                                     OperatorSinkCombineInput &input) const {
	auto &g = input.global_state.Cast<AlignedInsertGlobalState>();
	auto &l = input.local_state.Cast<AlignedInsertLocalState>();
	if (l.row_count > 0) {
		g.collection.Combine(l.collection);
		g.row_count += l.row_count;
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalAlignedInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	auto &g = input.global_state.Cast<AlignedInsertGlobalState>();
	if (g.row_count == 0) {
		return SinkFinalizeType::READY;
	}

	// Stage the buffered rows as a temp parquet inside the table's _tmp dir
	// (invisible to readers and to group discovery).
	auto fs = FileSystem::CreateLocal();
	string tmp_dir = root + "/" + table + "/_tmp";
	fs->CreateDirectoriesRecursive(tmp_dir);
	string staged = tmp_dir + "/dml-insert-" + StringUtil::Format("%lld", (long long)Timestamp::GetEpochMs(
	                                                                    Timestamp::GetCurrentTimestamp())) +
	                "-" + StringUtil::Format("%d", (int)(idx_t)this % 100000) + ".parquet";

	auto &staged_names = row_names;
	{
		ParquetWriter writer(context, FileSystem::GetFileSystem(context), staged, row_types, staged_names,
		                     duckdb_parquet::CompressionCodec::ZSTD, ChildFieldIDs(), ShreddingType(),
		                     vector<pair<string, string>>(), nullptr, optional_idx(), 1073741824ULL, 1, 0.01,
		                     ZStdFileSystem::DefaultCompressionLevel(), ParquetVersion::V1, GeoParquetVersion::V1);
		unique_ptr<ParquetWriteTransformData> transform;
		ColumnDataCollection buffer(context, row_types);
		ColumnDataAppendState append;
		buffer.InitializeAppend(append);
		ColumnDataScanState scan_state;
		g.collection.InitializeScan(scan_state);
		DataChunk scan_chunk;
		g.collection.InitializeScanChunk(scan_chunk);
		while (g.collection.Scan(scan_state, scan_chunk)) {
			buffer.Append(scan_chunk);
			if (buffer.Count() >= 122880) {
				writer.Flush(buffer, transform);
				buffer.Reset();
				buffer.InitializeAppend(append);
			}
			scan_chunk.Reset();
		}
		writer.Flush(buffer, transform);
		writer.Finalize();
	}

	// Run the mutator on a worker thread (nested-query deadlock avoidance).
	auto &db = DatabaseInstance::GetDatabase(context);
	const string &tbl = table;
	const string &rt = root;
	const string staged_path = staged;
	std::thread worker([&db, &g, tbl, rt, staged_path]() {
		try {
			Connection con(db);
			auto result = con.Query("SELECT rows_inserted, rows_updated FROM aligned_upsert('" +
			                        StringUtil::Replace(tbl, "'", "''") + "', '" +
			                        StringUtil::Replace(staged_path, "'", "''") + "', root='" +
			                        StringUtil::Replace(rt, "'", "''") + "')");
			if (result->HasError()) {
				g.error = result->GetError();
				return;
			}
			auto &mat = result->Cast<MaterializedQueryResult>();
			if (mat.RowCount() > 0) {
				g.rows_inserted = mat.GetValue<int64_t>(0, 0);
				g.rows_updated = mat.GetValue<int64_t>(1, 0);
			}
		} catch (std::exception &ex) {
			g.error = ex.what();
		}
	});
	worker.join();

	// Best-effort cleanup of the staging file.
	try {
		if (g.error.empty()) {
			fs->RemoveFile(staged);
		}
	} catch (...) {
	}
	if (!g.error.empty()) {
		throw IOException("aligned INSERT failed: %s", g.error);
	}
	return SinkFinalizeType::READY;
}

SourceResultType PhysicalAlignedInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<AlignedInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.row_count)));
	return SourceResultType::FINISHED;
}

unique_ptr<GlobalSourceState> PhysicalAlignedInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<AlignedInsertSourceState>();
}

} // namespace duckdb







