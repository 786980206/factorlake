//! Standard DML physical operators for aligned attached tables (Phase 8).
//!
//! INSERT: buffer rows -> temp parquet under <table>/_tmp/ -> aligned_upsert
//! mutator places them into the column groups atomically. The upsert runs on
//! a dedicated thread with its own Connection (nested queries on the caller's
//! context deadlock 鈥?see aligned_catalog.cpp).

#include "execution/aligned_dml.hpp"

#include "catalog/aligned_catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "mutator/aligned_mutator.hpp"
#include "scan/aligned_scan.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "parquet_writer.hpp"
#include "zstd_file_system.hpp"

#include <thread>


#ifdef DELETE
#undef DELETE
#endif

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

	// Direct upsert from the in-memory ColumnDataCollection — no temp parquet
	// file, no intermediate serialization. The mutator reads the collection
	// directly via the source_collection field in MutateBindData.
	// Run on a worker thread (nested-query deadlock avoidance: the caller's
	// context may be holding pipeline locks).
	auto &db = DatabaseInstance::GetDatabase(context);
	const string &tbl = table;
	const string &rt = root;
	// Build the mapping string from the table's column groups + the insert's
	// column names. Auto-derive mapping (no mapping string) lets the mutator
	// assign each source column to the group that owns it.
	std::thread worker([&db, &g, tbl, rt, this]() {
		try {
			Connection con(db);
			auto result = AlignedUpsertFromCollection(*con.context, tbl, rt, "", g.collection, row_names);
			g.rows_inserted = result.rows_inserted;
			g.rows_updated = result.rows_updated;
		} catch (std::exception &ex) {
			g.error = ex.what();
		} catch (...) {
			g.error = "unknown error during aligned upsert";
		}
	});
	worker.join();

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










//===----------------------------------------------------------------------===//
// PhysicalAlignedDelete
//===----------------------------------------------------------------------===//
struct AlignedDeleteGlobalState : public GlobalSinkState {
	mutex lock;
	vector<int64_t> rowids; // selected logical row numbers
	idx_t rows_deleted = 0;
	string error;
	string staged_path; // keys parquet staged by the worker (cleanup after join)
};

PhysicalAlignedDelete::PhysicalAlignedDelete(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                                             const string &table_p, const string &root_p, idx_t est_card)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::DELETE_OPERATOR, std::move(types_p), est_card), table(table_p),
      root(root_p) {
}

unique_ptr<GlobalSinkState> PhysicalAlignedDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<AlignedDeleteGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalAlignedDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<LocalSinkState>();
}

SinkResultType PhysicalAlignedDelete::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &input) const {
	auto &g = input.global_state.Cast<AlignedDeleteGlobalState>();
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	// The child plan projects the table's rowid column (last column).
	auto &row_ids = chunk.data[chunk.ColumnCount() - 1];
	UnifiedVectorFormat vdata;
	row_ids.ToUnifiedFormat(chunk.size(), vdata);
	auto ids = UnifiedVectorFormat::GetData<int64_t>(vdata);
	lock_guard<mutex> l(g.lock);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto ridx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(ridx)) {
			g.rowids.push_back(ids[ridx]);
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalAlignedDelete::Combine(ExecutionContext &context,
                                                     OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

//! Scans the index group's (date, symbol) columns and writes the keys of the
//! given rowids to a staging parquet for aligned_delete.
static string StageKeysForRowids(ClientContext &context, const string &root, const string &table_name,
                                 const vector<int64_t> &rowids, const string &tmp_dir) {
	auto sorted = rowids;
	sort(sorted.begin(), sorted.end());
	sorted.erase(unique(sorted.begin(), sorted.end()), sorted.end());

	vector<LogicalType> scan_types;
	vector<string> scan_names;
	auto bind_data = AlignedBindForCatalog(context, root, table_name, scan_types, scan_names);
	// Locate the key columns in the schema by name.
	idx_t date_pos = DConstants::INVALID_INDEX;
	idx_t symbol_pos = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < scan_names.size(); i++) {
		if (StringUtil::CIEquals(scan_names[i], "date")) {
			date_pos = i;
		} else if (StringUtil::CIEquals(scan_names[i], "symbol")) {
			symbol_pos = i;
		}
	}
	if (date_pos == DConstants::INVALID_INDEX || symbol_pos == DConstants::INVALID_INDEX) {
		throw IOException("aligned DELETE: primary key columns (symbol, date) not found in '%s'", table_name);
	}

	// Scan symbol/date only (v8: symbol first, then date); logical row numbers
	// come from a sequential counter (a single local state consumes the shared
	// cursor in order).
	vector<column_t> col_ids = {symbol_pos, date_pos};
	TableFunctionInitInput init_input(bind_data.get(), col_ids, {}, nullptr);
	{
		string nm;
		for (auto &n : scan_names) {
			nm += n + ",";
		}
	}
	auto gstate = AlignedInitGlobal(context, init_input);
	ThreadContext thread_context(context);
	ExecutionContext exec(context, thread_context, nullptr);
	auto lstate = AlignedInitLocal(exec, init_input, gstate.get());

	DataChunk scan_chunk;
	scan_chunk.Initialize(context, scan_types);

	// v8: keys are (symbol, date) — symbol first to match the index schema.
	auto keys_types = vector<LogicalType> {scan_types[symbol_pos], scan_types[date_pos]};
	ColumnDataCollection keys(context, keys_types);
	ColumnDataAppendState append;
	keys.InitializeAppend(append);
	DataChunk out_chunk;
	out_chunk.Initialize(context, keys_types);

	idx_t next = 0;      // cursor into sorted rowids
	int64_t abs_row = 0; // logical row number of scan_chunk row 0
	while (next < sorted.size()) {
		AlignedScanFunction(context, TableFunctionInput(bind_data.get(), lstate.get(), gstate.get()), scan_chunk);
		if (scan_chunk.size() == 0) {
			break;
		}
		UnifiedVectorFormat sv, dv;
		scan_chunk.data[0].ToUnifiedFormat(scan_chunk.size(), sv); // symbol
		scan_chunk.data[1].ToUnifiedFormat(scan_chunk.size(), dv); // date
		auto sptr = UnifiedVectorFormat::GetData<string_t>(sv);
		auto dptr = UnifiedVectorFormat::GetData<int32_t>(dv);     // DATE = int32 days since epoch
		idx_t out_n = 0;
		for (idx_t i = 0; i < scan_chunk.size(); i++) {
			int64_t rid = abs_row + static_cast<int64_t>(i);
			bool want = false;
			while (next < sorted.size() && sorted[next] < rid) {
				next++;
			}
			if (next < sorted.size() && sorted[next] == rid) {
				want = true;
				next++;
			}
			if (want) {
				auto si = sv.sel->get_index(i);
				auto di = dv.sel->get_index(i);
				out_chunk.SetValue(0, out_n, Value(sptr[si].GetString())); // symbol
				out_chunk.SetValue(1, out_n, Value::DATE(date_t(dptr[di]))); // date
				out_n++;
			}
		}
		abs_row += static_cast<int64_t>(scan_chunk.size());
		if (out_n > 0) {
			out_chunk.SetCardinality(out_n);
			keys.Append(out_chunk);
			out_chunk.Reset();
		}
		scan_chunk.Reset();
	}

	// Stage to parquet (v8: column order symbol, date).
	auto fs = FileSystem::CreateLocal();
	fs->CreateDirectoriesRecursive(tmp_dir);
	string staged = tmp_dir + "/dml-delete-keys-" +
	                StringUtil::Format("%lld", (long long)Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp())) +
	                "-" + StringUtil::Format("%d", (int)(idx_t)&keys % 100000) + ".parquet";
	ParquetWriter writer(context, FileSystem::GetFileSystem(context), staged, keys_types,
	                     vector<string> {"symbol", "date"}, duckdb_parquet::CompressionCodec::ZSTD, ChildFieldIDs(),
	                     ShreddingType(), vector<pair<string, string>>(), nullptr, optional_idx(), 1073741824ULL, 1,
	                     0.01, ZStdFileSystem::DefaultCompressionLevel(), ParquetVersion::V1, GeoParquetVersion::V1);
	unique_ptr<ParquetWriteTransformData> transform;
	writer.Flush(keys, transform);
	writer.Finalize();
	return staged;
}

SinkFinalizeType PhysicalAlignedDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	auto &g = input.global_state.Cast<AlignedDeleteGlobalState>();
	if (g.rowids.empty()) {
		return SinkFinalizeType::READY;
	}

	string tmp_dir = root + "/" + table + "/_tmp";
	auto &db = DatabaseInstance::GetDatabase(context);
	std::thread worker([&db, &g, tbl = table, rt = root, rowids = g.rowids, tmp_dir]() {
		string staged;
		try {
			Connection key_con(db);
			// Resolve rowids -> keys on the worker's own context (reentrant
			// scanning on the caller's pipeline context is not safe).
			staged = StageKeysForRowids(*key_con.context, rt, tbl, rowids, tmp_dir);
			g.staged_path = staged;
		} catch (std::exception &ex) {
			g.error = string("key resolution failed: ") + ex.what();
			return;
		}
		try {
			Connection con(db);
			auto result = con.Query("SELECT rows_deleted FROM aligned_delete('" +
			                        StringUtil::Replace(tbl, "'", "''") + "', '" +
			                        StringUtil::Replace(staged, "'", "''") + "', root='" +
			                        StringUtil::Replace(rt, "'", "''") + "')");
			if (result->HasError()) {
				g.error = result->GetError();
				return;
			}
			auto &mat = result->Cast<MaterializedQueryResult>();
			if (mat.RowCount() > 0) {
				g.rows_deleted = mat.GetValue<int64_t>(0, 0);
			}
		} catch (std::exception &ex) {
			g.error = ex.what();
		}
	});
	worker.join();

	if (!g.staged_path.empty()) {
		try {
			FileSystem::CreateLocal()->RemoveFile(g.staged_path);
		} catch (...) {
		}
	}
	if (!g.error.empty()) {
		throw IOException("aligned DELETE failed: %s", g.error);
	}
	return SinkFinalizeType::READY;
}

SourceResultType PhysicalAlignedDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<AlignedDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.rows_deleted)));
	return SourceResultType::FINISHED;
}

unique_ptr<GlobalSourceState> PhysicalAlignedDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<AlignedInsertSourceState>();
}


//===----------------------------------------------------------------------===//
// PhysicalAlignedUpdate
//
// Standard UPDATE on an attached aligned table. Only the SET columns are
// fetched (base BindUpdateConstraints); the child chunk carries their new
// values plus a trailing rowid. The sink collects (rowid, set values); the
// finalizer resolves each rowid to its (date, symbol) key by scanning the
// index group, then hands [date, symbol, set...] rows to aligned_upsert with
// a mapping limited to the touched columns (columns absent from old parts due
// to schema evolution must NOT be force-updated).
//===----------------------------------------------------------------------===//
struct AlignedUpdateGlobalState : public GlobalSinkState {
	// (rowid, set values...) per row, accumulated across sink calls.
	ColumnDataCollection collection;
	idx_t row_count = 0;
	string error;
	explicit AlignedUpdateGlobalState(ClientContext &context, const vector<LogicalType> &types)
	    : collection(context, types) {
	}
};

struct AlignedUpdateLocalState : public LocalSinkState {
	DataChunk row_chunk;
};

PhysicalAlignedUpdate::PhysicalAlignedUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                                             vector<string> set_names_p, vector<string> set_groups_p,
                                             const string &table_p, const string &root_p, idx_t est_card)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::UPDATE, std::move(types_p), est_card),
      table(table_p), root(root_p), set_names(std::move(set_names_p)), set_groups(std::move(set_groups_p)) {
}

unique_ptr<GlobalSinkState> PhysicalAlignedUpdate::GetGlobalSinkState(ClientContext &context) const {
	vector<LogicalType> types;
	types.push_back(LogicalType::BIGINT); // rowid
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
	return make_uniq<AlignedUpdateGlobalState>(context, types);
}

unique_ptr<LocalSinkState> PhysicalAlignedUpdate::GetLocalSinkState(ExecutionContext &context) const {
	auto gstate_ptr = GetGlobalSinkState(context.client);
	auto &gstate = gstate_ptr->Cast<AlignedUpdateGlobalState>();
	auto state = make_uniq<AlignedUpdateLocalState>();
	state->row_chunk.Initialize(context.client, gstate.collection.Types());
	return std::move(state);
}

SinkResultType PhysicalAlignedUpdate::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &input) const {
	auto &g = input.global_state.Cast<AlignedUpdateGlobalState>();
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	auto &out = input.local_state.Cast<AlignedUpdateLocalState>().row_chunk;
	out.Reset();
	// Trailing column = rowid; preceding columns = new values of the SET cols.
	auto &row_ids = chunk.data[chunk.ColumnCount() - 1];
	out.data[0].Reference(row_ids);
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			out.data[i + 1].Reference(Value(expressions[i]->return_type));
			continue;
		}
		auto &binding = expressions[i]->Cast<BoundReferenceExpression>();
		out.data[i + 1].Reference(chunk.data[binding.index]);
	}
	out.SetCardinality(chunk.size());
	g.collection.Append(out);
	g.row_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalAlignedUpdate::Combine(ExecutionContext &context,
                                                     OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

//! Scans the index group's (date, symbol) for the given rowids and appends the
//! matched rows as (rowid, date, symbol) in ascending rowid order.
static void ResolveKeysForRowids(ClientContext &context, const string &root, const string &table_name,
                                 const vector<int64_t> &rowids, ColumnDataCollection &out,
                                 ColumnDataAppendState &append) {
	auto sorted = rowids;
	sort(sorted.begin(), sorted.end());
	sorted.erase(unique(sorted.begin(), sorted.end()), sorted.end());

	vector<LogicalType> scan_types;
	vector<string> scan_names;
	auto bind_data = AlignedBindForCatalog(context, root, table_name, scan_types, scan_names);
	idx_t date_pos = DConstants::INVALID_INDEX;
	idx_t symbol_pos = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < scan_names.size(); i++) {
		if (StringUtil::CIEquals(scan_names[i], "date")) {
			date_pos = i;
		} else if (StringUtil::CIEquals(scan_names[i], "symbol")) {
			symbol_pos = i;
		}
	}
	if (date_pos == DConstants::INVALID_INDEX || symbol_pos == DConstants::INVALID_INDEX) {
		throw IOException("aligned UPDATE: primary key columns (symbol, date) not found in '%s'", table_name);
	}
	// v8: scan symbol first, then date.
	vector<column_t> col_ids = {symbol_pos, date_pos};
	TableFunctionInitInput init_input(bind_data.get(), col_ids, {}, nullptr);
	auto gstate = AlignedInitGlobal(context, init_input);
	ThreadContext thread_context(context);
	ExecutionContext exec(context, thread_context, nullptr);
	auto lstate = AlignedInitLocal(exec, init_input, gstate.get());

	DataChunk scan_chunk;
	scan_chunk.Initialize(context, scan_types);
	DataChunk row_chunk;
	row_chunk.Initialize(context, out.Types());

	idx_t next = 0;
	int64_t abs_row = 0;
	while (next < sorted.size()) {
		AlignedScanFunction(context, TableFunctionInput(bind_data.get(), lstate.get(), gstate.get()), scan_chunk);
		if (scan_chunk.size() == 0) {
			break;
		}
		UnifiedVectorFormat sv, dv;
		scan_chunk.data[0].ToUnifiedFormat(scan_chunk.size(), sv); // symbol
		scan_chunk.data[1].ToUnifiedFormat(scan_chunk.size(), dv); // date
		auto sptr = UnifiedVectorFormat::GetData<string_t>(sv);
		auto dptr = UnifiedVectorFormat::GetData<int32_t>(dv);
		idx_t out_n = 0;
		for (idx_t i = 0; i < scan_chunk.size(); i++) {
			int64_t rid = abs_row + static_cast<int64_t>(i);
			bool want = false;
			while (next < sorted.size() && sorted[next] < rid) {
				next++;
			}
			if (next < sorted.size() && sorted[next] == rid) {
				want = true;
				next++;
			}
			if (want) {
				auto si = sv.sel->get_index(i);
				auto di = dv.sel->get_index(i);
				row_chunk.SetValue(0, out_n, Value::BIGINT(rid));
				// v8: row_chunk layout = (rowid, symbol, date)
				row_chunk.SetValue(1, out_n, Value(sptr[si].GetString()));
				row_chunk.SetValue(2, out_n, Value::DATE(date_t(dptr[di])));
				out_n++;
			}
		}
		abs_row += static_cast<int64_t>(scan_chunk.size());
		if (out_n > 0) {
			row_chunk.SetCardinality(out_n);
			out.Append(row_chunk);
			row_chunk.Reset();
		}
		scan_chunk.Reset();
	}
}

SinkFinalizeType PhysicalAlignedUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	auto &g = input.global_state.Cast<AlignedUpdateGlobalState>();
	if (g.row_count == 0) {
		return SinkFinalizeType::READY;
	}

	auto &db = DatabaseInstance::GetDatabase(context);
	string staged;
	string err;
	std::thread worker([&db, &g, &staged, &err, this, tbl = table, rt = root]() {
		try {
			Connection con(db);
			ClientContext &wctx = *con.context;

			// Collect all rowids + set values from the sink collection.
			vector<int64_t> rowids;
			std::map<int64_t, std::vector<Value>> set_by_row;
			{
				ColumnDataScanState ss;
				g.collection.InitializeScan(ss);
				DataChunk c;
				g.collection.InitializeScanChunk(c);
				while (g.collection.Scan(ss, c)) {
					for (idx_t r = 0; r < c.size(); r++) {
						int64_t rid = c.GetValue(0, r).GetValue<int64_t>();
						rowids.push_back(rid);
						std::vector<Value> vals;
						for (idx_t k = 1; k < c.ColumnCount(); k++) {
							vals.push_back(c.GetValue(k, r));
						}
						set_by_row[rid] = vals;
					}
					c.Reset();
				}
			}

			// Resolve keys: (rowid, symbol, date) — v8: symbol before date.
			auto keys_types = vector<LogicalType> {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::DATE};
			ColumnDataCollection keys(wctx, keys_types);
			ColumnDataAppendState append;
			keys.InitializeAppend(append);
			ResolveKeysForRowids(wctx, rt, tbl, rowids, keys, append);

			// staged schema: symbol, date, then set columns in set_names order (v8)
			vector<LogicalType> staged_types;
			vector<string> staged_names;
			staged_types.push_back(LogicalType::VARCHAR);
			staged_names.push_back("symbol");
			staged_types.push_back(LogicalType::DATE);
			staged_names.push_back("date");
			for (auto &expr : this->expressions) {
				staged_types.push_back(expr->return_type);
			}
			for (auto &nm : this->set_names) {
				staged_names.push_back(nm);
			}

			ColumnDataCollection staged_coll(wctx, staged_types);
			ColumnDataAppendState staged_append;
			staged_coll.InitializeAppend(staged_append);
			DataChunk out_chunk;
			out_chunk.Initialize(wctx, staged_types);
			idx_t out_n = 0;
			ColumnDataScanState ss;
			keys.InitializeScan(ss);
			DataChunk kc;
			keys.InitializeScanChunk(kc);
			while (keys.Scan(ss, kc)) {
				for (idx_t r = 0; r < kc.size(); r++) {
					int64_t rid = kc.GetValue(0, r).GetValue<int64_t>();
					auto it = set_by_row.find(rid);
					if (it == set_by_row.end()) {
						continue;
					}
					// kc layout: (rowid, symbol, date) — v8
					out_chunk.SetValue(0, out_n, kc.GetValue(1, r)); // symbol
					out_chunk.SetValue(1, out_n, kc.GetValue(2, r)); // date
					for (idx_t k = 0; k < it->second.size(); k++) {
						out_chunk.SetValue(2 + k, out_n, it->second[k]);
					}
					out_n++;
					if (out_n >= STANDARD_VECTOR_SIZE) {
						out_chunk.SetCardinality(out_n);
						staged_coll.Append(out_chunk);
						out_chunk.Reset();
						out_n = 0;
					}
				}
				kc.Reset();
			}
			if (out_n > 0) {
				out_chunk.SetCardinality(out_n);
				staged_coll.Append(out_chunk);
			}

			auto fs_local = FileSystem::CreateLocal();
			auto &fs = *fs_local;
			string tmp_dir = rt + "/" + tbl + "/_tmp";
			fs.CreateDirectoriesRecursive(tmp_dir);
			staged = tmp_dir + "/dml-update-" +
			         StringUtil::Format("%lld", (long long)Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp())) +
			         "-" + StringUtil::Format("%d", (int)(idx_t)&rowids % 100000) + ".parquet";

			ParquetWriter writer(wctx, fs, staged, staged_types, staged_names,
			                     duckdb_parquet::CompressionCodec::ZSTD, ChildFieldIDs(), ShreddingType(),
			                     vector<pair<string, string>>(), nullptr, optional_idx(), 1073741824ULL, 1, 0.01,
			                     ZStdFileSystem::DefaultCompressionLevel(), ParquetVersion::V1, GeoParquetVersion::V1);
			unique_ptr<ParquetWriteTransformData> transform;
			writer.Flush(staged_coll, transform);
			writer.Finalize();

			// Upsert mapping (v8): index:symbol,date(+ any index-group set
			// columns); each non-index set group gets its own mapping segment.
			string index_cols = "symbol,date";
			string mapping = "index:symbol,date";
			for (idx_t i = 0; i < this->set_names.size(); i++) {
				if (this->set_groups[i].empty()) {
					continue;
				}
				if (this->set_groups[i] == "index") {
					index_cols += "," + this->set_names[i];
				} else {
					mapping += ";" + this->set_groups[i] + ":" + this->set_names[i];
				}
			}
			if (index_cols != "symbol,date") {
				mapping = "index:" + index_cols + mapping.substr(strlen("index:symbol,date"));
			}

			Connection con2(db);
			auto result = con2.Query("SELECT rows_inserted, rows_updated FROM aligned_upsert('" +
			                         StringUtil::Replace(tbl, "'", "''") + "', '" +
			                         StringUtil::Replace(staged, "'", "''") + "', '" +
			                         StringUtil::Replace(mapping, "'", "''") + "', root='" +
			                         StringUtil::Replace(rt, "'", "''") + "')");
			if (result->HasError()) {
				err = result->GetError();
				return;
			}
			auto &mat = result->Cast<MaterializedQueryResult>();
			if (mat.RowCount() > 0) {
				g.row_count = mat.GetValue<int64_t>(1, 0); // rows_updated
			}
		} catch (std::exception &ex) {
			err = ex.what();
		}
	});
	worker.join();

	if (!staged.empty()) {
		try {
			FileSystem::CreateLocal()->RemoveFile(staged);
		} catch (...) {
		}
	}
	if (!err.empty()) {
		throw IOException("aligned UPDATE failed: %s", err);
	}
	return SinkFinalizeType::READY;
}

SourceResultType PhysicalAlignedUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<AlignedUpdateGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.row_count)));
	return SourceResultType::FINISHED;
}

unique_ptr<GlobalSourceState> PhysicalAlignedUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<AlignedInsertSourceState>();
}

} // namespace duckdb
