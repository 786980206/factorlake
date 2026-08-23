//! Standard DML physical operators for aligned attached tables (Phase 8).
//!
//! INSERT/UPDATE/DELETE: buffer rows in-memory -> direct C++ mutator call
//! (AlignedUpsertFromCollection / AlignedDeleteFromCollection). No temp parquet
//! file, no worker thread, no nested SQL query. The mutator does only
//! filesystem + parquet I/O (no catalog/executor access), so it is safe to
//! call directly on the pipeline thread.

#include "execution/aligned_dml.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "mutator/aligned_mutator.hpp"
#include "scan/aligned_scan.hpp"

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
                                             const string &table_p, const string &root_p, idx_t est_card,
                                             string explicit_mapping_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::INSERT, std::move(types_p), est_card),
      table(table_p), root(root_p), row_types(std::move(row_types_p)), row_names(std::move(row_names_p)),
      explicit_mapping(std::move(explicit_mapping_p)) {
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

	// For large INSERTs, batch the upsert to avoid materializing all rows
	// in memory at once. Each batch is a separate mutator transaction
	// (re-acquires write lock, re-reads table plan). The batch size is
	// chosen to balance memory usage vs per-batch metadata overhead.
	static constexpr idx_t INSERT_BATCH_SIZE = 1048576; // 1M rows per batch

	try {
		if (g.collection.Count() <= INSERT_BATCH_SIZE) {
			// Small enough — single call (the common case).
			AlignedUpsertFromCollection(context, table, root, explicit_mapping, g.collection, row_names);
		} else {
			// Large INSERT — scan the collection in batches, calling the
			// mutator per batch. Each batch is an independent transaction.
			// The write lock ensures batches are serialized (no concurrent
			// writes from other processes), and the mutator re-reads the
			// table plan each time to pick up parts written by prior batches.
			ColumnDataScanState scan_state;
			g.collection.InitializeScan(scan_state);
			DataChunk scan_chunk;
			g.collection.InitializeScanChunk(scan_chunk);

			// Batch collection — reused across batches to avoid reallocation.
			ColumnDataCollection batch(context, row_types);
			ColumnDataAppendState batch_append;
			idx_t batch_rows = 0;
			while (g.collection.Scan(scan_state, scan_chunk)) {
				if (scan_chunk.size() == 0) {
					continue;
				}
				batch.Append(scan_chunk);
				batch_rows += scan_chunk.size();

				if (batch_rows >= INSERT_BATCH_SIZE) {
					AlignedUpsertFromCollection(context, table, root, explicit_mapping, batch, row_names);
					batch.Reset();
					batch.InitializeAppend(batch_append);
					batch_rows = 0;
				}
			}
			// Flush the final partial batch.
			if (batch_rows > 0) {
				AlignedUpsertFromCollection(context, table, root, explicit_mapping, batch, row_names);
			}
		}
	} catch (std::exception &ex) {
		throw IOException("aligned INSERT failed: %s", ex.what());
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
// Shared key resolver (used by both DELETE and UPDATE)
//===----------------------------------------------------------------------===//
//! Scans the index group's (symbol, date) columns and collects the keys of the
//! given rowids into an in-memory ColumnDataCollection. No temp parquet file.
//!
//! The returned collection has three columns in the layout
//! (rowid BIGINT, symbol VARCHAR, date DATE), emitted in ascending rowid
//! order (only rowids that actually exist in the table produce a row). The
//! `op_name` parameter ("DELETE"/"UPDATE") is used in error messages. The
//! (rowid, symbol, date) superset layout lets both callers share one
//! implementation: the UPDATE caller merge-joins the result against the
//! collected set values on rowid, and the DELETE caller projects out the
//! leading rowid column to obtain the (symbol, date) collection that the
//! delete mutator requires.
static ColumnDataCollection ResolveKeysForRowids(ClientContext &context, const string &root,
                                                 const string &table_name, const vector<int64_t> &rowids,
                                                 const string &op_name) {
	auto sorted = rowids;
	sort(sorted.begin(), sorted.end());
	sorted.erase(unique(sorted.begin(), sorted.end()), sorted.end());

	vector<LogicalType> scan_types;
	vector<string> scan_names;
	auto bind_data = AlignedBindForCatalog(context, root, table_name, scan_types, scan_names);
	// v8: the primary key column names are authoritative from the table plan
	// (plan.groups[0] is the index group): symbol_column is col0,
	// partition_source is col1 (the DATE/TIMESTAMP column). Do NOT hardcode
	// "date"/"symbol" — a table may use different column names.
	auto &plan = bind_data->Cast<AlignedTableBindData>().plan;
	if (plan.groups.empty()) {
		throw IOException("aligned %s: table '%s' has no column groups to resolve keys against", op_name, table_name);
	}
	const string &symbol_name = plan.groups[0].symbol_column;
	const string &date_name = plan.groups[0].partition_source;
	if (symbol_name.empty() || date_name.empty()) {
		throw IOException("aligned %s: primary key columns not resolved for '%s' (symbol_column/partition_source "
		                  "empty in table plan)",
		                  op_name, table_name);
	}
	idx_t date_pos = DConstants::INVALID_INDEX;
	idx_t symbol_pos = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < scan_names.size(); i++) {
		if (StringUtil::CIEquals(scan_names[i], date_name)) {
			date_pos = i;
		} else if (StringUtil::CIEquals(scan_names[i], symbol_name)) {
			symbol_pos = i;
		}
	}
	if (date_pos == DConstants::INVALID_INDEX || symbol_pos == DConstants::INVALID_INDEX) {
		throw IOException("aligned %s: primary key columns (%s, %s) not found in '%s'", op_name, symbol_name, date_name,
		                  table_name);
	}

	// Scan symbol/date only (v8: symbol first, then date); logical row numbers
	// come from a sequential counter (a single local state consumes the shared
	// cursor in order).
	vector<column_t> col_ids = {symbol_pos, date_pos};
	TableFunctionInitInput init_input(bind_data.get(), col_ids, {}, nullptr);
	auto gstate = AlignedInitGlobal(context, init_input);
	ThreadContext thread_context(context);
	ExecutionContext exec(context, thread_context, nullptr);
	auto lstate = AlignedInitLocal(exec, init_input, gstate.get());

	DataChunk scan_chunk;
	scan_chunk.Initialize(context, scan_types);

	// Output layout: (rowid, symbol, date). rowid is emitted first so the
	// UPDATE merge-join can join on it; the DELETE caller drops the leading
	// rowid to obtain the (symbol, date) collection the mutator requires.
	auto out_types = vector<LogicalType> {LogicalType::BIGINT, scan_types[symbol_pos], scan_types[date_pos]};
	ColumnDataCollection keys(context, out_types);
	ColumnDataAppendState append;
	keys.InitializeAppend(append);
	DataChunk out_chunk;
	out_chunk.Initialize(context, out_types);

	idx_t next = 0;      // cursor into sorted rowids
	int64_t abs_row = 0; // logical row number of scan_chunk row 0
	while (next < sorted.size()) {
		AlignedScanFunction(context, TableFunctionInput(bind_data.get(), lstate.get(), gstate.get()), scan_chunk);
		if (scan_chunk.size() == 0) {
			break;
		}
		UnifiedVectorFormat sv, dv;
		scan_chunk.data[0].ToUnifiedFormat(scan_chunk.size(), sv); // symbol
		scan_chunk.data[1].ToUnifiedFormat(scan_chunk.size(), dv); // date/timestamp
		auto sptr = UnifiedVectorFormat::GetData<string_t>(sv);
		// The date column may be DATE (int32) or TIMESTAMP (int64).
		bool is_timestamp = scan_chunk.data[1].GetType().id() == LogicalTypeId::TIMESTAMP;
		auto dptr32 = is_timestamp ? nullptr : UnifiedVectorFormat::GetData<int32_t>(dv);
		auto dptr64 = is_timestamp ? UnifiedVectorFormat::GetData<int64_t>(dv) : nullptr;
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
				out_chunk.SetValue(0, out_n, Value::BIGINT(rid));            // rowid
				out_chunk.SetValue(1, out_n, Value(sptr[si].GetString()));   // symbol
				if (is_timestamp) {
					out_chunk.SetValue(2, out_n, Value::TIMESTAMP(timestamp_t(dptr64[di])));
				} else {
					out_chunk.SetValue(2, out_n, Value::DATE(date_t(dptr32[di])));
				}
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

	return keys;
}

//! Projects the leading rowid column out of a (rowid, symbol, date) collection
//! produced by ResolveKeysForRowids, returning a (symbol, date) collection
//! suitable for AlignedDeleteFromCollection (which requires exactly the two
//! key columns). Used by PhysicalAlignedDelete.
static ColumnDataCollection ProjectRowidFromKeys(ClientContext &context, const ColumnDataCollection &resolved) {
	auto keys_types = vector<LogicalType> {resolved.Types()[1], resolved.Types()[2]};
	ColumnDataCollection keys(context, keys_types);
	ColumnDataAppendState append;
	keys.InitializeAppend(append);
	ColumnDataScanState ss;
	DataChunk in_chunk;
	DataChunk out_chunk;
	resolved.InitializeScan(ss);
	resolved.InitializeScanChunk(in_chunk);
	out_chunk.Initialize(context, keys_types);
	while (resolved.Scan(ss, in_chunk)) {
		if (in_chunk.size() == 0) {
			continue;
		}
		out_chunk.data[0].Reference(in_chunk.data[1]); // symbol
		out_chunk.data[1].Reference(in_chunk.data[2]); // date
		out_chunk.SetCardinality(in_chunk.size());
		keys.Append(out_chunk);
		out_chunk.Reset();
		in_chunk.Reset();
	}
	return keys;
}

//===----------------------------------------------------------------------===//
// PhysicalAlignedDelete
//===----------------------------------------------------------------------===//
struct AlignedDeleteGlobalState : public GlobalSinkState {
	mutex lock;
	vector<int64_t> rowids; // selected logical row numbers
	idx_t rows_deleted = 0;
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

SinkFinalizeType PhysicalAlignedDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	auto &g = input.global_state.Cast<AlignedDeleteGlobalState>();
	if (g.rowids.empty()) {
		return SinkFinalizeType::READY;
	}

	// Direct C++ call — no worker thread, no Connection, no temp parquet.
	// 1. Resolve rowids → (rowid, symbol, date) via direct index scan.
	// 2. Project out rowid → (symbol, date) keys.
	// 3. Delete via AlignedDeleteFromCollection (in-memory keys collection).
	// Neither step touches the catalog or executor; safe on the pipeline thread.
	try {
		auto resolved = ResolveKeysForRowids(context, root, table, g.rowids, "DELETE");
		auto keys = ProjectRowidFromKeys(context, resolved);
		auto result = AlignedDeleteFromCollection(context, table, root, keys);
		g.rows_deleted = result.rows_deleted;
	} catch (std::exception &ex) {
		throw IOException("aligned DELETE failed: %s", ex.what());
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

SinkFinalizeType PhysicalAlignedUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	auto &g = input.global_state.Cast<AlignedUpdateGlobalState>();
	if (g.row_count == 0) {
		return SinkFinalizeType::READY;
	}

	// Direct C++ call — no worker thread, no Connection, no temp parquet.
	// 1. Collect rowids + set values from the sink collection.
	// 2. Resolve rowids → (symbol, date) keys via direct index scan.
	// 3. Build staged collection (symbol, date, set values...).
	// 4. Upsert via AlignedUpsertFromCollection (in-memory collection).
	// None of these steps touch the catalog or executor; safe on the pipeline
	// thread.
	try {
		// v8: the primary key column names are authoritative from the table
		// plan (plan.groups[0] is the index group): symbol_column is col0,
		// partition_source is col1 (the DATE/TIMESTAMP column). The staged
		// collection and the upsert mapping must use these names — NOT
		// hardcoded "date"/"symbol" — or the mutator's key-column validation
		// would reject a table that uses different column names.
		TablePlan plan;
		BuildTablePlan(context, root, table, plan);
		if (plan.groups.empty()) {
			throw IOException("aligned UPDATE: table '%s' has no column groups to resolve keys against", table);
		}
		const string symbol_name = plan.groups[0].symbol_column;
		const string date_name = plan.groups[0].partition_source;
		if (symbol_name.empty() || date_name.empty()) {
			throw IOException("aligned UPDATE: primary key columns not resolved for '%s' (symbol_column/partition_source "
			                  "empty in table plan)",
			                  table);
		}

		// Collect all rowids + set values from the sink collection into a
		// sorted vector for efficient merge with resolved keys.
		struct RowData {
			int64_t rowid;
			vector<Value> set_values;
		};
		vector<RowData> rows;
		rows.reserve(g.row_count);
		{
			ColumnDataScanState ss;
			g.collection.InitializeScan(ss);
			DataChunk c;
			g.collection.InitializeScanChunk(c);
			while (g.collection.Scan(ss, c)) {
				for (idx_t r = 0; r < c.size(); r++) {
					RowData rd;
					rd.rowid = c.GetValue(0, r).GetValue<int64_t>();
					for (idx_t k = 1; k < c.ColumnCount(); k++) {
						rd.set_values.push_back(c.GetValue(k, r));
					}
					rows.push_back(std::move(rd));
				}
				c.Reset();
			}
		}
		// Sort by rowid for merge with resolved keys (which are also sorted).
		sort(rows.begin(), rows.end(),
		     [](const RowData &a, const RowData &b) { return a.rowid < b.rowid; });

		// Resolve keys: (rowid, symbol, date) — v8: symbol before date. The
		// shared resolver returns a collection in this exact layout.
		vector<int64_t> rowids;
		rowids.reserve(rows.size());
		for (auto &rd : rows) {
			rowids.push_back(rd.rowid);
		}
		auto keys = ResolveKeysForRowids(context, root, table, rowids, "UPDATE");

		// staged schema: <symbol_col>, <date_col>, then set columns in
		// set_names order (v8). The key column NAMES come from the table plan.
		// The date column type must match the index group's actual type
		// (DATE or TIMESTAMP) — using a hardcoded DATE would truncate
		// TIMESTAMP keys to midnight.
		LogicalType date_type = LogicalType::DATE;
		auto &index_gp = plan.groups[0];
		for (idx_t ci = 0; ci < index_gp.column_order.size(); ci++) {
			if (StringUtil::CIEquals(index_gp.column_order[ci], date_name)) {
				date_type = index_gp.schema_types[ci];
				break;
			}
		}
		vector<LogicalType> staged_types;
		vector<string> staged_names;
		staged_types.push_back(LogicalType::VARCHAR);
		staged_names.push_back(symbol_name);
		staged_types.push_back(date_type);
		staged_names.push_back(date_name);
		for (auto &expr : expressions) {
			staged_types.push_back(expr->return_type);
		}
		for (auto &nm : set_names) {
			staged_names.push_back(nm);
		}

		ColumnDataCollection staged_coll(context, staged_types);
		ColumnDataAppendState staged_append;
		staged_coll.InitializeAppend(staged_append);
		DataChunk out_chunk;
		out_chunk.Initialize(context, staged_types);
		idx_t out_n = 0;

		// Merge-join the resolved keys (sorted by rowid) with the collected
		// set values (sorted by rowid). Both sides are sorted → single pass,
		// no hash map needed.
		ColumnDataScanState ss;
		keys.InitializeScan(ss);
		DataChunk kc;
		keys.InitializeScanChunk(kc);
		idx_t row_idx = 0; // cursor into `rows`
		while (keys.Scan(ss, kc)) {
			for (idx_t r = 0; r < kc.size(); r++) {
				int64_t rid = kc.GetValue(0, r).GetValue<int64_t>();
				// Advance `rows` cursor to match rid (both sorted).
				while (row_idx < rows.size() && rows[row_idx].rowid < rid) {
					row_idx++;
				}
				if (row_idx >= rows.size() || rows[row_idx].rowid != rid) {
					continue; // key not in set rows (shouldn't happen)
				}
				// kc layout: (rowid, symbol, date) — v8
				out_chunk.SetValue(0, out_n, kc.GetValue(1, r)); // symbol
				out_chunk.SetValue(1, out_n, kc.GetValue(2, r)); // date
				for (idx_t k = 0; k < rows[row_idx].set_values.size(); k++) {
					out_chunk.SetValue(2 + k, out_n, rows[row_idx].set_values[k]);
				}
				out_n++;
				row_idx++;
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

		// Upsert mapping (v8): index:<symbol_col>,<date_col> (+ any
		// index-group set columns); each non-index set group gets its own
		// mapping segment with comma-separated columns. The key column
		// names come from the table plan. Columns in the same group must
		// be merged into one segment (e.g. "g1/data:val,info"), not
		// repeated as separate segments.
		string index_cols = symbol_name + "," + date_name;
		// Collect non-index set columns per group (preserve first-seen order).
		vector<string> group_order;
		case_insensitive_map_t<vector<string>> group_cols;
		for (idx_t i = 0; i < set_names.size(); i++) {
			if (set_groups[i].empty()) {
				continue;
			}
			if (set_groups[i] == "index") {
				index_cols += "," + set_names[i];
			} else {
				if (group_cols.find(set_groups[i]) == group_cols.end()) {
					group_order.push_back(set_groups[i]);
				}
				group_cols[set_groups[i]].push_back(set_names[i]);
			}
		}
		string mapping = "index:" + index_cols;
		for (auto &grp : group_order) {
			mapping += ";" + grp + ":";
			for (idx_t j = 0; j < group_cols[grp].size(); j++) {
				if (j > 0) {
					mapping += ",";
				}
				mapping += group_cols[grp][j];
			}
		}

		auto result = AlignedUpsertFromCollection(context, table, root, mapping, staged_coll, staged_names);
		g.row_count = result.rows_updated;
	} catch (std::exception &ex) {
		throw IOException("aligned UPDATE failed: %s", ex.what());
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
