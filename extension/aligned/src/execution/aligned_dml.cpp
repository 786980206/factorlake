//! Standard DML physical operators for aligned attached tables (Phase 8).
//!
//! INSERT/UPDATE/DELETE: buffer rows in-memory -> direct C++ mutator call
//! (AlignedUpsertFromCollection / AlignedDeleteFromCollection). No temp parquet
//! file, no worker thread, no nested SQL query. The mutator does only
//! filesystem + parquet I/O (no catalog/executor access), so it is safe to
//! call directly on the pipeline thread.

#include "execution/aligned_dml.hpp"

#include "catalog/aligned_catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "mutator/aligned_mutator.hpp"
#include "scan/aligned_scan.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"

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

	// Direct C++ call — no worker thread, no Connection, no temp parquet.
	// The mutator (BuildTablePlan + KeyResolver + ExecuteAndCommit) does only
	// filesystem + parquet I/O; it does NOT touch the catalog or executor, so
	// there is no nested-query deadlock risk on the pipeline thread.
	try {
		auto result = AlignedUpsertFromCollection(context, table, root, "", g.collection, row_names);
		g.rows_inserted = result.rows_inserted;
		g.rows_updated = result.rows_updated;
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

//! Scans the index group's (symbol, date) columns and collects the keys of the
//! given rowids into an in-memory ColumnDataCollection. No temp parquet file.
static ColumnDataCollection ResolveKeysForRowids(ClientContext &context, const string &root,
                                                 const string &table_name, const vector<int64_t> &rowids) {
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

	return keys;
}

SinkFinalizeType PhysicalAlignedDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	auto &g = input.global_state.Cast<AlignedDeleteGlobalState>();
	if (g.rowids.empty()) {
		return SinkFinalizeType::READY;
	}

	// Direct C++ call — no worker thread, no Connection, no temp parquet.
	// 1. Resolve rowids → (symbol, date) keys via direct index scan.
	// 2. Delete via AlignedDeleteFromCollection (in-memory keys collection).
	// Neither step touches the catalog or executor; safe on the pipeline thread.
	try {
		auto keys = ResolveKeysForRowids(context, root, table, g.rowids);
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

	// Direct C++ call — no worker thread, no Connection, no temp parquet.
	// 1. Collect rowids + set values from the sink collection.
	// 2. Resolve rowids → (symbol, date) keys via direct index scan.
	// 3. Build staged collection (symbol, date, set values...).
	// 4. Upsert via AlignedUpsertFromCollection (in-memory collection).
	// None of these steps touch the catalog or executor; safe on the pipeline
	// thread.
	try {
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
		ColumnDataCollection keys(context, keys_types);
		ColumnDataAppendState append;
		keys.InitializeAppend(append);
		ResolveKeysForRowids(context, root, table, rowids, keys, append);

		// staged schema: symbol, date, then set columns in set_names order (v8)
		vector<LogicalType> staged_types;
		vector<string> staged_names;
		staged_types.push_back(LogicalType::VARCHAR);
		staged_names.push_back("symbol");
		staged_types.push_back(LogicalType::DATE);
		staged_names.push_back("date");
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

		// Upsert mapping (v8): index:symbol,date(+ any index-group set
		// columns); each non-index set group gets its own mapping segment.
		string index_cols = "symbol,date";
		string mapping = "index:symbol,date";
		for (idx_t i = 0; i < set_names.size(); i++) {
			if (set_groups[i].empty()) {
				continue;
			}
			if (set_groups[i] == "index") {
				index_cols += "," + set_names[i];
			} else {
				mapping += ";" + set_groups[i] + ":" + set_names[i];
			}
		}
		if (index_cols != "symbol,date") {
			mapping = "index:" + index_cols + mapping.substr(strlen("index:symbol,date"));
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
