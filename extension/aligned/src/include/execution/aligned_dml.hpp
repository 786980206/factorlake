#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class AlignedTableEntry;

//! Standard-INSERT sink for aligned attached tables (Phase 8b). Buffers the
//! incoming rows, writes them to a temp parquet under the table's _tmp dir,
//! then reuses the aligned_upsert mutator to place them into the column
//! groups atomically. Emits one Count row (rows written).
class PhysicalAlignedInsert : public PhysicalOperator {
public:
	//! types_p = operator output ({Count}); row_types/row_names = the table's
	//! full row schema (what the child plan produces and what we stage).
	PhysicalAlignedInsert(PhysicalPlan &plan, vector<LogicalType> types_p, vector<LogicalType> row_types_p,
	                      vector<string> row_names_p, const string &table, const string &root, idx_t est_card,
	                      string explicit_mapping = "");

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	bool IsSource() const override {
		return true;
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

private:
	string table;
	string root;
	vector<LogicalType> row_types;
	vector<string> row_names;
	//! When non-empty, the INSERT specified a subset of columns. This mapping
	//! string tells the mutator exactly which group+columns were set, so groups
	//! whose columns were not in the INSERT column list can be skipped entirely
	//! (no RewritePart). Format: "index:sym,date[,cols];group:cols".
	string explicit_mapping;
};
//! Standard-UPDATE for aligned attached tables (upsert semantics: matched
//! keys are rewritten in place; unmatched keys would be inserted by the
//! underlying aligned_upsert). BindUpdateConstraints on the table entry
//! forces a full-row fetch, so each sink chunk carries complete new row
//! images which are staged and handed to the mutator.
class PhysicalAlignedUpdate : public PhysicalOperator {
public:
	PhysicalAlignedUpdate(PhysicalPlan &plan, vector<LogicalType> types_p, vector<string> set_names_p,
	                      vector<string> set_groups_p, const string &table, const string &root, idx_t est_card);

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	bool IsSource() const override {
		return true;
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

	//! New-value expressions per updated column (BoundReference into the
	//! child chunk, or VALUE_DEFAULT), mirroring LogicalUpdate.
	vector<unique_ptr<Expression>> expressions;
	//! Column name / owning group per expression (aligned with expressions).
	vector<string> set_names;
	vector<string> set_groups;

private:
	string table;
	string root;
	vector<LogicalType> row_types;
	vector<string> row_names;
};

//! Standard-DELETE for aligned attached tables. Collects the rowids selected
//! by the WHERE clause, resolves them to (date, symbol) keys by scanning the
//! index group, then reuses the aligned_delete mutator on a worker thread.
class PhysicalAlignedDelete : public PhysicalOperator {
public:
	PhysicalAlignedDelete(PhysicalPlan &plan, vector<LogicalType> types_p, const string &table, const string &root,
	                      idx_t est_card);

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	bool IsSource() const override {
		return true;
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

private:
	string table;
	string root;
};

} // namespace duckdb




