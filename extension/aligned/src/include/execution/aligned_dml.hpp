#pragma once

#include "duckdb/execution/physical_operator.hpp"

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
	                      vector<string> row_names_p, const string &table, const string &root, idx_t est_card);

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
};

} // namespace duckdb
