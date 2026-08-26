#include "catalog/aligned_groups.hpp"
#include "catalog/manifest.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data / state
//===----------------------------------------------------------------------===//

struct AlignedGroupsBindData : public TableFunctionData {
	TablePlan plan;
	vector<LogicalType> types;
	vector<string> names;
};

struct AlignedGroupsGlobalState : public GlobalTableFunctionState {
	idx_t next_group = 0;
};

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

unique_ptr<FunctionData> AlignedGroupsBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AlignedGroupsBindData>();

	if (input.inputs.size() != 1) {
		throw BinderException("aligned_groups: expected (table_name)");
	}
	string table = StringValue::Get(input.inputs[0]);

	auto root_it = input.named_parameters.find("root");
	const Value *root_param = (root_it != input.named_parameters.end()) ? &root_it->second : nullptr;
	string root = ResolveDataRoot(context, root_param, "aligned_groups");

	BuildTablePlan(context, root, table, result->plan);

	result->types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
	result->names = {"group_name", "columns", "partition_count"};
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> AlignedGroupsInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<AlignedGroupsGlobalState>();
}

//===----------------------------------------------------------------------===//
// Function
//===----------------------------------------------------------------------===//

void AlignedGroupsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<AlignedGroupsBindData>();
	auto &gstate = data.global_state->Cast<AlignedGroupsGlobalState>();

	idx_t rows_emitted = 0;
	while (gstate.next_group < bind.plan.groups.size() && rows_emitted < STANDARD_VECTOR_SIZE) {
		auto &g = bind.plan.groups[gstate.next_group];

		// group_name
		output.SetValue(0, rows_emitted, Value(g.manifest.group));

		// columns: semicolon-separated column names (avoids ambiguity with
		// SQLLogicTest's comma-delimited output format)
		string columns_str;
		for (idx_t i = 0; i < g.column_order.size(); i++) {
			if (i > 0) {
				columns_str += ";";
			}
			columns_str += g.column_order[i];
		}
		output.SetValue(1, rows_emitted, Value(columns_str));

		// partition_count
		output.SetValue(2, rows_emitted, Value::BIGINT(NumericCast<int64_t>(g.partitions.size())));

		gstate.next_group++;
		rows_emitted++;
	}

	output.SetCardinality(rows_emitted);
}

} // namespace duckdb
