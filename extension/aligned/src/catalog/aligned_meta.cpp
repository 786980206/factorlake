#include "catalog/aligned_meta.hpp"
#include "catalog/manifest.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data / state
//===----------------------------------------------------------------------===//

struct AlignedMetaBindData : public TableFunctionData {
	TablePlan plan;
	vector<LogicalType> types;
	vector<string> names;
};

struct AlignedMetaGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

unique_ptr<FunctionData> AlignedMetaBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AlignedMetaBindData>();

	if (input.inputs.size() != 1) {
		throw BinderException("aligned_meta: expected (table_name)");
	}
	string table = StringValue::Get(input.inputs[0]);

	auto root_it = input.named_parameters.find("root");
	const Value *root_param = (root_it != input.named_parameters.end()) ? &root_it->second : nullptr;
	string root = ResolveDataRoot(context, root_param, "aligned_meta");

	BuildTablePlan(context, root, table, result->plan);

	result->types = {
	    LogicalType::VARCHAR,  // table_name
	    LogicalType::VARCHAR,  // table_path
	    LogicalType::VARCHAR,  // partition_template
	    LogicalType::BIGINT,   // total_rows
	    LogicalType::BIGINT,   // group_count
	    LogicalType::BIGINT,   // partition_count
	    LogicalType::BIGINT,   // part_count
	    LogicalType::VARCHAR,  // groups
	    LogicalType::VARCHAR,  // partitions
	    LogicalType::VARCHAR,  // schema
	    LogicalType::VARCHAR,  // column_mapping
	};
	result->names = {"table_name",   "table_path",      "partition_template", "total_rows",
	                 "group_count",  "partition_count", "part_count",         "groups",
	                 "partitions",   "schema",          "column_mapping"};
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> AlignedMetaInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<AlignedMetaGlobalState>();
}

//===----------------------------------------------------------------------===//
// Function
//===----------------------------------------------------------------------===//

void AlignedMetaFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<AlignedMetaBindData>();
	auto &gstate = data.global_state->Cast<AlignedMetaGlobalState>();

	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &plan = bind.plan;

	// table_name
	output.SetValue(0, 0, Value(plan.table_name));

	// table_path
	output.SetValue(1, 0, Value(plan.table_path));

	// partition_template (from index group)
	string partition_template;
	if (!plan.groups.empty() && !plan.groups[0].manifest.partitioning.empty()) {
		partition_template = plan.groups[0].manifest.partitioning[0].template_str;
	}
	output.SetValue(2, 0, Value(partition_template));

	// total_rows (from index group partitions)
	idx_t total_rows = plan.row_count;
	output.SetValue(3, 0, Value::BIGINT(NumericCast<int64_t>(total_rows)));

	// group_count
	idx_t group_count = plan.groups.size();
	output.SetValue(4, 0, Value::BIGINT(NumericCast<int64_t>(group_count)));

	// partition_count (from index group)
	idx_t partition_count = 0;
	if (!plan.groups.empty()) {
		partition_count = plan.groups[0].partitions.size();
	}
	output.SetValue(5, 0, Value::BIGINT(NumericCast<int64_t>(partition_count)));

	// part_count (total parquet files across all groups)
	idx_t part_count = 0;
	for (auto &g : plan.groups) {
		part_count += g.parts.size();
	}
	output.SetValue(6, 0, Value::BIGINT(NumericCast<int64_t>(part_count)));

	// groups: "group_name:col1,col2;group_name2:col3,col4"
	string groups_str;
	for (idx_t gi = 0; gi < plan.groups.size(); gi++) {
		auto &g = plan.groups[gi];
		if (gi > 0) {
			groups_str += ";";
		}
		groups_str += g.manifest.group;
		groups_str += ":";
		for (idx_t c = 0; c < g.column_order.size(); c++) {
			if (c > 0) {
				groups_str += ",";
			}
			groups_str += g.column_order[c];
		}
	}
	output.SetValue(7, 0, Value(groups_str));

	// partitions: "key1,key2,..." (from index group)
	string partitions_str;
	if (!plan.groups.empty()) {
		auto &index_group = plan.groups[0];
		for (idx_t p = 0; p < index_group.partitions.size(); p++) {
			if (p > 0) {
				partitions_str += ",";
			}
			partitions_str += index_group.partitions[p].key;
		}
	}
	output.SetValue(8, 0, Value(partitions_str));

	// schema: "col_name:type,col_name:type,..." (full table schema: all
	// unique columns from index group first, then non-index group columns
	// that don't already appear, in group order)
	string schema_str;
	case_insensitive_set_t seen_cols;
	for (auto &g : plan.groups) {
		for (idx_t c = 0; c < g.column_order.size(); c++) {
			auto &col_name = g.column_order[c];
			if (seen_cols.find(col_name) != seen_cols.end()) {
				continue; // skip duplicate column names across groups
			}
			seen_cols.insert(col_name);
			if (!schema_str.empty()) {
				schema_str += ",";
			}
			schema_str += col_name;
			schema_str += ":";
			schema_str += g.schema_types[c].ToString();
		}
	}
	output.SetValue(9, 0, Value(schema_str));

	// column_mapping: "bare_name:lv1.lv2.bare_name;bare_name2:lv1.lv2.bare_name2;..."
	// Maps each non-index unique column's bare name to its qualified
	// "lv1.lv2.col" alias. Index columns and duplicated cross-group columns
	// (which only have the qualified name) are not included.
	string mapping_str;
	for (auto &g : plan.groups) {
		if (g.manifest.group == "index") {
			continue;
		}
		for (idx_t c = 0; c < g.column_order.size(); c++) {
			auto &col_name = g.column_order[c];
			// Skip columns that also exist in the index group (shadows).
			bool in_index = false;
			if (!plan.groups.empty()) {
				for (auto &ic : plan.groups[0].column_order) {
					if (StringUtil::CIEquals(ic, col_name)) {
						in_index = true;
						break;
					}
				}
			}
			if (in_index) {
				continue;
			}
			// Skip duplicated cross-group columns (only have qualified name).
			bool duplicated = false;
			for (auto &g2 : plan.groups) {
				if (g2.manifest.group == "index") {
					continue;
				}
				if (g2.manifest.group == g.manifest.group) {
					continue;
				}
				for (auto &c2 : g2.column_order) {
					if (StringUtil::CIEquals(c2, col_name)) {
						duplicated = true;
						break;
					}
				}
				if (duplicated) {
					break;
				}
			}
			if (duplicated) {
				continue;
			}
			if (!mapping_str.empty()) {
				mapping_str += ";";
			}
			mapping_str += col_name;
			mapping_str += ":";
			mapping_str += g.lv1 + "." + g.lv2 + "." + col_name;
		}
	}
	output.SetValue(10, 0, Value(mapping_str));

	output.SetCardinality(1);
}

} // namespace duckdb
