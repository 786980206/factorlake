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

	// Determine which bare column names are duplicated across non-index
	// groups. Duplicated names must use the qualified "lv1.lv2.col" form
	// to be queryable; unique names use the bare name.
	case_insensitive_map_t<idx_t> non_index_col_counts;
	for (auto &g : plan.groups) {
		if (g.manifest.group == "index") {
			continue;
		}
		for (auto &col : g.column_order) {
			non_index_col_counts[col]++;
		}
	}

	// Collect index column names for shadow detection.
	case_insensitive_set_t index_cols;
	if (!plan.groups.empty()) {
		for (auto &ic : plan.groups[0].column_order) {
			index_cols.insert(ic);
		}
	}

	// schema: "queryable_name:type,..." — index columns use bare names,
	// non-index unique columns use bare names, cross-group duplicated
	// columns use qualified "lv1.lv2.col" (one entry per owning group).
	// Index-shadow columns (same name in index) are skipped from non-index
	// groups (they are the index column, not a separate queryable column).
	string schema_str;
	case_insensitive_set_t seen_bare;
	for (auto &g : plan.groups) {
		bool is_index = (g.manifest.group == "index");
		for (idx_t c = 0; c < g.column_order.size(); c++) {
			auto &col_name = g.column_order[c];
			if (!is_index && index_cols.count(col_name) > 0) {
				// Index shadow: the scan path ignores this column in
				// non-index groups. Skip it from the schema too.
				continue;
			}
			bool duplicated = false;
			if (!is_index) {
				auto it = non_index_col_counts.find(col_name);
				duplicated = (it != non_index_col_counts.end() && it->second > 1);
			}
			if (is_index || !duplicated) {
				// Bare name: skip if already seen.
				if (seen_bare.find(col_name) != seen_bare.end()) {
					continue;
				}
				seen_bare.insert(col_name);
				if (!schema_str.empty()) {
					schema_str += ",";
				}
				schema_str += col_name;
				schema_str += ":";
				schema_str += g.schema_types[c].ToString();
			} else {
				// Duplicated: qualified name, one per owning group.
				if (!schema_str.empty()) {
					schema_str += ",";
				}
				schema_str += g.lv1 + "." + g.lv2 + "." + col_name;
				schema_str += ":";
				schema_str += g.schema_types[c].ToString();
			}
		}
	}
	output.SetValue(9, 0, Value(schema_str));

	// column_mapping: "queryable_name:lv1.lv2.col;..." for every non-index
	// column. queryable_name = bare name for unique columns, qualified
	// "lv1.lv2.col" for cross-group duplicated columns (which are their
	// own alias). Index-shadow columns are skipped.
	string mapping_str;
	for (auto &g : plan.groups) {
		if (g.manifest.group == "index") {
			continue;
		}
		for (idx_t c = 0; c < g.column_order.size(); c++) {
			auto &col_name = g.column_order[c];
			// Skip columns that also exist in the index group (shadows).
			if (index_cols.count(col_name) > 0) {
				continue;
			}
			bool duplicated = false;
			auto cnt_it = non_index_col_counts.find(col_name);
			if (cnt_it != non_index_col_counts.end() && cnt_it->second > 1) {
				duplicated = true;
			}
			auto qualified = g.lv1 + "." + g.lv2 + "." + col_name;
			if (!mapping_str.empty()) {
				mapping_str += ";";
			}
			// For unique columns: bare_name:lv1.lv2.col
			// For duplicated columns: lv1.lv2.col:lv1.lv2.col (self-alias)
			mapping_str += duplicated ? qualified : col_name;
			mapping_str += ":";
			mapping_str += qualified;
		}
	}
	output.SetValue(10, 0, Value(mapping_str));

	output.SetCardinality(1);
}

} // namespace duckdb
