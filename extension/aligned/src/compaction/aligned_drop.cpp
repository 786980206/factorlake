#include "compaction/aligned_drop.hpp"

#include "catalog/manifest.hpp"
#include "io/parquet_io.hpp"
#include "mutator/aligned_mutator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parallel/async_result.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data / state
//===----------------------------------------------------------------------===//

struct AlignedDropBindData : public TableFunctionData {
	TablePlan plan;
	string group_name; // "index" or a column group path like "factor/alpha101"
	vector<LogicalType> types;
	vector<string> names;
};

struct AlignedDropGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

//===----------------------------------------------------------------------===//
// Helpers
//===----------------------------------------------------------------------===//

//! (CountRecursive is now in io/parquet_io.hpp — shared with aligned_create)

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

unique_ptr<FunctionData> AlignedDropBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AlignedDropBindData>();
	if (input.inputs.size() != 2) {
		throw BinderException("aligned_drop: expected (table_name, group_name)");
	}
	result->group_name = StringValue::Get(input.inputs[1]);
	string table = StringValue::Get(input.inputs[0]);

	auto root_it = input.named_parameters.find("root");
	const Value *root_param = (root_it != input.named_parameters.end()) ? &root_it->second : nullptr;
	string root = ResolveDataRoot(context, root_param, "aligned_drop");

	// Normalize root path (strip trailing separators)
	while (!root.empty() && (root.back() == '/' || root.back() == '\\')) {
		root.pop_back();
	}

	auto &fs = FileSystem::GetFileSystem(context);
	result->plan.table_path = root + "/" + table;
	result->plan.table_name = table;

	if (!fs.DirectoryExists(result->plan.table_path)) {
		throw IOException("Aligned table '%s': table directory does not exist at '%s'", table,
		                  result->plan.table_path);
	}

	// For "index" (drop entire table), we don't need to validate groups.
	if (!StringUtil::CIEquals(result->group_name, "index")) {
		// For a single group drop, find the group directory directly.
		// We use a glob to discover groups — but skip the partition-aligned
		// contract validation that BuildTablePlan performs, since the user
		// is deleting the group anyway.
		string group_path = result->plan.table_path + "/" + result->group_name;
		if (!fs.DirectoryExists(group_path)) {
			throw BinderException("aligned_drop: unknown group '%s' in table '%s'",
			                      result->group_name, table);
		}
	}

	result->types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	result->names = {"dirs_removed", "files_removed", "txid"};
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> AlignedDropInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<AlignedDropGlobalState>();
}

//===----------------------------------------------------------------------===//
// Drop
//===----------------------------------------------------------------------===//

void AlignedDropFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<AlignedDropBindData>();
	auto &gstate = data.global_state->Cast<AlignedDropGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &fs = FileSystem::GetFileSystem(context);

	// Acquire the table-level write lock for mutual exclusion with concurrent
	// writers (aligned_upsert / aligned_delete / aligned_compact).
	TableWriteLock write_lock(fs, bind.plan.table_path);

	idx_t txid = NextTransactionId();

	idx_t dirs_removed = 0;
	idx_t files_removed = 0;

	if (StringUtil::CIEquals(bind.group_name, "index")) {
		// Dropping the index group = dropping the entire table.
		// Count all files and subdirectories under the table directory,
		// then remove the entire table directory.
		if (fs.DirectoryExists(bind.plan.table_path)) {
			CountRecursive(fs, bind.plan.table_path, dirs_removed, files_removed);
			fs.RemoveDirectory(bind.plan.table_path);
		}
	} else {
		// Dropping a single non-index column group: remove the group's
		// directory directly. Other groups (including index) remain untouched.
		string group_path = bind.plan.table_path + "/" + bind.group_name;
		if (fs.DirectoryExists(group_path)) {
			CountRecursive(fs, group_path, dirs_removed, files_removed);
			fs.RemoveDirectory(group_path);
		}
	}

	// Emit one result row.
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(dirs_removed)));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(files_removed)));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(txid)));
}

} // namespace duckdb
