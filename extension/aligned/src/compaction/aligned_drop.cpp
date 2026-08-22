#include "compaction/aligned_drop.hpp"

#include "catalog/manifest.hpp"
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

//! Recursively count files and subdirectories under a path.
static void CountRecursive(FileSystem &fs, const string &path, idx_t &dirs_count, idx_t &files_count) {
	fs.ListFiles(path, [&](OpenFileInfo &info) {
		string child = path + "/" + info.path;
		if (fs.DirectoryExists(child)) {
			dirs_count++;
			CountRecursive(fs, child, dirs_count, files_count);
		} else {
			files_count++;
		}
	});
}

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

	string root;
	auto entry = input.named_parameters.find("root");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		root = StringValue::Get(entry->second);
	} else {
		Value setting_value;
		if (!context.TryGetCurrentSetting("aligned_data_root", setting_value)) {
			throw BinderException("aligned_drop: no data root configured. Pass root='...' or SET aligned_data_root");
		}
		root = StringValue::Get(setting_value);
	}

	BuildTablePlan(context, root, table, result->plan);

	// Validate the group name against the discovered groups. "index" is
	// always valid (it drops the entire table). Other names must match an
	// existing group.
	bool found = StringUtil::CIEquals(result->group_name, "index");
	if (!found) {
		for (auto &g : result->plan.groups) {
			if (StringUtil::CIEquals(g.manifest.group, result->group_name)) {
				found = true;
				break;
			}
		}
	}
	if (!found) {
		throw BinderException("aligned_drop: unknown group '%s' in table '%s'", result->group_name, table);
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
		// Dropping a single non-index column group: find the group's
		// directory path and remove it. Other groups (including index)
		// remain untouched.
		string group_path;
		for (auto &g : bind.plan.groups) {
			if (StringUtil::CIEquals(g.manifest.group, bind.group_name)) {
				group_path = g.group_path;
				break;
			}
		}
		if (group_path.empty()) {
			// Should not happen — bind validates the group name.
			throw InternalException("aligned_drop: group '%s' not found in plan", bind.group_name);
		}
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
