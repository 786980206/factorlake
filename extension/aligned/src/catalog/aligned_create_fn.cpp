#include "catalog/aligned_create_fn.hpp"
#include "catalog/aligned_create.hpp"
#include "catalog/manifest.hpp"
#include "mutator/aligned_mutator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parallel/async_result.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data / state
//===----------------------------------------------------------------------===//

struct AlignedCreateBindData : public TableFunctionData {
	string table_name;
	string root;
	vector<ColumnDefinition> columns;
	string groups_option;
	string partition_template;
	vector<LogicalType> types;
	vector<string> names;
};

struct AlignedCreateGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

unique_ptr<FunctionData> AlignedCreateBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AlignedCreateBindData>();

	if (input.inputs.size() != 2) {
		throw BinderException("aligned_create: expected (table_name, columns)");
	}

	result->table_name = StringValue::Get(input.inputs[0]);
	if (result->table_name.empty()) {
		throw BinderException("aligned_create: table_name must not be empty");
	}

	// Parse the column definition string using DuckDB's SQL parser.
	// e.g. "symbol VARCHAR, date DATE, close DOUBLE" → ColumnList
	string columns_str = StringValue::Get(input.inputs[1]);
	if (columns_str.empty()) {
		throw BinderException("aligned_create: columns definition must not be empty");
	}
	auto column_list = Parser::ParseColumnList(columns_str);
	for (auto &col : column_list.Logical()) {
		auto col_copy = col.Copy();
		// The parser returns UNBOUND types for column definitions; resolve
		// them to concrete LogicalTypes using TransformStringToLogicalType.
		if (col_copy.Type().id() == LogicalTypeId::UNBOUND) {
			col_copy.SetType(TransformStringToLogicalType(col_copy.Type().ToString(), context));
		}
		result->columns.push_back(std::move(col_copy));
	}

	// Optional groups mapping (named parameter)
	auto groups_entry = input.named_parameters.find("groups");
	if (groups_entry != input.named_parameters.end() && !groups_entry->second.IsNull()) {
		result->groups_option = StringValue::Get(groups_entry->second);
	}

	// Named parameters
	result->partition_template = "month=%Y-%m"; // default
	auto pt = input.named_parameters.find("partition_template");
	if (pt != input.named_parameters.end() && !pt->second.IsNull()) {
		result->partition_template = StringValue::Get(pt->second);
	}

	auto root_entry = input.named_parameters.find("root");
	if (root_entry != input.named_parameters.end() && !root_entry->second.IsNull()) {
		result->root = StringValue::Get(root_entry->second);
	} else {
		Value setting_value;
		if (!context.TryGetCurrentSetting("aligned_data_root", setting_value)) {
			throw BinderException("aligned_create: no data root configured. Pass root='...' or SET aligned_data_root");
		}
		result->root = StringValue::Get(setting_value);
	}

	result->types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	result->names = {"dirs_created", "files_created", "txid"};
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> AlignedCreateInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<AlignedCreateGlobalState>();
}

//===----------------------------------------------------------------------===//
// Create
//===----------------------------------------------------------------------===//

//! Recursively count files and subdirectories under a path.
//! Skips the `.aligned_write.lock` file (transient, created by TableWriteLock).
static void CountRecursive(FileSystem &fs, const string &path, idx_t &dirs_count, idx_t &files_count) {
	fs.ListFiles(path, [&](OpenFileInfo &info) {
		// Skip the write lock file
		if (StringUtil::CIEquals(info.path, ".aligned_write.lock")) {
			return;
		}
		string child = path + "/" + info.path;
		if (fs.DirectoryExists(child)) {
			dirs_count++;
			CountRecursive(fs, child, dirs_count, files_count);
		} else {
			files_count++;
		}
	});
}

void AlignedCreateFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<AlignedCreateBindData>();
	auto &gstate = data.global_state->Cast<AlignedCreateGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &fs = FileSystem::GetFileSystem(context);
	string table_dir = bind.root + "/" + bind.table_name;

	// Acquire write lock for mutual exclusion with concurrent writers.
	TableWriteLock write_lock(fs, table_dir);

	idx_t txid = NextTransactionId();

	// Delegate to the existing AlignedCreateTable helper (new-table mode).
	AlignedCreateTable(context, bind.root, bind.table_name, bind.columns,
	                   bind.groups_option, bind.partition_template);

	// Count the created dirs and files.
	idx_t dirs_created = 0;
	idx_t files_created = 0;
	if (fs.DirectoryExists(table_dir)) {
		CountRecursive(fs, table_dir, dirs_created, files_created);
	}

	// Emit one result row.
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(dirs_created)));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(files_created)));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(txid)));
}

} // namespace duckdb
