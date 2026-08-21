//! aligned_attach / aligned_detach (Phase 8)
//!
//! Register every logical table found under the data root as a REAL DuckDB
//! catalog table (`CREATE OR REPLACE TABLE name AS SELECT * FROM
//! aligned_table(name)`). After attach, standard SQL works against the bare
//! table name: SELECT / INSERT / UPDATE / DELETE. The catalog table is a
//! DuckDB-native materialization of the aligned storage; writes accumulate in
//! DuckDB's own storage until synced back (see README).
//!
//! Locking: the DDL MUST run at execution time (init_global), not at bind
//! time. The outer query's bind holds a read lock on the catalog while it
//! resolves `aligned_attach`; issuing CREATE/DROP there would wait for the
//! same catalog's write lock 鈫?deadlock.

#include "catalog/aligned_attach.hpp"

#include <thread>

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

struct AlignedAttachBindData : public FunctionData {
	string root;
	bool detach = false;
	vector<string> tables; // discovered candidate names only

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<AlignedAttachBindData>();
		result->root = root;
		result->detach = detach;
		result->tables = tables;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<AlignedAttachBindData>();
		return root == other.root && detach == other.detach;
	}
};

struct AlignedAttachGlobalState : public GlobalTableFunctionState {
	vector<string> tables;
	vector<string> statuses;
	bool finished = false;
};

//! Lists candidate logical-table directories under `root` (one level deep,
//! skipping dot/underscore-prefixed entries like `_tmp`).
static vector<string> DiscoverTables(FileSystem &fs, const string &root) {
	vector<string> result;
	if (!fs.DirectoryExists(root)) {
		throw IOException("aligned_attach: data root does not exist: '%s'", root);
	}
	fs.ListFiles(root, [&](const string &fname, bool is_dir) {
		if (!is_dir) {
			return;
		}
		if (fname.empty() || fname[0] == '.' || fname[0] == '_') {
			return;
		}
		result.push_back(fname);
	});
	sort(result.begin(), result.end());
	return result;
}

static string ResolveRoot(ClientContext &context, const named_parameter_map_t &named_params) {
	auto entry = named_params.find("root");
	if (entry != named_params.end() && !entry->second.IsNull()) {
		return StringValue::Get(entry->second);
	}
	Value setting_value;
	if (!context.TryGetCurrentSetting("aligned_data_root", setting_value)) {
		throw BinderException("aligned_attach: no data root configured. Use aligned_attach(root='...') or "
		                      "SET aligned_data_root = '...'");
	}
	return StringValue::Get(setting_value);
}

static unique_ptr<FunctionData> AttachBindInternal(ClientContext &context, const string &root, bool detach,
                                                   const vector<Value> &inputs, vector<LogicalType> &return_types,
                                                   vector<string> &names) {
	auto result = make_uniq<AlignedAttachBindData>();
	result->root = root;
	result->detach = detach;
	if (!inputs.empty()) {
		// Single-table form: aligned_attach('cnstk_ixday')
		result->tables.push_back(StringValue::Get(inputs[0]));
	} else {
		auto fs = FileSystem::CreateLocal();
		result->tables = DiscoverTables(*fs, root);
	}

	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	names = {"table_name", "status"};
	return std::move(result);
}

static unique_ptr<FunctionData> AttachBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto root = ResolveRoot(context, input.named_parameters);
	return AttachBindInternal(context, root, false, input.inputs, return_types, names);
}

static unique_ptr<FunctionData> DetachBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto root = ResolveRoot(context, input.named_parameters);
	return AttachBindInternal(context, root, true, input.inputs, return_types, names);
}

static unique_ptr<GlobalTableFunctionState> AttachInitGlobal(ClientContext &context,
                                                             TableFunctionInitInput &input) {
	auto gstate = make_uniq<AlignedAttachGlobalState>();
	auto &bind = input.bind_data->Cast<AlignedAttachBindData>();
	gstate->tables = bind.tables;
	gstate->statuses.assign(bind.tables.size(), "");

	// Run the DDL on a dedicated thread: issuing a nested synchronous query
	// from inside this query's own execution deadlocks.
	auto &db = DatabaseInstance::GetDatabase(context);
	auto *root = &bind.root;
	auto detach = bind.detach;
	std::thread worker([&db, root, detach, gstate_ptr = gstate.get()]() {
		try {
			Connection con(db);
			for (idx_t i = 0; i < gstate_ptr->tables.size(); i++) {
				auto &table = gstate_ptr->tables[i];
			string quoted = KeywordHelper::WriteOptionallyQuoted(table);
				if (detach) {
					con.Query("DROP TABLE IF EXISTS " + quoted);
					gstate_ptr->statuses[i] = "detached";
				} else {
					con.Query("CREATE OR REPLACE TABLE " + quoted + " AS SELECT * FROM aligned_table('" +
					          StringUtil::Replace(table, "'", "''") + "', root='" +
					          StringUtil::Replace(*root, "'", "''") + "')");
					gstate_ptr->statuses[i] = "attached";
				}
			}
		} catch (std::exception &ex) {
			for (auto &s : gstate_ptr->statuses) {
				if (s.empty()) {
					s = string("error: ") + ex.what();
				}
			}
		}
	});
	worker.join();
	return std::move(gstate);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<AlignedAttachGlobalState>();
	if (gstate.finished) {
		output.SetCardinality(0);
		return;
	}
	idx_t row = 0;
	for (idx_t i = 0; i < gstate.tables.size() && row < STANDARD_VECTOR_SIZE; i++) {
		output.data[0].SetValue(row, Value(gstate.tables[i]));
		output.data[1].SetValue(row, Value(gstate.statuses[i]));
		row++;
	}
	output.SetCardinality(row);
	gstate.finished = true;
}

TableFunctionSet CreateAlignedAttachFunctions() {
	TableFunctionSet set("aligned_attach");
	// all tables: aligned_attach()  |  single table: aligned_attach('name')
	TableFunction fn({LogicalType::VARCHAR}, AttachFunction, AttachBind, AttachInitGlobal, nullptr);
	fn.varargs = LogicalType::VARCHAR;
	fn.named_parameters["root"] = LogicalType::VARCHAR;
	set.AddFunction(fn);
	return set;
}

TableFunctionSet CreateAlignedDetachFunctions() {
	TableFunctionSet set("aligned_detach");
	TableFunction fn({LogicalType::VARCHAR}, AttachFunction, DetachBind, AttachInitGlobal, nullptr);
	fn.varargs = LogicalType::VARCHAR;
	fn.named_parameters["root"] = LogicalType::VARCHAR;
	set.AddFunction(fn);
	return set;
}

} // namespace duckdb



