#include "copy/aligned_copy.hpp"

#include "catalog/manifest.hpp"
#include "io/parquet_io.hpp"
#include "resolver/partition_resolver.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"

#include <atomic>
#include <thread>

#include "parquet_writer.hpp"
#include "zstd_file_system.hpp"

// parquet_writer.hpp transitively includes windows.h which redefines
// MoveFile as MoveFileA. Undo it after all includes.
#ifdef _WIN32
#undef MoveFile
#endif

namespace duckdb {

static constexpr idx_t RG_SIZE = 131072;

//===----------------------------------------------------------------------===//
// GlobalState
//===----------------------------------------------------------------------===//

AlignedCopyGlobalState::AlignedCopyGlobalState(ClientContext &ctx, FileSystem &f, const AlignedCopyBindData &bd)
    : context(ctx), fs(f), bind_data(bd) {
}

void AlignedCopyGlobalState::Flush(const string &partition_key, ColumnDataCollection &buffer) {
	idx_t rows = buffer.Count();
	if (rows == 0) {
		return;
	}

	// Find or create the PartitionWriter.
	PartitionWriter *pw;
	{
		std::lock_guard<std::mutex> glock(lock);
		auto it = writers.find(partition_key);
		if (it == writers.end()) {
			auto new_pw = make_uniq<PartitionWriter>();
			new_pw->partition_key = partition_key;
			new_pw->part_dir = bind_data.group_path + "/" + partition_key;

			// Clean old files on first write (OVERWRITE semantics).
			if (cleaned_partitions.find(partition_key) == cleaned_partitions.end()) {
				cleaned_partitions.insert(partition_key);
				if (fs.DirectoryExists(new_pw->part_dir)) {
					fs.ListFiles(new_pw->part_dir, [&](const string &name, bool is_dir) {
						if (!is_dir && StringUtil::EndsWith(name, ".parquet")) {
							fs.RemoveFile(new_pw->part_dir + "/" + name);
						}
					});
				}
			}
			fs.CreateDirectoriesRecursive(new_pw->part_dir);

			// Create first part file (temp name with rows=0).
			new_pw->part_index = 0;
			string file_path = new_pw->part_dir + "/" + FormatPartName(0, 0);
			new_pw->writer = CreateParquetWriter(context, fs, file_path,
			                                      bind_data.column_names, bind_data.sql_types);
			pw = new_pw.get();
			writers[partition_key] = std::move(new_pw);
		} else {
			pw = it->second.get();
		}
	}

	pw->received_rows += rows;

	// Flush the buffer as one Row Group.
	pw->writer->Flush(buffer, pw->transform_data);
	pw->rows_in_current_part += rows;
	pw->row_groups_in_current_part++;

	// Rotate part file at boundary.
	if (pw->row_groups_in_current_part >= bind_data.row_groups_per_file) {
		pw->writer->Finalize();
		RenamePartFile(*pw);
		pw->written_rows += pw->rows_in_current_part;
		pw->part_index++;
		string file_path = pw->part_dir + "/" + FormatPartName(pw->part_index, 0);
		pw->writer = CreateParquetWriter(context, fs, file_path,
		                                 bind_data.column_names, bind_data.sql_types);
		pw->rows_in_current_part = 0;
		pw->row_groups_in_current_part = 0;
	}
}

void AlignedCopyGlobalState::FinalizePartition(PartitionWriter &pw) {
	if (!pw.writer) {
		return;
	}
	pw.writer->Finalize();
	RenamePartFile(pw);
	pw.written_rows += pw.rows_in_current_part;
	pw.finalized = true;
	pw.writer.reset();
}

void AlignedCopyGlobalState::RenamePartFile(PartitionWriter &pw) {
	string tmp_name = FormatPartName(pw.part_index, 0);
	string final_name = FormatPartName(pw.part_index, pw.rows_in_current_part);
	string tmp_path = pw.part_dir + "/" + tmp_name;
	string final_path = pw.part_dir + "/" + final_name;
	if (tmp_path != final_path && fs.FileExists(tmp_path)) {
		fs.MoveFile(tmp_path, final_path);
	}
	if (pw.rows_in_current_part == 0 && fs.FileExists(final_path)) {
		fs.RemoveFile(final_path);
	}
}

//===----------------------------------------------------------------------===//
// LocalState
//===----------------------------------------------------------------------===//

AlignedCopyLocalState::AlignedCopyLocalState(ClientContext &context, const vector<LogicalType> &types) {
}

//===----------------------------------------------------------------------===//
// Copy options
//===----------------------------------------------------------------------===//

static void AlignedCopyOptions(ClientContext &context, CopyOptionsInput &input) {
	input.options["group"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::WRITE_ONLY);
}

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> AlignedCopyBind(ClientContext &context, CopyFunctionBindInput &input,
                                                const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto bind_data = make_uniq<AlignedCopyBindData>();

	// Parse GROUP option.
	string group_name;
	for (auto &option : input.info.options) {
		if (StringUtil::Lower(option.first) == "group") {
			if (option.second.empty()) {
				throw BinderException("aligned COPY: GROUP option requires a value");
			}
			group_name = option.second[0].ToString();
		}
	}
	if (group_name.empty()) {
		throw BinderException("aligned COPY: GROUP option is required (e.g. GROUP 'index' or GROUP 'panel/ma')");
	}

	// Resolve data root.
	const Value *root_param = nullptr;
	auto root_opt = input.info.options.find("root");
	if (root_opt != input.info.options.end() && !root_opt->second.empty()) {
		root_param = &root_opt->second[0];
	}
	bind_data->root = ResolveDataRoot(context, root_param, "aligned COPY");
	bind_data->table_name = input.info.file_path;
	bind_data->group_name = group_name;

	// Validate group name.
	if (!StringUtil::CIEquals(group_name, "index")) {
		auto slash = group_name.find('/');
		if (slash == string::npos || group_name.find('/', slash + 1) != string::npos ||
		    slash == 0 || slash + 1 >= group_name.size()) {
			throw BinderException("aligned COPY: group name '%s' must be 'index' or 'lv1/lv2'", group_name);
		}
	}

	// Discover table structure.
	TablePlan plan;
	BuildTablePlan(context, bind_data->root, bind_data->table_name, plan);
	if (plan.groups.empty()) {
		throw IOException("aligned COPY: table '%s' has no column groups", bind_data->table_name);
	}

	// Index group defines partition config.
	auto &index_group = plan.groups[0];
	if (index_group.partition_source.empty()) {
		throw IOException("aligned COPY: table '%s' index group has no partition source column",
		                  bind_data->table_name);
	}
	bind_data->partition_col_name = index_group.partition_source;
	if (!index_group.manifest.partitioning.empty()) {
		bind_data->partition_template = index_group.manifest.partitioning[0].template_str;
	} else {
		bind_data->partition_template = "month=%Y-%m";
	}

	// Find partition column position in the input.
	for (idx_t i = 0; i < names.size(); i++) {
		if (StringUtil::CIEquals(names[i], bind_data->partition_col_name)) {
			bind_data->partition_col_pos = i;
			break;
		}
	}
	if (bind_data->partition_col_pos == DConstants::INVALID_INDEX) {
		throw BinderException("aligned COPY: partition column '%s' not found in the query columns",
		                     bind_data->partition_col_name);
	}

	// Find symbol column position in the input.
	auto &symbol_col_name = index_group.symbol_column;
	for (idx_t i = 0; i < names.size(); i++) {
		if (StringUtil::CIEquals(names[i], symbol_col_name)) {
			bind_data->symbol_col_pos = i;
			break;
		}
	}

	// Find the group plan (if the group exists with parts).
	const GroupPlan *group_plan = nullptr;
	for (auto &g : plan.groups) {
		if (StringUtil::CIEquals(g.manifest.group, group_name)) {
			group_plan = &g;
			break;
		}
	}
	bind_data->group_path = bind_data->root + "/" + bind_data->table_name + "/" + group_name;

	if (group_plan && !group_plan->column_order.empty()) {
		bind_data->is_new_group = false;
		bind_data->group_columns = group_plan->column_order;
		bind_data->group_types = group_plan->schema_types;
	} else {
		bind_data->is_new_group = true;
		auto &symbol_col = index_group.symbol_column;
		auto &date_col = index_group.partition_source;
		for (idx_t i = 0; i < names.size(); i++) {
			if (StringUtil::CIEquals(names[i], symbol_col) || StringUtil::CIEquals(names[i], date_col)) {
				continue;
			}
			bind_data->group_columns.push_back(names[i]);
			bind_data->group_types.push_back(sql_types[i]);
		}
		if (bind_data->group_columns.empty()) {
			throw BinderException("aligned COPY: new group '%s' would have no columns", group_name);
		}
	}

	// Determine output columns (intersection of query cols and group cols,
	// in group schema order).
	for (idx_t gi = 0; gi < bind_data->group_columns.size(); gi++) {
		for (idx_t i = 0; i < names.size(); i++) {
			if (StringUtil::CIEquals(names[i], bind_data->group_columns[gi])) {
				bind_data->column_names.push_back(bind_data->group_columns[gi]);
				bind_data->sql_types.push_back(bind_data->group_types[gi]);
				break;
			}
		}
	}
	if (bind_data->column_names.empty()) {
		throw BinderException("aligned COPY: no columns from group '%s' found in the query", group_name);
	}

	bind_data->input_names = names;
	bind_data->input_types = sql_types;

	// Build column mapping: output col i -> input col input_col_map[i].
	bind_data->input_col_map.resize(bind_data->column_names.size(), DConstants::INVALID_INDEX);
	for (idx_t i = 0; i < bind_data->column_names.size(); i++) {
		for (idx_t j = 0; j < names.size(); j++) {
			if (StringUtil::CIEquals(names[j], bind_data->column_names[i])) {
				bind_data->input_col_map[i] = j;
				break;
			}
		}
		if (bind_data->input_col_map[i] == DConstants::INVALID_INDEX) {
			throw BinderException("aligned COPY: column '%s' not found in query", bind_data->column_names[i]);
		}
	}

	if (bind_data->partition_col_pos < bind_data->input_types.size()) {
		bind_data->is_timestamp = bind_data->input_types[bind_data->partition_col_pos].id() == LogicalTypeId::TIMESTAMP;
	}

	return std::move(bind_data);
}

static vector<unique_ptr<Expression>> AlignedCopySelect(CopyToSelectInput &input) {
	return {};
}

//===----------------------------------------------------------------------===//
// Initialize
//===----------------------------------------------------------------------===//

static unique_ptr<GlobalFunctionData> AlignedCopyInitializeGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                                   const string &file_path) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &fs = FileSystem::GetFileSystem(context);
	return make_uniq<AlignedCopyGlobalState>(context, fs, bind_data);
}

static unique_ptr<LocalFunctionData> AlignedCopyInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto result = make_uniq<AlignedCopyLocalState>(context.client, bind_data.sql_types);

	result->projected_chunk.Initialize(context.client, bind_data.sql_types);

	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Sink: single-threaded (REGULAR_COPY_TO_FILE).
//
// Input is expected to be ordered by (symbol, date). We do run detection:
// scan each input chunk row-by-row, detect partition boundaries, project
// rows to group schema, and accumulate into a per-RG buffer. When the
// buffer reaches RG_SIZE rows or the partition changes, flush it to
// ParquetWriter.
//
// This is the user's design:
//   - Single-pass scan of sorted input -> detect partition runs
//   - Project each run to group schema columns
//   - Accumulate into RG-sized buffer (ColumnDataCollection)
//   - When buffer is full or partition changes, flush to ParquetWriter
//===----------------------------------------------------------------------===//

// Project a contiguous range of rows from input to the group-schema
// projected_chunk. Copies columns per input_col_map, casting when needed.
static void ProjectRows(ClientContext &ctx, const AlignedCopyBindData &bind_data,
                        DataChunk &input, idx_t start_row, idx_t count,
                        DataChunk &output) {
	output.SetCardinality(count);
	SelectionVector sel(count);
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(i, start_row + i);
	}
	for (idx_t c = 0; c < bind_data.sql_types.size(); c++) {
		idx_t src_col = bind_data.input_col_map[c];
		auto &src_vec = input.data[src_col];
		auto &tgt_vec = output.data[c];
		if (src_vec.GetType() == tgt_vec.GetType()) {
			VectorOperations::Copy(src_vec, tgt_vec, sel, count, 0, 0);
		} else {
			// Type mismatch: copy then cast.
			Vector temp(src_vec.GetType(), count);
			VectorOperations::Copy(src_vec, temp, sel, count, 0, 0);
			VectorOperations::Cast(ctx, temp, tgt_vec, count);
		}
	}
}

static void AlignedCopySink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                             LocalFunctionData &lstate, DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();
	auto &local_state = lstate.Cast<AlignedCopyLocalState>();

	if (input.size() == 0) {
		return;
	}

	const auto count = input.size();
	auto &part_vec = input.data[bind_data.partition_col_pos];
	UnifiedVectorFormat part_fmt;
	part_vec.ToUnifiedFormat(count, part_fmt);

	// Run detection: scan rows, find partition boundaries.
	// For sorted (symbol, date) input, partition values are monotonically
	// non-decreasing, so runs are contiguous.
	idx_t run_start = 0;
	string run_partition_key;
	bool first_row = true;

	// Helper: append a run of projected rows to the per-partition buffer.
	auto append_run = [&](const string &pk, idx_t start, idx_t len) {
		if (len == 0) return;

		// Get or create the per-partition buffer.
		auto buf_it = local_state.rg_buffers.find(pk);
		if (buf_it == local_state.rg_buffers.end()) {
			auto buf = make_uniq<ColumnDataCollection>(context.client, bind_data.sql_types);
			auto app = make_uniq<ColumnDataAppendState>();
			buf->InitializeAppend(*app);
			local_state.rg_buffers[pk] = std::move(buf);
			local_state.rg_appends[pk] = std::move(app);
			buf_it = local_state.rg_buffers.find(pk);
		}

		// Project rows [start, start+len) to group schema.
		local_state.projected_chunk.Reset();
		ProjectRows(context.client, bind_data, input, start, len,
		            local_state.projected_chunk);
		buf_it->second->Append(*local_state.rg_appends[pk], local_state.projected_chunk);

		// Flush if buffer reaches RG_SIZE.
		if (buf_it->second->Count() >= RG_SIZE) {
			global_state.Flush(pk, *buf_it->second);
			// Reset buffer.
			auto new_buf = make_uniq<ColumnDataCollection>(context.client, bind_data.sql_types);
			auto new_app = make_uniq<ColumnDataAppendState>();
			new_buf->InitializeAppend(*new_app);
			local_state.rg_buffers[pk] = std::move(new_buf);
			local_state.rg_appends[pk] = std::move(new_app);
		}
	};

	for (idx_t i = 0; i < count; i++) {
		auto pi = part_fmt.sel->get_index(i);
		if (!part_fmt.validity.RowIsValid(pi)) {
			throw IOException("aligned COPY: NULL in partition column at row %llu", i);
		}
		int64_t date_val;
		if (bind_data.is_timestamp) {
			date_val = UnifiedVectorFormat::GetData<int64_t>(part_fmt)[pi];
		} else {
			date_val = static_cast<int64_t>(UnifiedVectorFormat::GetData<int32_t>(part_fmt)[pi]);
		}
		string pk;
		if (!EvaluatePartitionTemplate(bind_data.partition_template, date_val,
		                                bind_data.is_timestamp, pk)) {
			throw IOException("aligned COPY: cannot evaluate partition template '%s'",
			                  bind_data.partition_template);
		}

		if (first_row) {
			run_partition_key = pk;
			run_start = 0;
			first_row = false;
			continue;
		}

		if (pk != run_partition_key) {
			// Partition boundary: append the run [run_start, i) to the
			// current partition's buffer.
			idx_t run_len = i - run_start;
			append_run(run_partition_key, run_start, run_len);
			run_partition_key = pk;
			run_start = i;
		}
	}

	// Flush the last run.
	if (!first_row) {
		idx_t run_len = count - run_start;
		append_run(run_partition_key, run_start, run_len);
	}
}

//===----------------------------------------------------------------------===//
// Combine: flush remaining RG buffer to the last partition.
//===----------------------------------------------------------------------===//

static void AlignedCopyCombine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                                LocalFunctionData &lstate) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();
	auto &local_state = lstate.Cast<AlignedCopyLocalState>();

	// Flush all remaining per-partition buffers.
	for (auto &kv : local_state.rg_buffers) {
		if (kv.second && kv.second->Count() > 0) {
			global_state.Flush(kv.first, *kv.second);
		}
	}
	local_state.rg_buffers.clear();
	local_state.rg_appends.clear();
}

//===----------------------------------------------------------------------===//
// Finalize: finalize all partition writers in parallel.
// Uses a thread pool to parallelize the CPU-intensive Parquet encoding
// and compression across partitions.
//===----------------------------------------------------------------------===//

static void AlignedCopyFinalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();

	// Collect partition writers into a vector for parallel processing.
	vector<PartitionWriter *> work;
	for (auto &kv : global_state.writers) {
		if (kv.second->writer) {
			work.push_back(kv.second.get());
		}
	}

	// Determine thread count: min(partitions, hardware_concurrency).
	idx_t num_threads = MinValue<idx_t>(work.size(), std::thread::hardware_concurrency());
	if (num_threads == 0) {
		num_threads = 1;
	}

	// Parallel finalization.
	vector<std::thread> threads;
	std::atomic<idx_t> next_idx{0};
	std::exception_ptr first_error;

	for (idx_t t = 0; t < num_threads; t++) {
		threads.emplace_back([&]() {
			try {
				while (true) {
					idx_t idx = next_idx.fetch_add(1);
					if (idx >= work.size()) {
						break;
					}
					global_state.FinalizePartition(*work[idx]);
				}
			} catch (...) {
				if (!first_error) {
					first_error = std::current_exception();
				}
			}
		});
	}

	for (auto &t : threads) {
		t.join();
	}

	if (first_error) {
		std::rethrow_exception(first_error);
	}

	// Accounting verification.
	for (auto &kv : global_state.writers) {
		auto &pw = *kv.second;
		if (pw.received_rows != pw.written_rows) {
			throw InternalException("aligned COPY: partition '%s' accounting mismatch: "
			                        "received=%llu, written=%llu (data may be corrupted)",
			                        pw.partition_key, (unsigned long long)pw.received_rows,
			                        (unsigned long long)pw.written_rows);
		}
	}
}

//===----------------------------------------------------------------------===//

// Always use REGULAR_COPY_TO_FILE (single-threaded Sink) to preserve
// input row order. The input must be sorted by (symbol, date) — the
// primary key contract. Finalize parallelizes Parquet encoding/compression
// across partitions.
static CopyFunctionExecutionMode AlignedCopyExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

CopyFunction GetAlignedCopyFunction() {
	CopyFunction function("aligned");
	function.copy_to_bind = AlignedCopyBind;
	function.copy_options = AlignedCopyOptions;
	function.copy_to_select = AlignedCopySelect;
	function.copy_to_initialize_global = AlignedCopyInitializeGlobal;
	function.copy_to_initialize_local = AlignedCopyInitializeLocal;
	function.copy_to_sink = AlignedCopySink;
	function.copy_to_combine = AlignedCopyCombine;
	function.copy_to_finalize = AlignedCopyFinalize;
	function.execution_mode = AlignedCopyExecutionMode;
	function.extension = "parquet";
	return function;
}

} // namespace duckdb
