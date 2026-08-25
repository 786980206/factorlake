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
#include <limits>
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
	// Create the thread pool: min(8, hardware_concurrency) workers.
	num_workers = std::thread::hardware_concurrency();
	if (num_workers == 0) {
		num_workers = 1;
	}
	if (num_workers > 8) {
		num_workers = 8;  // Cap: disk I/O becomes bottleneck beyond this.
	}
	for (idx_t i = 0; i < num_workers; i++) {
		workers.push_back(make_uniq<FlushWorker>());
	}
	for (idx_t i = 0; i < num_workers; i++) {
		threads.emplace_back([this, i]() { WorkerLoop(*workers[i]); });
	}
}

AlignedCopyGlobalState::~AlignedCopyGlobalState() {
	PushSentinels();
	JoinThreads();
}

idx_t AlignedCopyGlobalState::AssignThread(const string &partition_key) {
	auto it = partition_to_thread.find(partition_key);
	if (it != partition_to_thread.end()) {
		return it->second;
	}
	idx_t tid = next_thread_id.fetch_add(1) % num_workers;
	partition_to_thread[partition_key] = tid;
	return tid;
}

void AlignedCopyGlobalState::PushJob(FlushJob &&job, idx_t worker_id) {
	{
		std::lock_guard<std::mutex> lock(workers[worker_id]->queue_lock);
		workers[worker_id]->queue.push_back(std::move(job));
	}
	workers[worker_id]->queue_cv.notify_one();
}

void AlignedCopyGlobalState::PushSentinels() {
	for (idx_t i = 0; i < num_workers; i++) {
		FlushJob sentinel;
		sentinel.is_sentinel = true;
		{
			std::lock_guard<std::mutex> lock(workers[i]->queue_lock);
			workers[i]->queue.push_back(std::move(sentinel));
		}
		workers[i]->queue_cv.notify_one();
	}
}

void AlignedCopyGlobalState::JoinThreads() {
	for (auto &t : threads) {
		if (t.joinable()) {
			t.join();
		}
	}
	threads.clear();
}

void AlignedCopyGlobalState::WorkerLoop(FlushWorker &worker) {
	while (true) {
		FlushJob job;
		{
			std::unique_lock<std::mutex> lock(worker.queue_lock);
			worker.queue_cv.wait(lock, [&worker] { return !worker.queue.empty(); });
			job = std::move(worker.queue.front());
			worker.queue.pop_front();
		}
		if (job.is_sentinel) {
			break;
		}
		try {
			// Find or create the PerPartitionState (thread-local, no lock).
			auto it = worker.partitions.find(job.partition_key);
			if (it == worker.partitions.end()) {
				auto pp = make_uniq<PerPartitionState>();
				pp->partition_key = job.partition_key;
				pp->part_dir = bind_data.group_path + "/" + job.partition_key;
				InitPartition(*pp);
				it = worker.partitions.emplace(job.partition_key, std::move(pp)).first;
			}
			FlushToPartition(*it->second, *job.buffer);
		} catch (...) {
			if (!worker.error) {
				worker.error = std::current_exception();
			}
		}
	}

	// Finalize all partitions owned by this worker.
	for (auto &kv : worker.partitions) {
		try {
			FinalizePartition(*kv.second);
		} catch (...) {
			if (!worker.error) {
				worker.error = std::current_exception();
			}
		}
	}
}

void AlignedCopyGlobalState::InitPartition(PerPartitionState &pp) {
	// Clean old files (OVERWRITE semantics).
	if (fs.DirectoryExists(pp.part_dir)) {
		fs.ListFiles(pp.part_dir, [&](const string &name, bool is_dir) {
			if (!is_dir && StringUtil::EndsWith(name, ".parquet")) {
				fs.RemoveFile(pp.part_dir + "/" + name);
			}
		});
	}
	fs.CreateDirectoriesRecursive(pp.part_dir);

	// Create first part file.
	pp.part_index = 0;
	string file_path = pp.part_dir + "/" + FormatPartName(0, 0);
	pp.writer = CreateParquetWriter(context, fs, file_path,
	                                bind_data.column_names, bind_data.sql_types);
}

void AlignedCopyGlobalState::FlushToPartition(PerPartitionState &pp, ColumnDataCollection &buffer) {
	idx_t rows = buffer.Count();
	if (rows == 0) {
		return;
	}
	pp.received_rows += rows;
	pp.writer->Flush(buffer, pp.transform_data);
	pp.rows_in_current_part += rows;
	pp.row_groups_in_current_part++;

	if (pp.row_groups_in_current_part >= bind_data.row_groups_per_file) {
		RotatePartition(pp);
	}
}

void AlignedCopyGlobalState::RotatePartition(PerPartitionState &pp) {
	pp.writer->Finalize();
	RenamePartFile(pp);
	pp.written_rows += pp.rows_in_current_part;
	pp.part_index++;
	string file_path = pp.part_dir + "/" + FormatPartName(pp.part_index, 0);
	pp.writer = CreateParquetWriter(context, fs, file_path,
	                                bind_data.column_names, bind_data.sql_types);
	pp.rows_in_current_part = 0;
	pp.row_groups_in_current_part = 0;
}

void AlignedCopyGlobalState::FinalizePartition(PerPartitionState &pp) {
	if (!pp.writer) {
		return;
	}
	pp.writer->Finalize();
	RenamePartFile(pp);
	pp.written_rows += pp.rows_in_current_part;
	pp.finalized = true;
	pp.writer.reset();
}

void AlignedCopyGlobalState::RenamePartFile(PerPartitionState &pp) {
	string tmp_name = FormatPartName(pp.part_index, 0);
	string final_name = FormatPartName(pp.part_index, pp.rows_in_current_part);
	string tmp_path = pp.part_dir + "/" + tmp_name;
	string final_path = pp.part_dir + "/" + final_name;
	if (tmp_path != final_path && fs.FileExists(tmp_path)) {
		fs.MoveFile(tmp_path, final_path);
	}
	if (pp.rows_in_current_part == 0 && fs.FileExists(final_path)) {
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
// buffer reaches RG_SIZE rows, push it to a background FlushWorker thread
// for Parquet encoding + write.
//
// Key optimization: partition key cache.  Since input is sorted by date,
// consecutive rows almost always have the same partition key.  We cache
// the last (date_val, pk) pair and skip EvaluatePartitionTemplate on
// cache hit (just an integer compare).
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

	// Extract all date values into a contiguous array for fast scanning.
	// This avoids per-row ToUnifiedFormat lookups.
	auto *raw_data = bind_data.is_timestamp
		? UnifiedVectorFormat::GetData<int64_t>(part_fmt)
		: nullptr;
	auto *raw_data32 = !bind_data.is_timestamp
		? UnifiedVectorFormat::GetData<int32_t>(part_fmt)
		: nullptr;

	// Helper: append a run of projected rows to the per-partition buffer,
	// and push to background worker if buffer is full.
	auto append_run = [&](const string &pk, idx_t start, idx_t len) {
		if (len == 0) return;

		auto buf_it = local_state.rg_buffers.find(pk);
		if (buf_it == local_state.rg_buffers.end()) {
			auto buf = make_uniq<ColumnDataCollection>(context.client, bind_data.sql_types);
			auto app = make_uniq<ColumnDataAppendState>();
			buf->InitializeAppend(*app);
			local_state.rg_buffers[pk] = std::move(buf);
			local_state.rg_appends[pk] = std::move(app);
			buf_it = local_state.rg_buffers.find(pk);
		}

		local_state.projected_chunk.Reset();
		ProjectRows(context.client, bind_data, input, start, len,
		            local_state.projected_chunk);
		buf_it->second->Append(*local_state.rg_appends[pk], local_state.projected_chunk);

		if (buf_it->second->Count() >= RG_SIZE) {
			// Push to background worker.
			idx_t tid = global_state.AssignThread(pk);
			FlushJob job;
			job.partition_key = pk;
			job.buffer = std::move(buf_it->second);
			local_state.rg_buffers.erase(pk);
			local_state.rg_appends.erase(pk);
			global_state.PushJob(std::move(job), tid);
		}
	};

	// Helper: evaluate partition key for a date value, using the cache.
	auto eval_pk = [&](int64_t date_val) -> const string& {
		if (date_val == local_state.cached_date_val) {
			return local_state.cached_pk;
		}
		string pk;
		if (!EvaluatePartitionTemplate(bind_data.partition_template, date_val,
		                                bind_data.is_timestamp, pk)) {
			throw IOException("aligned COPY: cannot evaluate partition template '%s'",
			                  bind_data.partition_template);
		}
		local_state.cached_date_val = date_val;
		local_state.cached_pk = pk;
		return local_state.cached_pk;
	};

	// Get the partition key for the first row of this chunk.
	auto first_pi = part_fmt.sel->get_index(0);
	if (!part_fmt.validity.RowIsValid(first_pi)) {
		throw IOException("aligned COPY: NULL in partition column at row 0");
	}
	int64_t first_date_val = bind_data.is_timestamp
		? raw_data[first_pi]
		: static_cast<int64_t>(raw_data32[first_pi]);
	const string &first_pk = eval_pk(first_date_val);

	// Fast path: check if the entire chunk belongs to one partition.
	// Compare the last row's date value — if same partition key as first,
	// we can skip the per-row scan entirely.
	auto last_pi = part_fmt.sel->get_index(count - 1);
	if (!part_fmt.validity.RowIsValid(last_pi)) {
		throw IOException("aligned COPY: NULL in partition column at row %llu", count - 1);
	}
	int64_t last_date_val = bind_data.is_timestamp
		? raw_data[last_pi]
		: static_cast<int64_t>(raw_data32[last_pi]);

	if (first_date_val == last_date_val) {
		// Entire chunk is one partition — fast path, no per-row scan.
		// But we still need to check if this chunk's partition matches
		// the previous chunk's partition (run_partition_key).
		// Since we don't track cross-chunk state, just append directly.
		append_run(first_pk, 0, count);
		return;
	}

	// Slow path: chunk crosses a partition boundary. Scan to find it.
	// Since input is sorted by (symbol, date), there's at most a few
	// boundary points per chunk (typically just 1-2 for date=%Y-%m-%d).
	idx_t run_start = 0;
	string run_pk_str = first_pk;  // Own a copy, don't use dangling ref.

	for (idx_t i = 1; i < count; i++) {
		auto pi = part_fmt.sel->get_index(i);
		if (!part_fmt.validity.RowIsValid(pi)) {
			throw IOException("aligned COPY: NULL in partition column at row %llu", i);
		}
		int64_t date_val = bind_data.is_timestamp
			? raw_data[pi]
			: static_cast<int64_t>(raw_data32[pi]);

		if (date_val != local_state.cached_date_val) {
			// Potential partition boundary: date changed.
			const string &pk = eval_pk(date_val);
			if (pk != run_pk_str) {
				// Actual boundary: flush the run [run_start, i).
				append_run(run_pk_str, run_start, i - run_start);
				run_pk_str = pk;
				run_start = i;
			}
		}
	}

	// Flush the last run.
	append_run(run_pk_str, run_start, count - run_start);
}

//===----------------------------------------------------------------------===//
// Combine: push all remaining per-partition buffers to background workers,
// then push sentinels.
//===----------------------------------------------------------------------===//

static void AlignedCopyCombine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                                LocalFunctionData &lstate) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();
	auto &local_state = lstate.Cast<AlignedCopyLocalState>();

	// Push all remaining buffers to their assigned workers.
	for (auto &kv : local_state.rg_buffers) {
		if (kv.second && kv.second->Count() > 0) {
			idx_t tid = global_state.AssignThread(kv.first);
			FlushJob job;
			job.partition_key = kv.first;
			job.buffer = std::move(kv.second);
			global_state.PushJob(std::move(job), tid);
		}
	}
	local_state.rg_buffers.clear();
	local_state.rg_appends.clear();
}

//===----------------------------------------------------------------------===//
// Finalize: wait for all background workers to finish, check errors,
// verify accounting.
//===----------------------------------------------------------------------===//

static void AlignedCopyFinalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();

	// Push sentinels to stop all worker loops.
	global_state.PushSentinels();

	// Wait for all threads to finish.
	global_state.JoinThreads();

	// Check for errors from background threads.
	for (auto &worker : global_state.workers) {
		if (worker->error) {
			std::rethrow_exception(worker->error);
		}
	}

	// Accounting verification across all workers' partitions.
	for (auto &worker : global_state.workers) {
		for (auto &kv : worker->partitions) {
			auto &pp = *kv.second;
			if (pp.received_rows != pp.written_rows) {
				throw InternalException("aligned COPY: partition '%s' accounting mismatch: "
				                        "received=%llu, written=%llu (data may be corrupted)",
				                        pp.partition_key, (unsigned long long)pp.received_rows,
				                        (unsigned long long)pp.written_rows);
			}
		}
	}
}

//===----------------------------------------------------------------------===//

// Always use REGULAR_COPY_TO_FILE (single-threaded Sink) to preserve
// input row order. The input must be sorted by (symbol, date) — the
// primary key contract. Parquet encoding/compression is parallelized
// via a background FlushWorker thread pool in GlobalState.
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
