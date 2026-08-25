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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <thread>
#include <unordered_map>

#include "parquet_writer.hpp"
#include "zstd_file_system.hpp"

// parquet_writer.hpp transitively includes windows.h which redefines
// MoveFile as MoveFileA. Undo it after all includes.
#ifdef _WIN32
#undef MoveFile
#endif

namespace duckdb {

static constexpr idx_t RG_SIZE = 131072;

// Timing instrumentation (guarded by env var ALIGNED_COPY_TIMING).
static bool g_timing_enabled = false;
static std::chrono::steady_clock::time_point g_sink_start;
static std::atomic<idx_t> g_sink_chunks {0};
static std::atomic<idx_t> g_sink_rows {0};
static std::atomic<int64_t> g_sink_us {0};
static std::atomic<int64_t> g_combine_us {0};
static std::atomic<int64_t> g_sort_us {0};
static std::atomic<int64_t> g_merge_us {0};
static std::atomic<int64_t> g_extract_us {0};
static std::atomic<int64_t> g_perm_us {0};
static std::atomic<int64_t> g_build_us {0};
static std::atomic<int64_t> g_flush_us {0};

static inline int64_t elapsed_us(const std::chrono::steady_clock::time_point &start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

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
	{
		std::lock_guard<std::mutex> lock(partition_lock);
		auto it = partition_to_thread.find(partition_key);
		if (it != partition_to_thread.end()) {
			return it->second;
		}
	}
	idx_t tid = next_thread_id.fetch_add(1) % num_workers;
	{
		std::lock_guard<std::mutex> lock(partition_lock);
		partition_to_thread[partition_key] = tid;
	}
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

		// Buffer arrives out-of-order from parallel Sink threads.
		// Accumulate into pending; flush happens after all data arrives
		// (sentinel) with a global (symbol, date) sort per partition.
		auto &pk = job.partition_key;
		worker.pending[pk].push_back(std::move(job.buffer));
	}

	// All data has arrived. For each partition, merge all buffers into one
	// ColumnDataCollection, sort by (symbol, date), and flush in sorted order.
	for (auto &kv : worker.pending) {
		try {
			auto &pk = kv.first;
			auto pit = worker.partitions.find(pk);
			if (pit == worker.partitions.end()) {
				auto pp = make_uniq<PerPartitionState>();
				pp->partition_key = pk;
				pp->part_dir = bind_data.group_path + "/" + pk;
				InitPartition(*pp);
				pit = worker.partitions.emplace(pk, std::move(pp)).first;
			}
			SortAndFlushPartition(*pit->second, kv.second);
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
	// CreateDirectoriesRecursive can race on Windows when multiple FlushWorker
	// threads create the same year=YYYY subdirectory concurrently. Retry on
	// IOException if the directory was already created by another thread.
	for (int attempt = 0; attempt < 3; attempt++) {
		try {
			fs.CreateDirectoriesRecursive(pp.part_dir);
			break;
		} catch (const IOException &e) {
			if (fs.DirectoryExists(pp.part_dir)) {
				break;  // Another thread created it — that's fine.
			}
			if (attempt == 2) {
				throw;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	}

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

void AlignedCopyGlobalState::SortAndFlushPartition(PerPartitionState &pp,
                                                    vector<unique_ptr<ColumnDataCollection>> &buffers) {
	auto t_total = std::chrono::steady_clock::now();

	// Count total rows.
	idx_t total_rows = 0;
	for (auto &buf : buffers) {
		total_rows += buf->Count();
	}
	if (total_rows == 0) {
		return;
	}

	// Merge all buffers into one ColumnDataCollection.
	auto t_merge = std::chrono::steady_clock::now();
	vector<LogicalType> merged_types = bind_data.sql_types;
	if (bind_data.needs_hidden_sort_keys) {
		merged_types.push_back(bind_data.hidden_symbol_type);
		merged_types.push_back(bind_data.hidden_date_type);
	}
	auto merged = make_uniq<ColumnDataCollection>(context, merged_types);
	ColumnDataAppendState append_state;
	merged->InitializeAppend(append_state);
	vector<column_t> all_cols;
	for (idx_t c = 0; c < merged_types.size(); c++) {
		all_cols.push_back(c);
	}
	DataChunk merge_chunk;
	merge_chunk.Initialize(context, merged_types);
	for (auto &buf : buffers) {
		if (buf && buf->Count() > 0) {
			ColumnDataScanState scan_state;
			buf->InitializeScan(scan_state, all_cols);
			buf->InitializeScanChunk(scan_state, merge_chunk);
			while (buf->Scan(scan_state, merge_chunk)) {
				merged->Append(append_state, merge_chunk);
			}
		}
	}

	int64_t merge_us = elapsed_us(t_merge);
	if (g_timing_enabled) g_merge_us.fetch_add(merge_us);

	// Find symbol and date column indices in the merged CDC.
	// For groups with hidden sort keys, they are the last 2 columns.
	idx_t symbol_col = DConstants::INVALID_INDEX;
	idx_t date_col = DConstants::INVALID_INDEX;
	if (bind_data.needs_hidden_sort_keys) {
		symbol_col = bind_data.sql_types.size();
		date_col = bind_data.sql_types.size() + 1;
	} else {
		for (idx_t c = 0; c < bind_data.column_names.size(); c++) {
			if (StringUtil::CIEquals(bind_data.column_names[c], "symbol")) {
				symbol_col = c;
			} else if (StringUtil::CIEquals(bind_data.column_names[c], bind_data.partition_col_name)) {
				date_col = c;
			}
		}
	}

	if (symbol_col == DConstants::INVALID_INDEX || date_col == DConstants::INVALID_INDEX) {
		// Can't sort — flush as-is (already in arrival order).
		FlushToPartition(pp, *merged);
		return;
	}

	auto t_extract = std::chrono::steady_clock::now();

	// Extract symbol and date values for sorting.
	// Build a small string→int32 dictionary for fast integer comparisons
	// during std::stable_sort. unordered_map for O(1) lookup, then remap
	// indices to lexicographic order. stable_sort is ~O(n) on near-sorted
	// data (input was ORDER BY).
	vector<int32_t> symbol_idx(total_rows);
	vector<int64_t> date_values(total_rows);
	unordered_map<string, int32_t> sym_lookup;
	sym_lookup.reserve(2048);
	int32_t next_idx = 0;

	// Scan ALL columns (not a column subset) to avoid InitializeScan
	// column_id issues, then index into chunk.data[symbol_col]/[date_col].
	ColumnDataScanState scan_state;
	merged->InitializeScan(scan_state, all_cols);
	DataChunk chunk;
	merged->InitializeScanChunk(scan_state, chunk);

	idx_t row_offset = 0;
	while (merged->Scan(scan_state, chunk)) {
		auto &sym_vec = chunk.data[symbol_col];
		UnifiedVectorFormat sym_fmt;
		sym_vec.ToUnifiedFormat(chunk.size(), sym_fmt);
		auto sym_data = UnifiedVectorFormat::GetData<string_t>(sym_fmt);

		auto &date_vec = chunk.data[date_col];
		UnifiedVectorFormat date_fmt;
		date_vec.ToUnifiedFormat(chunk.size(), date_fmt);

		// Determine date column physical type from the actual vector,
		// not from bind_data.is_timestamp (which reflects the *input* type,
		// not the *output* type after cast).
		bool date_is_ts = (date_vec.GetType().id() == LogicalTypeId::TIMESTAMP);

		for (idx_t i = 0; i < chunk.size(); i++) {
			auto si = sym_fmt.sel->get_index(i);
			auto di = date_fmt.sel->get_index(i);
			auto sym_str = sym_data[si].GetString();
			auto dit = sym_lookup.find(sym_str);
			if (dit == sym_lookup.end()) {
				sym_lookup[sym_str] = next_idx;
				symbol_idx[row_offset + i] = next_idx;
				next_idx++;
			} else {
				symbol_idx[row_offset + i] = dit->second;
			}
			if (date_is_ts) {
				auto dd = UnifiedVectorFormat::GetData<int64_t>(date_fmt);
				date_values[row_offset + i] = dd[di];
			} else {
				auto dd = UnifiedVectorFormat::GetData<int32_t>(date_fmt);
				date_values[row_offset + i] = static_cast<int64_t>(dd[di]);
			}
		}
		row_offset += chunk.size();
	}

	// Reassign dictionary indices in lexicographic order so that
	// integer comparison of symbol_idx == string comparison of symbols.
	vector<string> sorted_syms;
	sorted_syms.reserve(sym_lookup.size());
	for (auto &kv : sym_lookup) {
		sorted_syms.push_back(kv.first);
	}
	std::sort(sorted_syms.begin(), sorted_syms.end());
	vector<int32_t> remap(next_idx);
	for (idx_t i = 0; i < sorted_syms.size(); i++) {
		remap[sym_lookup[sorted_syms[i]]] = static_cast<int32_t>(i);
	}
	for (idx_t i = 0; i < total_rows; i++) {
		symbol_idx[i] = remap[symbol_idx[i]];
	}

	int64_t extract_u = elapsed_us(t_extract);
	if (g_timing_enabled) g_extract_us.fetch_add(extract_u);

	// Create permutation sorted by (symbol, date).
	auto t_perm = std::chrono::steady_clock::now();
	vector<idx_t> perm(total_rows);
	for (idx_t i = 0; i < total_rows; i++) {
		perm[i] = i;
	}
	std::stable_sort(perm.begin(), perm.end(), [&](idx_t a, idx_t b) {
		if (symbol_idx[a] != symbol_idx[b]) return symbol_idx[a] < symbol_idx[b];
		return date_values[a] < date_values[b];
	});

	int64_t perm_u = elapsed_us(t_perm);
	if (g_timing_enabled) g_perm_us.fetch_add(perm_u);

	// Build sorted collection by re-appending rows in sorted order.
	// Scan merged into cached chunks, then output rows in perm order
	// (perm[i] = source row index of the i-th output row).
	// The sorted CDC contains only group schema columns — hidden sort keys
	// are stripped (copy loop only copies sql_types.size() columns).
	auto t_build = std::chrono::steady_clock::now();
	auto sorted = make_uniq<ColumnDataCollection>(context, bind_data.sql_types);
	ColumnDataAppendState sorted_append;
	sorted->InitializeAppend(sorted_append);

	// Cache all source chunks (with hidden sort keys for sorting reference).
	ColumnDataScanState full_scan;
	merged->InitializeScan(full_scan, all_cols);
	DataChunk full_chunk;
	merged->InitializeScanChunk(full_scan, full_chunk);

	vector<unique_ptr<DataChunk>> source_chunks;
	idx_t total_cached = 0;
	while (merged->Scan(full_scan, full_chunk)) {
		auto cached = make_uniq<DataChunk>();
		cached->Initialize(context, merged_types);
		cached->Reference(full_chunk);
		cached->SetCardinality(full_chunk.size());
		total_cached += full_chunk.size();
		source_chunks.push_back(std::move(cached));
	}

	// Now output rows in perm order. For each batch of STANDARD_VECTOR_SIZE
	// consecutive perm entries, determine which source chunk each row is in,
	// build a SelectionVector, and copy.
	DataChunk out_chunk;
	out_chunk.Initialize(context, bind_data.sql_types);

	// Precompute chunk boundaries.
	vector<idx_t> chunk_starts(source_chunks.size() + 1);
	chunk_starts[0] = 0;
	for (idx_t i = 0; i < source_chunks.size(); i++) {
		chunk_starts[i + 1] = chunk_starts[i] + source_chunks[i]->size();
	}

	idx_t perm_pos = 0;
	while (perm_pos < total_rows) {
		// Group consecutive perm entries that fall in the same source chunk.
		idx_t current_row = perm[perm_pos];
		// Find which chunk this row belongs to.
		idx_t chunk_idx = 0;
		while (chunk_idx + 1 < chunk_starts.size() && chunk_starts[chunk_idx + 1] <= current_row) {
			chunk_idx++;
		}
		auto &src = *source_chunks[chunk_idx];
		idx_t chunk_start = chunk_starts[chunk_idx];

		SelectionVector sel(STANDARD_VECTOR_SIZE);
		idx_t sel_count = 0;
		while (perm_pos < total_rows && sel_count < STANDARD_VECTOR_SIZE) {
			idx_t row = perm[perm_pos];
			// Check if this row is in the same chunk.
			if (row >= chunk_starts[chunk_idx] && row < chunk_starts[chunk_idx + 1]) {
				sel.set_index(sel_count, row - chunk_start);
				sel_count++;
				perm_pos++;
			} else {
				break;
			}
		}
		if (sel_count > 0) {
			out_chunk.Reset();
			for (idx_t c = 0; c < bind_data.sql_types.size(); c++) {
				VectorOperations::Copy(src.data[c], out_chunk.data[c], sel,
				                       sel_count, 0, 0);
			}
			out_chunk.SetCardinality(sel_count);
			sorted->Append(sorted_append, out_chunk);
		}
	}

	// Flush the sorted collection in RG-sized chunks.
	auto t_flush = std::chrono::steady_clock::now();
	FlushToPartition(pp, *sorted);

	if (g_timing_enabled) {
		g_build_us.fetch_add(elapsed_us(t_build));
		g_flush_us.fetch_add(elapsed_us(t_flush));
		g_sort_us.fetch_add(elapsed_us(t_total));
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

	// Check if this group's output schema includes symbol and date columns.
	// If not (non-index group), we need hidden sort-key columns appended to
	// the buffer CDC so SortAndFlushPartition can sort by (symbol, date).
	bind_data->needs_hidden_sort_keys = true;
	for (idx_t c = 0; c < bind_data->column_names.size(); c++) {
		if (StringUtil::CIEquals(bind_data->column_names[c], "symbol") ||
		    StringUtil::CIEquals(bind_data->column_names[c], bind_data->partition_col_name)) {
			bind_data->needs_hidden_sort_keys = false;
		}
	}
	if (bind_data->needs_hidden_sort_keys) {
		bind_data->hidden_symbol_input_col = bind_data->symbol_col_pos;
		bind_data->hidden_date_input_col = bind_data->partition_col_pos;
		if (bind_data->hidden_symbol_input_col < bind_data->input_types.size() &&
		    bind_data->hidden_date_input_col < bind_data->input_types.size()) {
			bind_data->hidden_symbol_type = bind_data->input_types[bind_data->hidden_symbol_input_col];
			bind_data->hidden_date_type = bind_data->input_types[bind_data->hidden_date_input_col];
		} else {
			bind_data->needs_hidden_sort_keys = false;
		}
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
	// Pre-create the group directory (single-threaded, before parallel Sink
	// starts). This avoids a TOCTOU race in Windows CreateDirectory when
	// multiple FlushWorker threads try to create the same parent directory
	// concurrently for a new group.
	fs.CreateDirectoriesRecursive(bind_data.group_path);
	return make_uniq<AlignedCopyGlobalState>(context, fs, bind_data);
}

static unique_ptr<LocalFunctionData> AlignedCopyInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	// The projected chunk includes group schema columns + hidden sort keys
	// (symbol, date) for non-index groups.
	vector<LogicalType> chunk_types = bind_data.sql_types;
	if (bind_data.needs_hidden_sort_keys) {
		chunk_types.push_back(bind_data.hidden_symbol_type);
		chunk_types.push_back(bind_data.hidden_date_type);
	}
	auto result = make_uniq<AlignedCopyLocalState>(context.client, chunk_types);
	result->projected_chunk.Initialize(context.client, chunk_types);
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Sink: parallel (PARALLEL_COPY_TO_FILE).
//
// Input is expected to be ordered by (symbol, date). We do run detection:
// scan each input chunk row-by-row, detect partition boundaries, project
// rows to group schema, and accumulate into a per-RG buffer. When the
// buffer reaches RG_SIZE rows, push it to a background FlushWorker thread
// for Parquet encoding + write.
//
// The source reader (parquet scan) runs in parallel (8 threads). Multiple
// Sink threads call this function concurrently, each with its own LocalState.
// Per-thread per-partition buffers avoid contention. The FlushWorker thread
// pool handles parallel Parquet encoding.
//===----------------------------------------------------------------------===//

// Project a contiguous range of rows from input to the group-schema
// projected_chunk. Copies columns per input_col_map, casting when needed.
// If needs_hidden_sort_keys, also appends symbol + date columns for sorting.
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
	// Append hidden sort-key columns (symbol + date) for non-index groups.
	if (bind_data.needs_hidden_sort_keys) {
		idx_t base = bind_data.sql_types.size();
		auto &sym_src = input.data[bind_data.hidden_symbol_input_col];
		auto &sym_tgt = output.data[base];
		VectorOperations::Copy(sym_src, sym_tgt, sel, count, 0, 0);
		auto &date_src = input.data[bind_data.hidden_date_input_col];
		auto &date_tgt = output.data[base + 1];
		if (date_src.GetType() == date_tgt.GetType()) {
			VectorOperations::Copy(date_src, date_tgt, sel, count, 0, 0);
		} else {
			Vector temp(date_src.GetType(), count);
			VectorOperations::Copy(date_src, temp, sel, count, 0, 0);
			VectorOperations::Cast(ctx, temp, date_tgt, count);
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

	if (!g_timing_enabled) {
		const char *env = std::getenv("ALIGNED_COPY_TIMING");
		g_timing_enabled = (env != nullptr);
		if (g_timing_enabled) g_sink_start = std::chrono::steady_clock::now();
	}

	auto t0 = std::chrono::steady_clock::now();

	// In PARALLEL_COPY_TO_FILE mode, multiple threads call Sink concurrently.
	// The source reader (parquet scan) runs in parallel, giving ~3.7x speedup.
	// Buffers arrive out-of-order; the FlushWorker accumulates all buffers per
	// partition and sorts by (symbol, date) before flushing — zero out-of-order.

	const auto count = input.size();
	auto &part_vec = input.data[bind_data.partition_col_pos];
	UnifiedVectorFormat part_fmt;
	part_vec.ToUnifiedFormat(count, part_fmt);

	// Extract all date values into a contiguous array for fast scanning.
	auto *raw_data = bind_data.is_timestamp
		? UnifiedVectorFormat::GetData<int64_t>(part_fmt)
		: nullptr;
	auto *raw_data32 = !bind_data.is_timestamp
		? UnifiedVectorFormat::GetData<int32_t>(part_fmt)
		: nullptr;

	// Helper: append a run of projected rows to the per-thread
	// per-partition buffer, and push to background worker if buffer is full.
	auto append_run = [&](const string &pk, idx_t start, idx_t len) {
		if (len == 0) return;

		auto buf_it = local_state.rg_buffers.find(pk);
		if (buf_it == local_state.rg_buffers.end()) {
			vector<LogicalType> buf_types = bind_data.sql_types;
			if (bind_data.needs_hidden_sort_keys) {
				buf_types.push_back(bind_data.hidden_symbol_type);
				buf_types.push_back(bind_data.hidden_date_type);
			}
			auto buf = make_uniq<ColumnDataCollection>(context.client, buf_types);
			auto app = make_uniq<ColumnDataAppendState>();
			buf->InitializeAppend(*app);
			local_state.rg_buffers[pk] = std::move(buf);
			local_state.rg_appends[pk] = std::move(app);
			buf_it = local_state.rg_buffers.find(pk);
		}

		// Project input columns to group schema.
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

	if (g_timing_enabled) {
		g_sink_chunks.fetch_add(1);
		g_sink_rows.fetch_add(count);
		g_sink_us.fetch_add(elapsed_us(t0));
	}
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

	auto t0 = std::chrono::steady_clock::now();

	// Push all remaining per-thread buffers to their assigned workers.
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

	if (g_timing_enabled) {
		g_combine_us.fetch_add(elapsed_us(t0));
	}
}

//===----------------------------------------------------------------------===//
// Finalize: wait for all background workers to finish, check errors,
// verify accounting.
//===----------------------------------------------------------------------===//

static void AlignedCopyFinalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();

	auto t0 = std::chrono::steady_clock::now();

	// Push sentinels to stop all worker loops.
	global_state.PushSentinels();

	// Wait for all threads to finish.
	global_state.JoinThreads();

	auto t_join_end = std::chrono::steady_clock::now();
	int64_t join_us = std::chrono::duration_cast<std::chrono::microseconds>(t_join_end - t0).count();

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

	int64_t finalize_us = std::chrono::duration_cast<std::chrono::microseconds>(
		std::chrono::steady_clock::now() - t0).count();

	if (g_timing_enabled) {
		int64_t total_us = std::chrono::duration_cast<std::chrono::microseconds>(
			std::chrono::steady_clock::now() - g_sink_start).count();
		double total_s = total_us / 1e6;
		double finalize_s = finalize_us / 1e6;
		double join_s = join_us / 1e6;
		double sink_s = g_sink_us.load() / 1e6;
		double combine_s = g_combine_us.load() / 1e6;
		double sort_s = g_sort_us.load() / 1e6;
		double merge_s = g_merge_us.load() / 1e6;
		double extract_s = g_extract_us.load() / 1e6;
		double perm_s = g_perm_us.load() / 1e6;
		double build_s = g_build_us.load() / 1e6;
		double flush_s = g_flush_us.load() / 1e6;
		fprintf(stderr,
		        "[ALIGNED_COPY_TIMING] total=%.3fs | sink=%.3fs (chunks=%zu, rows=%zu) | "
		        "combine=%.3fs | finalize=%.3fs (join_wait=%.3fs) | "
		        "sort=%.3fs [merge=%.3fs extract=%.3fs perm=%.3fs build=%.3fs flush=%.3fs]\n",
		        total_s,
		        sink_s, (size_t)g_sink_chunks.load(), (size_t)g_sink_rows.load(),
		        combine_s,
		        finalize_s, join_s,
		        sort_s,
		        merge_s, extract_s, perm_s, build_s, flush_s);
	}
}

//===----------------------------------------------------------------------===//

// Use PARALLEL_COPY_TO_FILE to allow the source reader (parquet scan) to
// run in parallel (8 threads). Multiple Sink threads process chunks
// concurrently with per-thread LocalState. The FlushWorker thread pool
// handles parallel Parquet encoding. Buffers arrive out-of-order; each
// FlushWorker accumulates all buffers for its partitions, then sorts by
// (symbol, date) before flushing — guaranteeing correct in-partition order
// with zero out-of-order rows.
static CopyFunctionExecutionMode AlignedCopyExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
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
