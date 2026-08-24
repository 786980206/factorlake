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

#include "parquet_writer.hpp"
#include "zstd_file_system.hpp"

// parquet_writer.hpp transitively includes windows.h which redefines
// MoveFile as MoveFileA. Undo it after all includes.
#ifdef _WIN32
#undef MoveFile
#endif

namespace duckdb {

//===----------------------------------------------------------------------===//
// LocalState
//===----------------------------------------------------------------------===//

AlignedCopyLocalState::AlignedCopyLocalState(ClientContext &context, const vector<LogicalType> &types)
    : types(types) {
}

AlignedCopyLocalState::PartitionBuffer *AlignedCopyLocalState::GetBuffer(const string &partition_key,
                                                                          ClientContext &context) {
	auto it = buffers.find(partition_key);
	if (it != buffers.end()) {
		return it->second.get();
	}
	auto pb = make_uniq<PartitionBuffer>(PartitionBuffer{
	    ColumnDataCollection(context, types), ColumnDataAppendState()});
	pb->collection.SetPartitionIndex(0);
	pb->collection.InitializeAppend(pb->append_state);
	auto *ptr = pb.get();
	buffers[partition_key] = std::move(pb);
	return ptr;
}

//===----------------------------------------------------------------------===//
// GlobalState
//===----------------------------------------------------------------------===//

AlignedCopyGlobalState::AlignedCopyGlobalState(ClientContext &ctx, FileSystem &f, const AlignedCopyBindData &bd)
    : context(ctx), fs(f), bind_data(bd) {
}

PartitionWriter *AlignedCopyGlobalState::Flush(const string &partition_key, ColumnDataCollection &buffer) {
	idx_t rows = buffer.Count();
	if (rows == 0) {
		return nullptr;
	}

	// Phase 1: Get or create the partition writer (needs global lock for map access).
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
	// Global lock released — other threads can create/lookup different partitions.

	// Phase 2: Flush under per-partition lock (different partitions flush in parallel).
	std::lock_guard<std::mutex> plock(pw->lock);
	pw->writer->Flush(buffer, pw->transform_data);
	pw->rows_in_current_part += rows;
	pw->row_groups_in_current_part++;

	// Rotate part file at the boundary.
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

	return pw;
}

void AlignedCopyGlobalState::FinalizePartition(PartitionWriter &pw) {
	if (!pw.writer) {
		return;
	}
	// Lock per-partition to serialize against any late Flush from Combine.
	std::lock_guard<std::mutex> plock(pw.lock);
	pw.writer->Finalize();
	RenamePartFile(pw);
	pw.written_rows += pw.rows_in_current_part;
	pw.finalized = true;
	pw.writer.reset();

	// Verify accounting: received == written.
	// (received_rows is updated in Sink; we check it here.)
}

void AlignedCopyGlobalState::RenamePartFile(PartitionWriter &pw) {
	string tmp_name = FormatPartName(pw.part_index, 0);
	string final_name = FormatPartName(pw.part_index, pw.rows_in_current_part);
	string tmp_path = pw.part_dir + "/" + tmp_name;
	string final_path = pw.part_dir + "/" + final_name;
	if (tmp_path != final_path && fs.FileExists(tmp_path)) {
		fs.MoveFile(tmp_path, final_path);
	}
	// Remove 0-row files (empty parts).
	if (pw.rows_in_current_part == 0 && fs.FileExists(final_path)) {
		fs.RemoveFile(final_path);
	}
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
		// New group: infer columns from the query (exclude key columns).
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

	// Build column mapping: output col i → input col input_col_map[i].
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
	return make_uniq<AlignedCopyLocalState>(context.client, bind_data.sql_types);
}

//===----------------------------------------------------------------------===//
// Sink: partition rows into per-partition local buffers. Does NOT touch
// any ParquetWriter. Only appends to local ColumnDataCollections.
//===----------------------------------------------------------------------===//

static void AlignedCopySink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                             LocalFunctionData &lstate, DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();
	auto &local_state = lstate.Cast<AlignedCopyLocalState>();

	if (input.size() == 0) {
		return;
	}

	local_state.received_rows += input.size();

	// Extract partition key for each row.
	idx_t part_col = bind_data.partition_col_pos;
	auto &part_vec = input.data[part_col];
	UnifiedVectorFormat part_fmt;
	part_vec.ToUnifiedFormat(input.size(), part_fmt);
	bool is_timestamp = part_vec.GetType().id() == LogicalTypeId::TIMESTAMP;

	// Group row indices by partition key using run-length detection.
	// Since input is typically sorted by (symbol, date), consecutive rows
	// usually share the same partition key. We detect partition boundaries
	// and batch-copy contiguous ranges, avoiding per-row map overhead.
	// We still use the partition_key_cache to avoid re-evaluating the
	// partition template for repeated date values.
	struct PartitionRun {
		idx_t start;
		idx_t count;
		string key;
	};
	vector<PartitionRun> runs;
	string current_key;
	idx_t run_start = 0;

	for (idx_t i = 0; i < input.size(); i++) {
		auto si = part_fmt.sel->get_index(i);
		if (!part_fmt.validity.RowIsValid(si)) {
			throw IOException("aligned COPY: NULL in partition column '%s' at row %llu",
			                  bind_data.partition_col_name, i);
		}
		int64_t date_val;
		if (is_timestamp) {
			date_val = UnifiedVectorFormat::GetData<int64_t>(part_fmt)[si];
		} else {
			date_val = static_cast<int64_t>(UnifiedVectorFormat::GetData<int32_t>(part_fmt)[si]);
		}
		// Cache lookup: avoid string formatting for repeated dates
		auto cache_it = local_state.partition_key_cache.find(date_val);
		const string *pk_ptr;
		if (cache_it != local_state.partition_key_cache.end()) {
			pk_ptr = &cache_it->second;
		} else {
			string pk;
			if (!EvaluatePartitionTemplate(bind_data.partition_template, date_val, pk)) {
				throw IOException("aligned COPY: cannot evaluate partition template '%s'",
				                  bind_data.partition_template);
			}
			pk_ptr = &local_state.partition_key_cache.emplace(date_val, std::move(pk)).first->second;
		}
		if (*pk_ptr != current_key) {
			if (i > run_start) {
				runs.push_back({run_start, i - run_start, std::move(current_key)});
			}
			current_key = *pk_ptr;
			run_start = i;
		}
	}
	// Final run
	if (input.size() > run_start) {
		runs.push_back({run_start, input.size() - run_start, std::move(current_key)});
	}

	// For each run: append (with optional column reorder) to local buffer.
	// Fast path: if input_col_map is identity AND all types match, append
	// the input chunk directly (zero-copy) instead of building a reordered
	// chunk. This is the common case when the query column order matches
	// the group schema order.
	bool identity_map = true;
	for (idx_t c = 0; c < bind_data.input_col_map.size(); c++) {
		if (bind_data.input_col_map[c] != c) {
			identity_map = false;
			break;
		}
	}
	bool types_match = identity_map;
	if (types_match) {
		for (idx_t c = 0; c < bind_data.sql_types.size(); c++) {
			if (input.data[c].GetType() != bind_data.sql_types[c]) {
				types_match = false;
				break;
			}
		}
	}

	for (auto &run : runs) {
		idx_t n = run.count;
		if (n == 0) {
			continue;
		}

		auto *pbuf = local_state.GetBuffer(run.key, context.client);

		// Fast path: identity map + types match + full-chunk run → zero-copy append.
		if (types_match && run.start == 0 && n == input.size()) {
			pbuf->collection.Append(pbuf->append_state, input);
			continue;
		}

		DataChunk chunk;
		chunk.Initialize(context.client, bind_data.sql_types);
		chunk.SetCardinality(n);

		bool full_chunk = (run.start == 0 && n == input.size());
		SelectionVector sel(n);
		if (!full_chunk) {
			// Partial run: build sequential selection vector for this range
			for (idx_t i = 0; i < n; i++) {
				sel.set_index(i, run.start + i);
			}
		}
		for (idx_t c = 0; c < bind_data.sql_types.size(); c++) {
			idx_t src_col = bind_data.input_col_map[c];
			auto &src_vec = input.data[src_col];
			auto &tgt_vec = chunk.data[c];
			if (src_vec.GetType() == tgt_vec.GetType()) {
				if (full_chunk) {
					// Full-chunk run: copy entire vector (no selection vector needed)
					VectorOperations::Copy(src_vec, tgt_vec, n, 0, 0);
				} else {
					VectorOperations::Copy(src_vec, tgt_vec, sel, n, 0, 0);
				}
			} else {
				Vector temp(src_vec.GetType(), n);
				if (full_chunk) {
					VectorOperations::Copy(src_vec, temp, n, 0, 0);
				} else {
					VectorOperations::Copy(src_vec, temp, sel, n, 0, 0);
				}
				VectorOperations::Cast(context.client, temp, tgt_vec, n);
			}
		}

		pbuf->collection.Append(pbuf->append_state, chunk);
	}
	// NOTE: Sink flushes per-RG (like native COPY TO PARQUET) to overlap
	// parquet compression with ingestion and bound CDC size. When a
	// per-partition local CDC reaches ALIGNED_DEFAULT_RG_ROWS (131072),
	// flush it to the global writer. Combine handles the final partial
	// CDC for each partition.
	constexpr idx_t RG_FLUSH_THRESHOLD = ALIGNED_DEFAULT_RG_ROWS;
	for (auto &kv : local_state.buffers) {
		auto &pbuf = *kv.second;
		if (pbuf.collection.Count() >= RG_FLUSH_THRESHOLD) {
			idx_t rows = pbuf.collection.Count();
			PartitionWriter *pw = global_state.Flush(kv.first, pbuf.collection);
			if (pw) {
				pw->received_rows += rows;
			}
			pbuf.collection.Reset();
			pbuf.collection.InitializeAppend(pbuf.append_state);
		}
	}
}

//===----------------------------------------------------------------------===//
// Combine: hand each partition's local buffer to the GlobalState FlushManager.
// This is the ONLY place that calls GlobalState::Flush (besides Finalize
// for the last partial data).
//===----------------------------------------------------------------------===//

static void AlignedCopyCombine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                                LocalFunctionData &lstate) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();
	auto &local_state = lstate.Cast<AlignedCopyLocalState>();

	for (auto &kv : local_state.buffers) {
		auto &partition_key = kv.first;
		auto &pbuf = *kv.second;
		idx_t rows = pbuf.collection.Count();
		if (rows == 0) {
			continue;
		}
		// Flush manages its own locking (global lock for writer lookup,
		// per-partition lock for the actual write). Different partitions
		// can flush in parallel across threads.
		PartitionWriter *pw = global_state.Flush(partition_key, pbuf.collection);
		// Track received rows on the partition writer (for final accounting).
		// received_rows is atomic — no lock needed. Flush returns the writer.
		if (pw) {
			pw->received_rows += rows;
		}
		// Reset the buffer (defensive — Combine is called once per thread).
		pbuf.collection.Reset();
		pbuf.collection.InitializeAppend(pbuf.append_state);
	}
}

//===----------------------------------------------------------------------===//
// Finalize: finalize all partition writers.  The destructor does nothing
// — all business logic lives here.
//===----------------------------------------------------------------------===//

static void AlignedCopyFinalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();

	for (auto &kv : global_state.writers) {
		auto &pw = *kv.second;
		if (pw.writer) {
			// FinalizePartition acquires its own per-partition lock;
			// different partitions can finalize in parallel.
			global_state.FinalizePartition(pw);
		}
		// Accounting verification: every received row must be written.
		idx_t received = pw.received_rows.load();
		idx_t written = pw.written_rows.load();
		if (received != written) {
			throw InternalException("aligned COPY: partition '%s' accounting mismatch: "
			                        "received=%llu, written=%llu (data may be corrupted)",
			                        pw.partition_key, (unsigned long long)received,
			                        (unsigned long long)written);
		}
	}
}

//===----------------------------------------------------------------------===//

static CopyFunctionExecutionMode AlignedCopyExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	if (!preserve_insertion_order) {
		return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
	}
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
