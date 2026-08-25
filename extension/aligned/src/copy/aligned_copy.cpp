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
// AlignedPartitionedColumnData
//===----------------------------------------------------------------------===//

AlignedPartitionedColumnData::AlignedPartitionedColumnData(ClientContext &context, vector<LogicalType> types,
                                                            idx_t partition_col_pos, string partition_template,
                                                            bool is_timestamp)
    : PartitionedColumnData(PartitionedColumnDataType::HIVE, context, std::move(types)),
      partition_col_pos(partition_col_pos), partition_template(std::move(partition_template)),
      is_timestamp(is_timestamp) {
	CreateAllocator();
}

idx_t AlignedPartitionedColumnData::RegisterPartition(const string &key,
                                                       PartitionedColumnDataAppendState &state) {
	auto it = key_to_id.find(key);
	if (it != key_to_id.end()) {
		return it->second;
	}
	idx_t id = key_to_id.size();
	key_to_id[key] = id;
	partition_keys[id] = key;

	// Create the partition collection, append state, and buffer — mirroring
	// HivePartitionedColumnData::AddNewPartition.
	if (state.partition_append_states.size() <= id) {
		state.partition_append_states.resize(id + 1);
		state.partition_buffers.resize(id + 1);
		partitions.resize(id + 1);
	}
	state.partition_append_states[id] = make_uniq<ColumnDataAppendState>();
	state.partition_buffers[id] = CreatePartitionBuffer();
	partitions[id] = CreatePartitionCollection(0);
	partitions[id]->InitializeAppend(*state.partition_append_states[id]);

	return id;
}

std::vector<idx_t> AlignedPartitionedColumnData::GetActivePartitionIds() const {
	std::vector<idx_t> ids;
	for (auto &kv : partition_keys) {
		if (partitions.size() > kv.first && partitions[kv.first] && partitions[kv.first]->Count() > 0) {
			ids.push_back(kv.first);
		}
	}
	return ids;
}

void AlignedPartitionedColumnData::ComputePartitionIndices(PartitionedColumnDataAppendState &state,
                                                           DataChunk &input) {
	const auto count = input.size();
	auto &part_vec = input.data[partition_col_pos];
	UnifiedVectorFormat part_fmt;
	part_vec.ToUnifiedFormat(count, part_fmt);

	// partition_indices was initialized as a FLAT UBIGINT vector with
	// STANDARD_VECTOR_SIZE capacity. We write directly into its buffer.
	auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);

	for (idx_t i = 0; i < count; i++) {
		auto si = part_fmt.sel->get_index(i);
		if (!part_fmt.validity.RowIsValid(si)) {
			throw IOException("aligned COPY: NULL in partition column at row %llu", i);
		}
		int64_t date_val;
		if (is_timestamp) {
			date_val = UnifiedVectorFormat::GetData<int64_t>(part_fmt)[si];
		} else {
			date_val = static_cast<int64_t>(UnifiedVectorFormat::GetData<int32_t>(part_fmt)[si]);
		}

		// Cache: single-slot fast cache for sorted-by-date input
		const string *pk_ptr;
		if (date_val == fast_cache_date && !fast_cache_key.empty()) {
			pk_ptr = &fast_cache_key;
		} else {
			auto cache_it = key_cache.find(date_val);
			if (cache_it != key_cache.end()) {
				pk_ptr = &cache_it->second;
			} else {
				string pk;
				if (!EvaluatePartitionTemplate(partition_template, date_val, is_timestamp, pk)) {
					throw IOException("aligned COPY: cannot evaluate partition template '%s'",
					                  partition_template);
				}
				pk_ptr = &key_cache.emplace(date_val, std::move(pk)).first->second;
			}
			fast_cache_date = date_val;
			fast_cache_key = *pk_ptr;
		}

		partition_indices[i] = RegisterPartition(*pk_ptr, state);
	}
}

//===----------------------------------------------------------------------===//
// LocalState
//===----------------------------------------------------------------------===//

AlignedCopyLocalState::AlignedCopyLocalState(ClientContext &context, const vector<LogicalType> &types) {
}

//===----------------------------------------------------------------------===//
// GlobalState
//===----------------------------------------------------------------------===//

AlignedCopyGlobalState::AlignedCopyGlobalState(ClientContext &ctx, FileSystem &f, const AlignedCopyBindData &bd)
    : context(ctx), fs(f), bind_data(bd) {
}

PartitionWriter *AlignedCopyGlobalState::Flush(const string &partition_key, ColumnDataCollection &buffer,
                                               std::unordered_map<string, PartitionWriter *> *writer_cache) {
	idx_t rows = buffer.Count();
	if (rows == 0) {
		return nullptr;
	}

	// Fast path: check the calling thread's local writer cache first.
	// This avoids acquiring the global lock on every Flush call for
	// partitions that have already been seen. The cache is populated
	// on first encounter of each partition key.
	if (writer_cache) {
		auto cache_it = writer_cache->find(partition_key);
		if (cache_it != writer_cache->end()) {
			PartitionWriter *pw = cache_it->second;
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
	}

	// Slow path: first time this thread sees this partition — acquire
	// global lock to create/lookup the PartitionWriter.
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
	// Cache the writer pointer for future calls from this thread.
	if (writer_cache) {
		(*writer_cache)[partition_key] = pw;
	}

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
	bind_data->input_types = sql_types;

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
	auto result = make_uniq<AlignedCopyLocalState>(context.client, bind_data.sql_types);

	// AlignedPartitionedColumnData uses the FULL INPUT schema (all columns
	// from the query, including symbol/date), so partition_col_pos (an input
	// position) is directly valid. In Combine, we project out only the group
	// schema columns before flushing to ParquetWriter.
	bool is_timestamp = false;
	if (bind_data.partition_col_pos < bind_data.input_types.size()) {
		is_timestamp = bind_data.input_types[bind_data.partition_col_pos].id() == LogicalTypeId::TIMESTAMP;
	}

	result->part_data = make_uniq<AlignedPartitionedColumnData>(
	    context.client, bind_data.input_types, bind_data.partition_col_pos,
	    bind_data.partition_template, is_timestamp);
	result->part_append_state = make_uniq<PartitionedColumnDataAppendState>();
	result->part_data->InitializeAppendState(*result->part_append_state);
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Sink: append input chunk to PartitionedColumnData. All partitioning,
// buffering, and selection vector management is handled by the
// PartitionedColumnData base class — no manual run detection.
//
// The input chunk is appended directly (all input columns, in input order).
// Column mapping/casting to group schema happens in Combine when projecting
// before flushing to ParquetWriter.
//===----------------------------------------------------------------------===//

static void AlignedCopySink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                             LocalFunctionData &lstate, DataChunk &input) {
	auto &local_state = lstate.Cast<AlignedCopyLocalState>();

	if (input.size() == 0) {
		return;
	}

	local_state.part_data->Append(*local_state.part_append_state, input);
}

//===----------------------------------------------------------------------===//
// Combine: flush all partitioned data to ParquetWriter via GlobalState.
// PartitionedColumnData::FlushAppendState finalizes per-partition buffers,
// then we iterate the partitions and flush each to the global writer.
//===----------------------------------------------------------------------===//

static void AlignedCopyCombine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                                LocalFunctionData &lstate) {
	auto &bind_data = bind_data_p.Cast<AlignedCopyBindData>();
	auto &global_state = gstate.Cast<AlignedCopyGlobalState>();
	auto &local_state = lstate.Cast<AlignedCopyLocalState>();

	// Flush remaining data from append state buffers into partition collections.
	local_state.part_data->FlushAppendState(*local_state.part_append_state);

	auto &partitions = local_state.part_data->GetPartitions();

	// Check if input already matches group schema (identity map + types match).
	// When true, the projection uses zero-copy Reference instead of full Copy.
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
			if (bind_data.input_types[c] != bind_data.sql_types[c]) {
				types_match = false;
				break;
			}
		}
	}

	for (idx_t i = 0; i < partitions.size(); i++) {
		if (!partitions[i] || partitions[i]->Count() == 0) {
			continue;
		}
		const string &partition_key = local_state.part_data->GetPartitionKey(i);
		idx_t rows = partitions[i]->Count();

		// Project from input schema (all columns) to group schema (only
		// group columns, reordered + cast). The PartitionedColumnData stores
		// all input columns; the ParquetWriter expects only group schema
		// columns.
		ColumnDataCollection projected(context.client, bind_data.sql_types);
		ColumnDataAppendState proj_append;
		projected.InitializeAppend(proj_append);

		for (auto &chunk : partitions[i]->Chunks()) {
			DataChunk mapped;
			mapped.Initialize(context.client, bind_data.sql_types);
			mapped.SetCardinality(chunk.size());

			for (idx_t c = 0; c < bind_data.sql_types.size(); c++) {
				idx_t src_col = bind_data.input_col_map[c];
				auto &src_vec = chunk.data[src_col];
				auto &tgt_vec = mapped.data[c];
				if (src_vec.GetType() == tgt_vec.GetType()) {
					// Zero-copy reference when types match.
					tgt_vec.Reference(src_vec);
				} else {
					Vector temp(src_vec.GetType(), chunk.size());
					VectorOperations::Copy(src_vec, temp, chunk.size(), 0, 0);
					VectorOperations::Cast(context.client, temp, tgt_vec, chunk.size());
				}
			}
			projected.Append(proj_append, mapped);
		}

		PartitionWriter *pw = global_state.Flush(partition_key, projected,
		                                            &local_state.writer_cache);
		if (pw) {
			pw->received_rows += rows;
		}
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
