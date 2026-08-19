#include "writer/aligned_writer.hpp"

#include "catalog/manifest.hpp"
#include "resolver/partition_resolver.hpp"
#include "resolver/row_space.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "parquet_field_id.hpp"
#include "parquet_shredding.hpp"
#include "zstd_file_system.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data / state
//===----------------------------------------------------------------------===//

struct AlignedWriteBindData : public TableFunctionData {
	TablePlan plan;
	string source_path;
	// Per group: source column names (in the written file's column order)
	vector<vector<string>> group_columns;
	// Per group: source column name of the partition key (from the manifest
	// templates); empty when the group has no partitioning
	vector<string> group_partition_col;
	idx_t start_row = DConstants::INVALID_INDEX; // explicit start; default = table end
	idx_t source_rows = 0;                       // validated at bind
	vector<LogicalType> types;
	vector<string> names;
};

struct AlignedWriteGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t rows_written = 0;
	idx_t parts_written = 0;
	idx_t txid = 0;
};

//===----------------------------------------------------------------------===//
// Small JSON / file helpers
//===----------------------------------------------------------------------===//

static void WriteTextFile(FileSystem &fs, const string &path, const string &content) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	// FILE_FLAGS_FILE_CREATE does not truncate an existing file: the old tail
	// would remain past the new content (invalid JSON on rewrite). Truncate
	// first.
	handle->Truncate(0);
	handle->Write(const_cast<char *>(content.c_str()), content.size());
	handle->Sync();
	handle->Close();
}

static string JsonEscape(const string &s) {
	string out;
	for (auto c : s) {
		if (c == '"' || c == '\\') {
			out += '\\';
			out += c;
		} else if (c == '\n') {
			out += "\\n";
		} else {
			out += c;
		}
	}
	return out;
}

static string JsonStringArray(const vector<string> &items) {
	string out = "[";
	for (idx_t i = 0; i < items.size(); i++) {
		if (i > 0) {
			out += ",";
		}
		out += "\"" + JsonEscape(items[i]) + "\"";
	}
	out += "]";
	return out;
}

//! Next transaction id = last_txid + 1. There are no commit markers anymore;
//! _table.json's last_txid field is the only transaction record (bumped on
//! every successful commit). Crash leftovers in _tmp/ are invisible to
//! readers ('.'/'_' dirs) and cleaned up by the next transaction.
static idx_t NextTransactionId(const TablePlan &plan) {
	return plan.table.last_txid + 1;
}

//! Scans a directory for existing part-*.parquet files and returns the next
//! free part index (part-%06llu).
static idx_t NextPartIndex(FileSystem &fs, const string &dir) {
	idx_t next = 0;
	if (!fs.DirectoryExists(dir)) {
		return 0;
	}
	fs.ListFiles(dir, [&](OpenFileInfo &info) {
		auto &name = info.path;
		if (StringUtil::StartsWith(name, "part-") && StringUtil::EndsWith(name, ".parquet") && name.size() > 13) {
			string idx_str = name.substr(5, name.size() - 5 - 8);
			try {
				unsigned long long value = std::stoull(idx_str);
				next = MaxValue<idx_t>(next, (idx_t)value + 1);
			} catch (...) {
			}
		}
	});
	return next;
}

//===----------------------------------------------------------------------===//
// Mapping parsing
//===----------------------------------------------------------------------===//

//! "group:col1,col2;group2:col3" -> map. Throws on malformed input.
static void ParseMapping(const string &mapping, case_insensitive_map_t<vector<string>> &out) {
	auto groups = StringUtil::Split(mapping, ';');
	for (auto &entry : groups) {
		if (entry.empty()) {
			continue;
		}
		auto colon = entry.find(':');
		if (colon == string::npos) {
			throw BinderException("aligned_write: invalid mapping entry '%s' (expected 'group:col1,col2')",
			                      entry);
		}
		string group_name = entry.substr(0, colon);
		StringUtil::Trim(group_name);
		auto columns = StringUtil::Split(entry.substr(colon + 1), ',');
		vector<string> cols;
		for (auto &c : columns) {
			string trimmed = c;
			StringUtil::Trim(trimmed);
			if (!trimmed.empty()) {
				cols.push_back(trimmed);
			}
		}
		if (group_name.empty() || cols.empty()) {
			throw BinderException("aligned_write: invalid mapping entry '%s' (expected 'group:col1,col2')", entry);
		}
		out[group_name] = std::move(cols);
	}
}

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

unique_ptr<FunctionData> AlignedWriteBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AlignedWriteBindData>();
	if (input.inputs.size() != 3) {
		throw BinderException("aligned_write: expected (table_name, source_path, mapping)");
	}
	string table = StringValue::Get(input.inputs[0]);
	result->source_path = StringValue::Get(input.inputs[1]);
	string mapping_str = StringValue::Get(input.inputs[2]);

	string root;
	auto entry = input.named_parameters.find("root");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		root = StringValue::Get(entry->second);
	} else {
		Value setting_value;
		if (!context.TryGetCurrentSetting("aligned_data_root", setting_value)) {
			throw BinderException("aligned_write: no data root configured. Pass root='...' or SET aligned_data_root");
		}
		root = StringValue::Get(setting_value);
	}
	auto start_entry = input.named_parameters.find("start_row");
	if (start_entry != input.named_parameters.end() && !start_entry->second.IsNull()) {
		result->start_row = start_entry->second.GetValue<uint64_t>();
	}

	BuildTablePlan(context, root, table, result->plan);

	// Parse + validate the mapping against the table groups
	case_insensitive_map_t<vector<string>> mapping;
	ParseMapping(mapping_str, mapping);
	result->group_columns.resize(result->plan.groups.size());
	result->group_partition_col.resize(result->plan.groups.size());
	for (auto &entry : mapping) {
		idx_t gi = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < result->plan.groups.size(); i++) {
			if (StringUtil::CIEquals(result->plan.groups[i].manifest.group, entry.first)) {
				gi = i;
				break;
			}
		}
		if (gi == DConstants::INVALID_INDEX) {
			throw BinderException("aligned_write: mapping references unknown group '%s'", entry.first);
		}
		result->group_columns[gi] = entry.second;
		// Partition source column from the manifest templates
		for (auto &t : result->plan.groups[gi].manifest.partitioning) {
			if (result->group_partition_col[gi].empty()) {
				result->group_partition_col[gi] = t.source;
			} else if (!StringUtil::CIEquals(result->group_partition_col[gi], t.source)) {
				throw BinderException("aligned_write: group '%s' partitions on multiple source columns ('%s' and "
				                      "'%s') — not supported",
				                      entry.first, result->group_partition_col[gi], t.source);
			}
		}
	}

	// Validate the source parquet: all mapped columns + partition columns exist
	auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(result->source_path), ParquetOptions(context));
	case_insensitive_map_t<LogicalType> source_schema;
	for (auto &col : reader->columns) {
		source_schema[col.name] = col.type;
	}
	// Partition source columns must be DATE
	for (idx_t gi = 0; gi < result->plan.groups.size(); gi++) {
		for (auto &col : result->group_columns[gi]) {
			if (source_schema.find(col) == source_schema.end()) {
				throw BinderException("aligned_write: column '%s' (group '%s') not found in source '%s'", col,
				                      result->plan.groups[gi].manifest.group, result->source_path);
			}
		}
		auto &pcol = result->group_partition_col[gi];
		if (!pcol.empty()) {
			auto it = source_schema.find(pcol);
			if (it == source_schema.end()) {
				throw BinderException("aligned_write: partition column '%s' (group '%s') not found in source '%s'",
				                      pcol, result->plan.groups[gi].manifest.group, result->source_path);
			}
			if (it->second.id() != LogicalTypeId::DATE) {
				throw BinderException("aligned_write: partition column '%s' must be DATE (got %s)", pcol,
				                      it->second.ToString());
			}
		}
	}
	result->source_rows = reader->NumRows();

	result->types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	result->names = {"rows_written", "parts_written", "txid"};
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> AlignedWriteInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<AlignedWriteGlobalState>();
}

//===----------------------------------------------------------------------===//
// The write itself
//===----------------------------------------------------------------------===//

namespace {

struct WrittenPart {
	string target_dir; // absolute partition dir
	string staged_path;
	string part_name;
	idx_t start_row;
	idx_t row_count;
	vector<string> columns;
};

struct GroupWriterState {
	const GroupPlan *group = nullptr;
	vector<LogicalType> col_types; // mapped columns, in written order
	vector<string> col_names;      // mapped column names, in written order
	vector<idx_t> src_pos;         // source chunk position per mapped column
	idx_t partition_pos = DConstants::INVALID_INDEX; // source chunk position of the partition column
	idx_t rgs = 131072;                              // row group size for flushing

	// Current open part (partition dir-scoped)
	string part_target_dir;
	string part_staged_path;
	string part_name;
	idx_t part_start_row = 0;
	idx_t part_rows = 0;
	unique_ptr<ParquetWriter> writer;
	unique_ptr<ParquetWriteTransformData> transform;
	unique_ptr<ColumnDataCollection> buffer;
	ColumnDataAppendState append_state;
	unique_ptr<DataChunk> slice; // scratch chunk for row-slice assembly

	vector<WrittenPart> written;
};

} // namespace

//! Formats the partition path ("/year=2026/month=08/day=19") of a group for a
//! date value (empty when the group has no partitioning).
static string FormatPartitionPath(const GroupPlan &group, date_t value) {
	string parts;
	for (auto &t : group.manifest.partitioning) {
		string formatted;
		if (!EvaluatePartitionTemplate(t.template_str, value, formatted)) {
			throw IOException("Aligned table '%s' group '%s': cannot evaluate partition template '%s'",
			                  group.manifest.group.c_str(), group.manifest.group.c_str(), t.template_str.c_str());
		}
		parts += "/" + formatted;
	}
	return parts;
}

static void ClosePart(GroupWriterState &s) {
	if (!s.writer) {
		return;
	}
	s.writer->Flush(*s.buffer, s.transform);
	s.writer->Finalize();
	s.buffer->Reset();
	WrittenPart part;
	part.target_dir = s.part_target_dir;
	part.staged_path = s.part_staged_path;
	part.part_name = s.part_name;
	part.start_row = s.part_start_row;
	part.row_count = s.part_rows;
	part.columns = s.col_names;
	s.written.push_back(std::move(part));
	s.writer.reset();
}

//===----------------------------------------------------------------------===//
// Main entry
//===----------------------------------------------------------------------===//

void AlignedWriteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<AlignedWriteBindData>();
	auto &gstate = data.global_state->Cast<AlignedWriteGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	// The write happens here (single-threaded by design; the table function
	// does not override MaxThreads). Any error aborts the transaction: the
	// staged _tmp tree is removed before rethrowing.
	auto &fs = FileSystem::GetFileSystem(context);

	idx_t start_row = bind.start_row == DConstants::INVALID_INDEX ? bind.plan.table.row_count : bind.start_row;
	if (start_row != bind.plan.table.row_count) {
		throw IOException("Aligned table '%s': append must start at the current table end (row %llu); got %llu",
		                  bind.plan.table.name, bind.plan.table.row_count, start_row);
	}
	idx_t new_total = start_row + bind.source_rows;

	// Simulate the post-append row space: every group must receive the full
	// appended range (the alignment contract forbids gaps).
	{
		vector<vector<PartInfo>> simulated(bind.plan.groups.size());
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			auto &group = bind.plan.groups[gi];
			if (bind.group_columns[gi].empty()) {
				throw IOException("Aligned table '%s': group '%s' has no mapped columns — every group must be "
				                  "written (alignment contract)",
				                  bind.plan.table.name, group.manifest.group);
			}
			simulated[gi] = group.parts;
			PartInfo extended;
			extended.start_row = start_row;
			extended.row_count = bind.source_rows;
			simulated[gi].push_back(extended);
			ValidateRowSpace(bind.plan.table.name, group.manifest.group, new_total, simulated[gi]);
		}
	}

	// txid = last committed txid + 1 (markers are gone)
	idx_t txid = NextTransactionId(bind.plan);
	gstate.txid = txid;
	string tmp_root = bind.plan.table_path + "/_tmp/transaction-" + to_string(txid);

	try {
		// Open the source and scan it in chunks
		auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(bind.source_path), ParquetOptions(context));

		// Needed source columns: all mapped columns + partition columns
		vector<string> needed;
		case_insensitive_map_t<idx_t> needed_pos; // name -> position in `needed`
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			for (auto &col : bind.group_columns[gi]) {
				if (needed_pos.find(col) == needed_pos.end()) {
					needed_pos[col] = needed.size();
					needed.push_back(col);
				}
			}
			if (!bind.group_partition_col[gi].empty() &&
			    needed_pos.find(bind.group_partition_col[gi]) == needed_pos.end()) {
				needed_pos[bind.group_partition_col[gi]] = needed.size();
				needed.push_back(bind.group_partition_col[gi]);
			}
		}
		vector<LogicalType> needed_types;
		vector<idx_t> needed_file_idx;
		for (auto &col : needed) {
			for (idx_t i = 0; i < reader->columns.size(); i++) {
				if (StringUtil::CIEquals(reader->columns[i].name, col)) {
					needed_file_idx.push_back(i);
					needed_types.push_back(reader->columns[i].type);
					break;
				}
			}
		}
		for (auto idx : needed_file_idx) {
			reader->column_ids.push_back(MultiFileLocalColumnId(idx));
		}

		// Prepare per-group writer states
		vector<GroupWriterState> gstates(bind.plan.groups.size());
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			auto &gs = gstates[gi];
			gs.group = &bind.plan.groups[gi];
			gs.rgs = bind.plan.table.row_group_size > 0 ? bind.plan.table.row_group_size : 131072;
			gs.col_names = bind.group_columns[gi];
			for (auto &col : bind.group_columns[gi]) {
				gs.col_types.push_back(reader->columns[needed_file_idx[needed_pos[col]]].type);
				gs.src_pos.push_back(needed_pos[col]);
			}
			if (!bind.group_partition_col[gi].empty()) {
				gs.partition_pos = needed_pos[bind.group_partition_col[gi]];
			}
			gs.buffer = make_uniq<ColumnDataCollection>(context, gs.col_types);
			gs.buffer->InitializeAppend(gs.append_state);
			gs.slice = make_uniq<DataChunk>();
			gs.slice->Initialize(context, gs.col_types);
		}

		ParquetReaderScanState scan_state;
		vector<idx_t> all_rgs;
		vector<PartitionStatistics> rg_stats;
		reader->GetPartitionStats(rg_stats);
		for (idx_t i = 0; i < rg_stats.size(); i++) {
			all_rgs.push_back(i);
		}
		reader->InitializeScan(context, scan_state, all_rgs);
		DataChunk source_chunk;
		source_chunk.Initialize(context, needed_types);

		idx_t src_row = 0;
		while (true) {
			auto res = reader->Scan(context, scan_state, source_chunk);
			auto async_type = res.GetResultType();
			if (async_type == AsyncResultType::FINISHED || async_type == AsyncResultType::BLOCKED) {
				break;
			}
			idx_t rows = source_chunk.size();
			if (rows == 0) {
				continue;
			}

			// Split each group's rows at partition changes and append
			for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
				auto &gs = gstates[gi];
				idx_t r = 0;
				// The partition column may be dictionary-encoded (e.g. a
				// constant date) — read via UnifiedVectorFormat, never with
				// FlatVector::GetData.
				date_t pv;
				UnifiedVectorFormat pvdata;
				const date_t *pdata = nullptr;
				const SelectionVector *psel = nullptr;
				if (gs.partition_pos != DConstants::INVALID_INDEX) {
					source_chunk.data[gs.partition_pos].ToUnifiedFormat(rows, pvdata);
					pdata = UnifiedVectorFormat::GetData<date_t>(pvdata);
					psel = pvdata.sel;
				}
				while (r < rows) {
					if (pdata) {
						pv = pdata[psel->get_index(r)];
					}
					idx_t r2 = r + 1;
					if (pdata) {
						while (r2 < rows && pdata[psel->get_index(r2)] == pv) {
							r2++;
						}
					}
					// target partition dir (absolute) for this group
					string part_path = pdata ? FormatPartitionPath(*gs.group, pv) : string();
					string target_dir = gs.group->group_path + part_path;

					// Open a new part when the partition dir changes
					if (gs.part_target_dir != target_dir) {
						ClosePart(gs);
						gs.part_target_dir = target_dir;
						gs.part_name = StringUtil::Format("part-%06llu",
						                                  (unsigned long long)NextPartIndex(fs, target_dir));
						gs.part_start_row = start_row + src_row + r;
						gs.part_rows = 0;
						// staged path under _tmp/transaction-<txid>/<group>/<partition path>
						string staged_dir = tmp_root + "/" + gs.group->manifest.group + part_path;
						fs.CreateDirectoriesRecursive(staged_dir);
						gs.part_staged_path = staged_dir + "/" + gs.part_name + ".parquet";
						gs.writer = make_uniq<ParquetWriter>(
						    context, fs, gs.part_staged_path, gs.col_types, gs.col_names,
						    duckdb_parquet::CompressionCodec::ZSTD, ChildFieldIDs(), ShreddingType(),
						    vector<pair<string, string>>(), nullptr, optional_idx(),
						    1073741824ULL /* PrimitiveColumnWriter::MAX_UNCOMPRESSED_DICT_PAGE_SIZE */, 1, 0.01,
						    ZStdFileSystem::DefaultCompressionLevel(), ParquetVersion::V1, GeoParquetVersion::V1);
					}

					// Assemble the row slice [r, r2) into the group's chunk
					idx_t slice_rows = r2 - r;
					gs.slice->Reset();
					gs.slice->SetCardinality(slice_rows);
					for (idx_t c = 0; c < gs.src_pos.size(); c++) {
						VectorOperations::Copy(source_chunk.data[gs.src_pos[c]], gs.slice->data[c], r2, r, 0);
					}
					gs.buffer->Append(gs.append_state, *gs.slice);
					gs.part_rows += slice_rows;

					// Flush a row group when full
					if (gs.buffer->Count() >= gs.rgs) {
						gs.writer->Flush(*gs.buffer, gs.transform);
						gs.buffer->Reset();
						gs.buffer->InitializeAppend(gs.append_state);
					}
					r = r2;
				}
			}
			src_row += rows;
		}

		if (src_row != bind.source_rows) {
			throw IOException("Aligned table '%s': source scan produced %llu rows, expected %llu",
			                  bind.plan.table.name, src_row, bind.source_rows);
		}

		// Close all parts (flush + finalize)
		for (auto &gs : gstates) {
			ClosePart(gs);
		}

		// Commit: move the staged parts into place. There are no sidecars and
		// no commit markers anymore — the part files themselves are the
		// commit record (each target dir + part name is unique per
		// transaction). Readers only see the parts after the moves complete;
		// a failed transaction leaves only _tmp/ leftovers, which are
		// invisible ('.'/'_' dirs) and cleaned up by the next commit.
		for (auto &gs : gstates) {
			for (auto &part : gs.written) {
				fs.CreateDirectoriesRecursive(part.target_dir);
				string target_path = part.target_dir + "/" + part.part_name + ".parquet";
				if (fs.FileExists(target_path)) {
					throw IOException("Aligned table '%s' group '%s': part '%s' already exists in '%s'",
					                  bind.plan.table.name, gs.group->manifest.group, part.part_name,
					                  part.target_dir);
				}
				fs.MoveFile(part.staged_path, target_path);
				gstate.parts_written++;
			}
		}

		// Bump the row count and last_txid in _table.json. The group
		// manifests (_group.json) are gone; row counts are derived from the
		// Parquet footers anyway.
		{
			auto &plan = bind.plan;
			string partitioning;
			if (!plan.table.partitioning.empty()) {
				partitioning = ",\"partitioning\":{";
				bool first_group = true;
				for (auto &entry : plan.table.partitioning) {
					if (!first_group) {
						partitioning += ",";
					}
					first_group = false;
					partitioning += "\"" + JsonEscape(entry.first) + "\":[";
					for (idx_t i = 0; i < entry.second.size(); i++) {
						if (i > 0) {
							partitioning += ",";
						}
						partitioning += "{\"template\":\"" + JsonEscape(entry.second[i].template_str) +
						                "\",\"source\":\"" + JsonEscape(entry.second[i].source) + "\"}";
					}
					partitioning += "]";
				}
				partitioning += "}";
			}
			string table_manifest =
			    "{\"name\":\"" + JsonEscape(plan.table.name) + "\",\"version\":" + to_string(plan.table.version) +
			    ",\"schema_version\":" + to_string(plan.table.schema_version) + ",\"key\":" +
			    JsonStringArray(plan.table.key) + ",\"canonical_order\":\"" + JsonEscape(plan.table.canonical_order) +
			    "\",\"row_count\":" + to_string(new_total) + ",\"row_group_size\":" +
			    to_string(plan.table.row_group_size);
			if (plan.table.part_rows > 0) {
				table_manifest += ",\"part_rows\":" + to_string(plan.table.part_rows);
			}
			table_manifest += ",\"last_txid\":" + to_string(txid) + ",\"groups\":" + JsonStringArray(plan.table.groups) +
			                  partitioning + "}";
			WriteTextFile(fs, plan.table_path + "/_table.json", table_manifest);
		}

		// Cleanup the staging tree (the transaction dir is removed recursively;
		// the empty _tmp parent is removed best-effort)
		if (fs.DirectoryExists(tmp_root)) {
			fs.RemoveDirectory(tmp_root);
		}
		string tmp_parent = bind.plan.table_path + "/_tmp";
		try {
			if (fs.DirectoryExists(tmp_parent)) {
				fs.RemoveDirectory(tmp_parent);
			}
		} catch (...) {
		}

		gstate.rows_written = bind.source_rows;
		output.SetCardinality(1);
		output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_written)));
		output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(gstate.parts_written)));
		output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(gstate.txid)));
	} catch (...) {
		// abort the transaction: remove the staging tree
		try {
			if (fs.DirectoryExists(tmp_root)) {
				fs.RemoveDirectory(tmp_root);
			}
			string tmp_parent = bind.plan.table_path + "/_tmp";
			if (fs.DirectoryExists(tmp_parent)) {
				fs.RemoveDirectory(tmp_parent);
			}
		} catch (...) {
		}
		throw;
	}
}

} // namespace duckdb
