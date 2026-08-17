#include "compaction/aligned_compactor.hpp"

#include "catalog/manifest.hpp"
#include "resolver/row_space.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "parquet_field_id.hpp"
#include "parquet_shredding.hpp"
#include "zstd_file_system.hpp"
#include "yyjson.hpp"

#include <set>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data / state
//===----------------------------------------------------------------------===//

struct AlignedCompactBindData : public TableFunctionData {
	TablePlan plan;
	idx_t group_idx = DConstants::INVALID_INDEX;
	vector<LogicalType> types;
	vector<string> names;
};

struct AlignedCompactGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t dirs_compacted = 0;
	idx_t parts_before = 0;
	idx_t parts_after = 0;
};

//===----------------------------------------------------------------------===//
// Small JSON / file helpers (see aligned_writer.cpp for the same helpers)
//===----------------------------------------------------------------------===//

static string ReadTextFile(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	idx_t size = handle->GetFileSize();
	string result;
	result.resize(size);
	if (size > 0) {
		handle->Read(&result[0], size, 0);
	}
	return result;
}

static void WriteTextFile(FileSystem &fs, const string &path, const string &content) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
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

static void ReadMarker(FileSystem &fs, const string &dir, idx_t &txid, vector<string> &parts) {
	string marker_path = dir + "/.aligned-commit.json";
	if (!fs.FileExists(marker_path)) {
		return;
	}
	string content = ReadTextFile(fs, marker_path);
	auto doc = duckdb_yyjson::yyjson_read(content.c_str(), content.size(), 0);
	if (!doc) {
		throw IOException("Aligned table: invalid commit marker JSON in '%s'", marker_path);
	}
	auto root = duckdb_yyjson::yyjson_doc_get_root(doc);
	auto txid_val = duckdb_yyjson::yyjson_obj_get(root, "txid");
	if (txid_val && duckdb_yyjson::yyjson_is_uint(txid_val)) {
		txid = duckdb_yyjson::yyjson_get_uint(txid_val);
	}
	auto parts_val = duckdb_yyjson::yyjson_obj_get(root, "parts");
	if (parts_val && duckdb_yyjson::yyjson_is_arr(parts_val)) {
		auto size = duckdb_yyjson::yyjson_arr_size(parts_val);
		for (size_t i = 0; i < size; i++) {
			auto item = duckdb_yyjson::yyjson_arr_get(parts_val, i);
			if (duckdb_yyjson::yyjson_is_str(item)) {
				parts.emplace_back(duckdb_yyjson::yyjson_get_str(item), duckdb_yyjson::yyjson_get_len(item));
			}
		}
	}
	duckdb_yyjson::yyjson_doc_free(doc);
}

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
// Bind
//===----------------------------------------------------------------------===//

unique_ptr<FunctionData> AlignedCompactBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AlignedCompactBindData>();
	if (input.inputs.size() != 2) {
		throw BinderException("aligned_compact: expected (table_name, group_name)");
	}
	string table = StringValue::Get(input.inputs[0]);
	string group_name = StringValue::Get(input.inputs[1]);

	string root;
	auto entry = input.named_parameters.find("root");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		root = StringValue::Get(entry->second);
	} else {
		Value setting_value;
		if (!context.TryGetCurrentSetting("aligned_data_root", setting_value)) {
			throw BinderException("aligned_compact: no data root configured. Pass root='...' or SET aligned_data_root");
		}
		root = StringValue::Get(setting_value);
	}

	BuildTablePlan(context, root, table, result->plan);
	for (idx_t gi = 0; gi < result->plan.groups.size(); gi++) {
		if (StringUtil::CIEquals(result->plan.groups[gi].manifest.group, group_name)) {
			result->group_idx = gi;
			break;
		}
	}
	if (result->group_idx == DConstants::INVALID_INDEX) {
		throw BinderException("aligned_compact: unknown group '%s'", group_name);
	}

	result->types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	result->names = {"dirs_compacted", "parts_before", "parts_after"};
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> AlignedCompactInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<AlignedCompactGlobalState>();
}

//===----------------------------------------------------------------------===//
// Compaction
//===----------------------------------------------------------------------===//

void AlignedCompactFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<AlignedCompactBindData>();
	auto &gstate = data.global_state->Cast<AlignedCompactGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &fs = FileSystem::GetFileSystem(context);
	auto &group = bind.plan.groups[bind.group_idx];

	// Group the parts by partition directory
	std::map<string, vector<const PartInfo *>> by_dir;
	for (auto &part : group.parts) {
		auto slash = part.path.find_last_of("/\\");
		string dir = slash == string::npos ? "" : part.path.substr(0, slash);
		by_dir[dir].push_back(&part);
	}

	// txid = max existing + 1
	idx_t txid = 1;
	for (auto &kv : by_dir) {
		idx_t marker_txid = 0;
		vector<string> marker_parts;
		ReadMarker(fs, kv.first, marker_txid, marker_parts);
		txid = MaxValue<idx_t>(txid, marker_txid + 1);
	}
	string tmp_root = bind.plan.table_path + "/_tmp/transaction-" + to_string(txid);

	try {
		idx_t dirs_compacted = 0;
		idx_t parts_before = 0;
		idx_t parts_after = 0;

		for (auto &kv : by_dir) {
			auto &dir = kv.first;
			auto &parts = kv.second;
			parts_before += parts.size();
			parts_after += parts.size();
			if (parts.size() < 2) {
				continue; // nothing to merge
			}

			// All parts must share the same column set (schema evolution
			// within a directory cannot be compacted in v1)
			auto &columns = parts[0]->columns;
			for (idx_t i = 1; i < parts.size(); i++) {
				if (parts[i]->columns != columns) {
					throw IOException("Aligned table '%s' group '%s': cannot compact directory '%s' — parts have "
					                  "different column sets (schema evolution within a directory)",
					                  bind.plan.table.name, group.manifest.group, dir);
				}
			}

			// Rows must be contiguous within the directory
			idx_t start_row = parts[0]->start_row;
			idx_t row_count = 0;
			for (idx_t i = 0; i < parts.size(); i++) {
				if (i > 0 && parts[i]->start_row != parts[i - 1]->start_row + parts[i - 1]->row_count) {
					throw IOException("Aligned table '%s' group '%s': cannot compact directory '%s' — parts are not "
					                  "contiguous (alignment violation)",
					                  bind.plan.table.name, group.manifest.group, dir);
				}
				row_count += parts[i]->row_count;
			}

			// Column types: from the first part
			auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(parts[0]->path), ParquetOptions(context));
			vector<LogicalType> col_types;
			for (idx_t i = 0; i < parts[0]->columns.size(); i++) {
				bool found = false;
				for (auto &rc : reader->columns) {
					if (StringUtil::CIEquals(rc.name, parts[0]->columns[i])) {
						col_types.push_back(rc.type);
						found = true;
						break;
					}
				}
				if (!found) {
					throw IOException("Aligned table '%s' group '%s': part '%s' is missing column '%s'",
					                  bind.plan.table.name, group.manifest.group, parts[0]->part_name,
					                  parts[0]->columns[i]);
				}
			}

			// Staged new part
			idx_t rgs = group.manifest.row_group_size > 0 ? group.manifest.row_group_size
			                                              : bind.plan.table.row_group_size;
			string part_name = StringUtil::Format("part-%06llu", (unsigned long long)NextPartIndex(fs, dir));
			string staged_dir = tmp_root + "/" + group.manifest.group;
			fs.CreateDirectoriesRecursive(staged_dir);
			string staged_path = staged_dir + "/" + part_name + ".parquet";
			auto writer = make_uniq<ParquetWriter>(
			    context, fs, staged_path, col_types, parts[0]->columns, duckdb_parquet::CompressionCodec::ZSTD,
			    ChildFieldIDs(), ShreddingType(), vector<pair<string, string>>(), nullptr, optional_idx(),
			    1073741824ULL /* PrimitiveColumnWriter::MAX_UNCOMPRESSED_DICT_PAGE_SIZE */, 1, 0.01,
			    ZStdFileSystem::DefaultCompressionLevel(), ParquetVersion::V1, GeoParquetVersion::V1);
			unique_ptr<ParquetWriteTransformData> transform;
			auto buffer = make_uniq<ColumnDataCollection>(context, col_types);
			ColumnDataAppendState append_state;
			buffer->InitializeAppend(append_state);

			// Read each old part in order and append (position alignment
			// preserved: the merged part covers [start_row, start_row + row_count))
			for (auto &part : parts) {
				auto part_reader =
				    make_uniq<ParquetReader>(context, OpenFileInfo(part->path), ParquetOptions(context));
				// fresh reader: column_ids is empty — read ALL columns in the
				// sidecar order
				for (idx_t i = 0; i < part->columns.size(); i++) {
					part_reader->column_ids.push_back(MultiFileLocalColumnId(i));
				}
				ParquetReaderScanState scan_state;
				vector<PartitionStatistics> rg_stats;
				part_reader->GetPartitionStats(rg_stats);
				vector<idx_t> all_rgs;
				for (idx_t i = 0; i < rg_stats.size(); i++) {
					all_rgs.push_back(i);
				}
				part_reader->InitializeScan(context, scan_state, all_rgs);
				DataChunk chunk;
				chunk.Initialize(context, col_types);
				while (true) {
					auto res = part_reader->Scan(context, scan_state, chunk);
					auto async_type = res.GetResultType();
					if (async_type == AsyncResultType::FINISHED || async_type == AsyncResultType::BLOCKED) {
						break;
					}
					if (chunk.size() == 0) {
						continue;
					}
					buffer->Append(append_state, chunk);
					if (buffer->Count() >= rgs) {
						writer->Flush(*buffer, transform);
						buffer->Reset();
						buffer->InitializeAppend(append_state);
					}
				}
			}
			writer->Flush(*buffer, transform);
			writer->Finalize();

			// Sidecar for the merged part
			string sidecar = "{\"table\":\"" + JsonEscape(bind.plan.table.name) + "\",\"group\":\"" +
			                 JsonEscape(group.manifest.group) + "\",\"part\":\"" + JsonEscape(part_name) +
			                 "\",\"start_row\":" + to_string(start_row) + ",\"row_count\":" + to_string(row_count) +
			                 ",\"row_group_size\":" + to_string(rgs) + ",\"columns\":" +
			                 JsonStringArray(parts[0]->columns) + "}";
			string staged_sidecar = staged_dir + "/" + part_name + ".aligned.json";
			WriteTextFile(fs, staged_sidecar, sidecar);

			// Commit: move the new part + sidecar into place, replace the
			// marker, then delete the old parts
			string target_path = dir + "/" + part_name + ".parquet";
			if (fs.FileExists(target_path)) {
				throw IOException("Aligned table '%s' group '%s': part '%s' already exists in '%s'",
				                  bind.plan.table.name, group.manifest.group, part_name, dir);
			}
			fs.MoveFile(staged_path, target_path);
			fs.MoveFile(staged_sidecar, dir + "/" + part_name + ".aligned.json");

			string marker = "{\"txid\":" + to_string(txid) + ",\"parts\":" + JsonStringArray({part_name}) + "}";
			string tmp_marker = dir + "/.aligned-commit.json.tmp";
			WriteTextFile(fs, tmp_marker, marker);
			fs.MoveFile(tmp_marker, dir + "/.aligned-commit.json");

			// Delete the old parts (after the marker switch: they are already
			// invisible; failure here only leaves orphaned files)
			for (auto &part : parts) {
				fs.RemoveFile(part->path);
				string old_sidecar = part->path.substr(0, part->path.size() - 8) + ".aligned.json";
				if (fs.FileExists(old_sidecar)) {
					fs.RemoveFile(old_sidecar);
				}
			}

			dirs_compacted++;
			parts_after -= parts.size();
			parts_after += 1;
		}

		// Cleanup the staging tree (best-effort for the empty parent)
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

		gstate.dirs_compacted = dirs_compacted;
		gstate.parts_before = parts_before;
		gstate.parts_after = parts_after;
		output.SetCardinality(1);
		output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.dirs_compacted)));
		output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(gstate.parts_before)));
		output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(gstate.parts_after)));
	} catch (...) {
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
