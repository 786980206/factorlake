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

#include <map>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data / state
//===----------------------------------------------------------------------===//

struct AlignedCompactBindData : public TableFunctionData {
	TablePlan plan;
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
// Small helpers (see aligned_writer.cpp for the same helpers)
//===----------------------------------------------------------------------===//

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

static void WriteTextFile(FileSystem &fs, const string &path, const string &content) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	// FILE_FLAGS_FILE_CREATE does not truncate an existing file: the old tail
	// would remain past the new content (invalid JSON on rewrite).
	handle->Truncate(0);
	handle->Write(const_cast<char *>(content.c_str()), content.size());
	handle->Sync();
	handle->Close();
}

//! Bumps last_txid in _table.json (row counts and everything else unchanged).
static void BumpLastTxid(FileSystem &fs, const TablePlan &plan, idx_t txid) {
	auto &table = plan.table;
	string partitioning;
	if (!table.partitioning.empty()) {
		partitioning = ",\"partitioning\":{";
		bool first_group = true;
		for (auto &entry : table.partitioning) {
			if (!first_group) {
				partitioning += ",";
			}
			first_group = false;
			partitioning += "\"" + JsonEscape(entry.first) + "\":[";
			for (idx_t i = 0; i < entry.second.size(); i++) {
				if (i > 0) {
					partitioning += ",";
				}
				partitioning += "{\"template\":\"" + JsonEscape(entry.second[i].template_str) + "\",\"source\":\"" +
				                JsonEscape(entry.second[i].source) + "\"}";
			}
			partitioning += "]";
		}
		partitioning += "}";
	}
	string manifest = "{\"name\":\"" + JsonEscape(table.name) + "\",\"version\":" + to_string(table.version) +
	                  ",\"rg_rows\":" + to_string(table.rg_rows);
	if (table.part_rows > 0) {
		manifest += ",\"part_rows\":" + to_string(table.part_rows);
	}
	manifest += ",\"last_txid\":" + to_string(txid) + ",\"groups\":" + JsonStringArray(table.groups) + partitioning + "}";
	WriteTextFile(fs, plan.table_path + "/_table.json", manifest);
}

//! Next transaction id = last_txid + 1 (there are no commit markers anymore;
//! _table.json's last_txid is the only transaction record, bumped on every
//! successful commit).
static idx_t NextTransactionId(const TablePlan &plan) {
	return plan.table.last_txid + 1;
}

static idx_t NextPartIndex(FileSystem &fs, const string &dir) {
	idx_t next = 0;
	if (!fs.DirectoryExists(dir)) {
		return 0;
	}
	fs.ListFiles(dir, [&](OpenFileInfo &info) {
		auto &name = info.path;
		if (name.size() >= 16 && StringUtil::EndsWith(name, ".parquet")) {
			// base = "0002-0000002048" (15 chars, '-' at position 4)
			string base = name.substr(0, name.size() - 8);
			if (base.size() == 15 && base[4] == '-') {
				bool digits = true;
				for (idx_t i = 0; i < 15; i++) {
					if (i != 4 && (base[i] < '0' || base[i] > '9')) {
						digits = false;
						break;
					}
				}
				if (digits) {
					try {
						unsigned long long value = std::stoull(base.substr(0, 4));
						next = MaxValue<idx_t>(next, (idx_t)value + 1);
					} catch (...) {
					}
				}
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
	// Group argument: 'all' or any existing group name. Compaction ALWAYS
	// processes every group: the full-alignment contract requires all groups
	// to keep the same part count, so a per-group compaction would leave the
	// table in a divergent state that the reader rejects fail-fast.
	bool found = StringUtil::CIEquals(group_name, "all");
	if (!found) {
		for (auto &g : result->plan.groups) {
			if (StringUtil::CIEquals(g.manifest.group, group_name)) {
				found = true;
				break;
			}
		}
	}
	if (!found) {
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

	// txid = last committed txid + 1 (markers are gone)
	idx_t txid = NextTransactionId(bind.plan);
	string tmp_root = bind.plan.table_path + "/_tmp/transaction-" + to_string(txid);

	try {
		idx_t dirs_compacted = 0;
		idx_t parts_before = 0;
		idx_t parts_after = 0;

		// Compaction processes EVERY group in one atomic transaction so the
		// full-alignment contract (same part count across groups) is
		// preserved; the parts are grouped by partition directory and merged
		// per directory.
		for (auto &group : bind.plan.groups) {
			// Group the parts by partition directory
			std::map<string, vector<const PartInfo *>> by_dir;
			for (auto &part : group.parts) {
				auto slash = part.path.find_last_of("/\\");
				string dir = slash == string::npos ? "" : part.path.substr(0, slash);
				by_dir[dir].push_back(&part);
			}

			for (auto &kv : by_dir) {
				auto &dir = kv.first;
				auto &parts = kv.second;
				parts_before += parts.size();
				parts_after += parts.size();
				if (parts.size() < 2) {
					continue; // nothing to merge
				}

			// All parts must share the same column set (schema evolution
			// within a directory cannot be compacted in v1). The merged part's
			// schema is the first part's FILE schema (per-part column metadata
			// is not stored in the plan — it is read from the footer here).
			auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(parts[0]->path), ParquetOptions(context));
			vector<string> columns;
			vector<LogicalType> col_types;
			for (auto &rc : reader->columns) {
				columns.push_back(rc.name);
				col_types.push_back(rc.type);
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

			// Staged new part. v6: the merged part is the only part of the
			// partition, so its self-describing name is "{idx:04d}-{rows:10d}"
			// with idx = 0 (every group merges the same partition together, so
			// cross-group indexes stay consistent). The staging path includes
			// the partition's relative path so that multiple partitions of one
			// group do not collide.
			idx_t rgs = bind.plan.table.rg_rows > 0 ? bind.plan.table.rg_rows : 131072;
			if (row_count > 9999999999ULL) {
				throw IOException("Aligned table '%s' group '%s': merged part '%s' holds %llu rows — more than the "
				                  "self-describing name can represent (10 digits)",
				                  bind.plan.table.name, group.manifest.group, dir, row_count);
			}
			string part_name = StringUtil::Format("0000-%010llu", (unsigned long long)row_count);
			string group_rel = dir.substr(group.group_path.size());
			string staged_dir = tmp_root + "/" + group.manifest.group + group_rel;
			fs.CreateDirectoriesRecursive(staged_dir);
			string staged_path = staged_dir + "/" + part_name + ".parquet";
			auto writer = make_uniq<ParquetWriter>(
			    context, fs, staged_path, col_types, columns, duckdb_parquet::CompressionCodec::ZSTD,
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
				// Every part must share the first part's column set (same names,
				// same order) — schema evolution within a directory is rejected
				if (part_reader->columns.size() != columns.size()) {
					throw IOException("Aligned table '%s' group '%s': cannot compact directory '%s' — parts have "
					                  "different column sets (schema evolution within a directory)",
					                  bind.plan.table.name, group.manifest.group, dir);
				}
				for (idx_t ci = 0; ci < columns.size(); ci++) {
					if (part_reader->columns[ci].name != columns[ci]) {
						throw IOException("Aligned table '%s' group '%s': cannot compact directory '%s' — parts have "
						                  "different column sets (schema evolution within a directory)",
						                  bind.plan.table.name, group.manifest.group, dir);
					}
				}
				// fresh reader: read ALL columns in file order (the merged part
				// keeps the first part's column order)
				for (idx_t i = 0; i < part_reader->columns.size(); i++) {
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

			// Commit: move the new part into place, then delete the old
			// parts. There are no sidecars and no commit markers — the part
			// file move is the atomic switch (readers glob only visible
			// part-*.parquet files).
			string target_path = dir + "/" + part_name + ".parquet";
			if (fs.FileExists(target_path)) {
				throw IOException("Aligned table '%s' group '%s': part '%s' already exists in '%s'",
				                  bind.plan.table.name, group.manifest.group, part_name, dir);
			}
			fs.MoveFile(staged_path, target_path);

			// Delete the old parts (after the new part is in place: they are
			// already invisible to readers; failure here only leaves
			// orphaned files)
			for (auto &part : parts) {
				fs.RemoveFile(part->path);
			}

			dirs_compacted++;
				parts_after -= parts.size();
				parts_after += 1;
			}
		}

		// Bump last_txid in _table.json (row counts unchanged — compaction
		// preserves the row space)
		if (dirs_compacted > 0) {
			BumpLastTxid(fs, bind.plan, txid);
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
