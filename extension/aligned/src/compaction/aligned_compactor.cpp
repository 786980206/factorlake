#include "compaction/aligned_compactor.hpp"

#include "catalog/manifest.hpp"
#include "mutator/aligned_mutator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "io/parquet_io.hpp"

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

//! In-process transaction counter (starts at 1, increments each call). Not
//! persisted — only used for the staging directory name.
// NextTransactionId is now shared with the mutator (see aligned_mutator.hpp).

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

	// Acquire the table-level write lock (file-based mutual exclusion across
	// concurrent aligned_upsert/aligned_delete/aligned_compact invocations).
	TableWriteLock write_lock(fs, bind.plan.table_path);

	// txid for the staging directory name (in-process counter, not persisted)
	idx_t txid = NextTransactionId();
	string tmp_root = bind.plan.table_path + "/_tmp/transaction-" + to_string(txid);

	try {
		idx_t dirs_compacted = 0;
		idx_t parts_before = 0;
		idx_t parts_after = 0;

		// === Phase 1: Stage all merged parts in _tmp ===
		// All groups are staged first. If any group/partition fails during
		// staging, we clean up _tmp and the table is unchanged — no old
		// parts have been deleted and no new parts have been moved into place.

		// Collect a list of staged→target moves + old-part deletions to
		// execute in Phase 2.
		struct PendingMove {
			string staged_path;
			string target_path;
			vector<string> old_paths;
			GroupPlan *group;
			string dir;
		};
		vector<PendingMove> pending_moves;

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
					                  bind.plan.table_name, group.manifest.group, dir);
				}
				row_count += parts[i]->row_count;
			}

			// Staged new part. v6: the merged part is the only part of the
			// partition, so its self-describing name is "{idx:04d}-{rows:10d}"
			// with idx = 0 (every group merges the same partition together, so
			// cross-group indexes stay consistent). The staging path includes
			// the partition's relative path so that multiple partitions of one
			// group do not collide.
			idx_t rgs = ALIGNED_DEFAULT_RG_ROWS;
			if (row_count > 9999999999ULL) {
				throw IOException("Aligned table '%s' group '%s': merged part '%s' holds %llu rows — more than the "
				                  "self-describing name can represent (10 digits)",
				                  bind.plan.table_name, group.manifest.group, dir, row_count);
			}
			string part_name = StringUtil::Format("0000-%010llu", (unsigned long long)row_count);
			string group_rel = dir.substr(group.group_path.size());
			string staged_dir = tmp_root + "/" + group.manifest.group + group_rel;
			fs.CreateDirectoriesRecursive(staged_dir);
			string staged_path = staged_dir + "/" + part_name + ".parquet";
			// Standard aligned-extension writer options (shared with aligned_create
			// / part_rewriter — see io::CreateParquetWriter).
			auto writer = CreateParquetWriter(context, fs, staged_path, columns, col_types);
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
					                  bind.plan.table_name, group.manifest.group, dir);
				}
				for (idx_t ci = 0; ci < columns.size(); ci++) {
					if (part_reader->columns[ci].name != columns[ci]) {
						throw IOException("Aligned table '%s' group '%s': cannot compact directory '%s' — parts have "
						                  "different column sets (schema evolution within a directory)",
						                  bind.plan.table_name, group.manifest.group, dir);
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

			// Record the pending move — do NOT move into place or delete
			// old parts yet. This is the key change for cross-group atomicity:
			// if a later group fails during staging, the old parts of earlier
			// groups are still on disk and the table is unchanged.
			string target_path = dir + "/" + part_name + ".parquet";
			if (fs.FileExists(target_path)) {
				throw IOException("Aligned table '%s' group '%s': part '%s' already exists in '%s'",
				                  bind.plan.table_name, group.manifest.group, part_name, dir);
			}

			PendingMove pm;
			pm.staged_path = staged_path;
			pm.target_path = target_path;
			for (auto &part : parts) {
				pm.old_paths.push_back(part->path);
			}
			pending_moves.push_back(std::move(pm));

			dirs_compacted++;
				parts_after -= parts.size();
				parts_after += 1;
			}
		}

		// === Phase 2: Commit — move all staged parts into place, then delete old ===
		// All staging succeeded. Now we atomically switch: move every new part
		// into its target directory, then delete all old parts. If a move fails
		// mid-way, the already-moved new parts are valid (they cover the same
		// row range), but the not-yet-moved groups still have their old parts.
		// The reader's fail-fast on part-count mismatch will catch this, but
		// this is a best-effort phase — MoveFile on the same volume is atomic.
		for (auto &pm : pending_moves) {
			fs.MoveFile(pm.staged_path, pm.target_path);
		}
		for (auto &pm : pending_moves) {
			for (auto &old : pm.old_paths) {
				fs.RemoveFile(old);
			}
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
