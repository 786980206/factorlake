#include "compaction/aligned_compactor.hpp"

#include "catalog/manifest.hpp"
#include "mutator/aligned_mutator.hpp"
#include "io/parquet_io.hpp"

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

	auto root_it = input.named_parameters.find("root");
	const Value *root_param = (root_it != input.named_parameters.end()) ? &root_it->second : nullptr;
	string root = ResolveDataRoot(context, root_param, "aligned_compact");

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
// Helpers
//===----------------------------------------------------------------------===//

//! Check whether a partition's parts are already normalized: every part
//! (except possibly the last) has exactly ALIGNED_DEFAULT_PART_ROWS, and the
//! last has ≤ ALIGNED_DEFAULT_PART_ROWS. 0-row parts are treated as already
//! fine (they keep the index consecutive).
static bool IsAlreadyNormalized(const vector<const PartInfo *> &parts) {
	for (idx_t i = 0; i < parts.size(); i++) {
		idx_t rc = parts[i]->row_count;
		if (rc == 0) {
			continue; // 0-row placeholder — leave as-is
		}
		if (i < parts.size() - 1) {
			if (rc != ALIGNED_DEFAULT_PART_ROWS) {
				return false;
			}
		} else {
			// Last part: can be ≤ threshold
			if (rc > ALIGNED_DEFAULT_PART_ROWS) {
				return false;
			}
		}
	}
	return true;
}

//! Read all rows from a set of parts into a writer, flushing at RG boundaries.
//! The parts are read in order (position alignment preserved). Returns the
//! total row count written.
static idx_t MergePartsToWriter(ClientContext &context, FileSystem &fs,
                                const vector<const PartInfo *> &parts,
                                const vector<string> &columns,
                                const vector<LogicalType> &col_types,
                                const string &out_path) {
	idx_t rgs = ALIGNED_DEFAULT_RG_ROWS;
	auto writer = CreateParquetWriter(context, fs, out_path, columns, col_types);
	unique_ptr<ParquetWriteTransformData> transform;
	auto buffer = make_uniq<ColumnDataCollection>(context, col_types);
	ColumnDataAppendState append_state;
	buffer->InitializeAppend(append_state);

	idx_t total_rows = 0;
	for (auto &part : parts) {
		if (part->row_count == 0) {
			continue; // skip 0-row placeholder parts
		}
		auto part_reader =
		    make_uniq<ParquetReader>(context, OpenFileInfo(part->path), ParquetOptions(context));
		// Validate schema matches the reference
		if (part_reader->columns.size() != columns.size()) {
			throw IOException("Aligned table: cannot compact — parts have different column counts");
		}
		for (idx_t ci = 0; ci < columns.size(); ci++) {
			if (part_reader->columns[ci].name != columns[ci]) {
				throw IOException("Aligned table: cannot compact — parts have different column sets");
			}
		}
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
		total_rows += part->row_count;
	}
	writer->Flush(*buffer, transform);
	writer->Finalize();
	return total_rows;
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
	// concurrent mutator / compactor invocations).
	TableWriteLock write_lock(fs, bind.plan.table_path);

	// txid for the staging directory name (in-process counter, not persisted)
	idx_t txid = NextTransactionId();
	string tmp_root = bind.plan.table_path + "/_tmp/transaction-" + to_string(txid);

	try {
		idx_t dirs_compacted = 0;
		idx_t parts_before = 0;
		idx_t parts_after = 0;

		// === Phase 1: Stage all normalized parts in _tmp ===
		// All groups are staged first. If any group/partition fails during
		// staging, we clean up _tmp and the table is unchanged — no old
		// parts have been deleted and no new parts have been moved into place.

		struct PendingMove {
			string staged_path;
			string target_path;
			vector<string> old_paths;
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

				// --- Check if normalization is needed ---
				// Skip if already normalized (all parts except last == threshold,
				// last <= threshold; 0-row parts left as-is).
				if (IsAlreadyNormalized(parts)) {
					parts_after += parts.size();
					continue;
				}

				// Count non-zero rows
				idx_t total_rows = 0;
				for (auto &p : parts) {
					total_rows += p->row_count;
				}

				// Compute the target number of output parts
				// (ceil(total_rows / threshold), minimum 1 even for 0 rows)
				idx_t num_parts = (total_rows + ALIGNED_DEFAULT_PART_ROWS - 1) / ALIGNED_DEFAULT_PART_ROWS;
				if (num_parts == 0) {
					num_parts = 1; // all 0-row → keep one 0-row part
				}

				// Read the reference schema from the first non-zero part
				vector<string> columns;
				vector<LogicalType> col_types;
				const PartInfo *ref_part = nullptr;
				for (auto &p : parts) {
					if (p->row_count > 0) {
						ref_part = p;
						break;
					}
				}
				if (!ref_part) {
					// All parts are 0-row — just keep them as-is
					parts_after += parts.size();
					continue;
				}
				auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(ref_part->path), ParquetOptions(context));
				for (auto &rc : reader->columns) {
					columns.push_back(rc.name);
					col_types.push_back(rc.type);
				}

				// Validate contiguity (parts must be contiguous in row space)
				for (idx_t i = 1; i < parts.size(); i++) {
					if (parts[i]->start_row != parts[i - 1]->start_row + parts[i - 1]->row_count) {
						throw IOException("Aligned table '%s' group '%s': cannot compact directory '%s' — parts are not "
						                  "contiguous (alignment violation)",
						                  bind.plan.table_name, group.manifest.group, dir);
					}
				}

				// Compute the split points: which rows go into which output part
				// Part 0: rows [0, threshold)
				// Part 1: rows [threshold, 2*threshold)
				// ...
				// Last part: rows [(num_parts-1)*threshold, total_rows)
				// Each output part is a SEPARATE parquet file.

				string group_rel = dir.substr(group.group_path.size());
				string staged_dir = tmp_root + "/" + group.manifest.group + group_rel;
				fs.CreateDirectoriesRecursive(staged_dir);

				// Strategy: read all parts into a single stream, write out
				// split parts. We read row-by-row via chunk scanning and
				// accumulate into per-output-part buffers.
				// For efficiency: if num_parts == 1, just merge all into one.
				// If num_parts > 1, read the stream and flush at threshold boundaries.

				vector<string> staged_paths;
				vector<idx_t> part_row_counts;

				if (num_parts == 1) {
					// Single output part — merge all source parts into one file
					string staged_path = staged_dir + "/" + FormatPartName(0, total_rows);
					MergePartsToWriter(context, fs, parts, columns, col_types, staged_path);
					staged_paths.push_back(staged_path);
					part_row_counts.push_back(total_rows);
				} else {
					// Multiple output parts — read the stream and split at
					// ALIGNED_DEFAULT_PART_ROWS boundaries.
					idx_t rgs = ALIGNED_DEFAULT_RG_ROWS;
					// One writer per output part
					unique_ptr<ParquetWriter> writer;
					unique_ptr<ParquetWriteTransformData> transform;
					auto buffer = make_uniq<ColumnDataCollection>(context, col_types);
					ColumnDataAppendState append_state;
					buffer->InitializeAppend(append_state);

					idx_t rows_written = 0;
					idx_t current_part = 0;

					auto flush_current_part = [&]() {
						if (writer) {
							writer->Flush(*buffer, transform);
							writer->Finalize();
							writer.reset();
							buffer->Reset();
							buffer->InitializeAppend(append_state);
						}
					};

					for (auto &part : parts) {
						if (part->row_count == 0) {
							continue;
						}
						auto part_reader =
						    make_uniq<ParquetReader>(context, OpenFileInfo(part->path), ParquetOptions(context));
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

							// Append chunk to buffer, checking for part boundary
							idx_t chunk_rows = chunk.size();
							idx_t chunk_consumed = 0;
							while (chunk_consumed < chunk_rows) {
								idx_t remaining_in_chunk = chunk_rows - chunk_consumed;
								idx_t rows_in_current_part = rows_written - current_part * ALIGNED_DEFAULT_PART_ROWS;
								idx_t capacity_in_current_part =
								    ALIGNED_DEFAULT_PART_ROWS - rows_in_current_part;
								idx_t to_take = MinValue<idx_t>(remaining_in_chunk, capacity_in_current_part);

								if (to_take > 0) {
									// Open writer for this part if not yet open
									if (!writer) {
										idx_t this_part_rows = (current_part == num_parts - 1)
										                           ? (total_rows - current_part * ALIGNED_DEFAULT_PART_ROWS)
										                           : ALIGNED_DEFAULT_PART_ROWS;
										string staged_path = staged_dir + "/" + FormatPartName(current_part, this_part_rows);
										writer = CreateParquetWriter(context, fs, staged_path, columns, col_types);
										staged_paths.push_back(staged_path);
										part_row_counts.push_back(this_part_rows);
									}
									// Append a slice of the chunk
									if (to_take == chunk_rows) {
										buffer->Append(append_state, chunk);
									} else {
										// Slice the chunk: use a SelectionVector
										SelectionVector sel(to_take);
										for (idx_t s = 0; s < to_take; s++) {
											sel.set_index(s, chunk_consumed + s);
										}
										DataChunk sliced;
										sliced.Initialize(context, col_types);
										sliced.Slice(chunk, sel, to_take);
										buffer->Append(append_state, sliced);
									}
									if (buffer->Count() >= rgs) {
										writer->Flush(*buffer, transform);
										buffer->Reset();
										buffer->InitializeAppend(append_state);
									}
									rows_written += to_take;
									chunk_consumed += to_take;
								}

								// Check if we've filled the current part
								rows_in_current_part = rows_written - current_part * ALIGNED_DEFAULT_PART_ROWS;
								if (rows_in_current_part >= ALIGNED_DEFAULT_PART_ROWS &&
								    current_part < num_parts - 1) {
									flush_current_part();
									current_part++;
								}
							}
						}
					}
					// Flush the last part
					flush_current_part();
				}

				// Record pending moves
				for (idx_t pi = 0; pi < staged_paths.size(); pi++) {
					PendingMove pm;
					pm.staged_path = staged_paths[pi];
					string part_name = FormatPartName(pi, part_row_counts[pi]);
					pm.target_path = dir + "/" + part_name;
					pending_moves.push_back(std::move(pm));
				}
				// All old parts in this directory will be replaced
				for (auto &pm : pending_moves) {
					if (pm.target_path.rfind(dir, 0) == 0) {
						// This is from this dir — already handled below
					}
				}

				// Collect old paths for this directory
				// (We'll delete them in Phase 2)
				dirs_compacted++;
				parts_after += num_parts;
			}
		}

		// Collect old paths per directory (all old parts in compacted dirs)
		// We need to know which directories were compacted to delete old files.
		std::set<string> compacted_dirs;
		for (auto &pm : pending_moves) {
			auto slash = pm.target_path.find_last_of("/\\");
			string dir = slash == string::npos ? "" : pm.target_path.substr(0, slash);
			compacted_dirs.insert(dir);
		}
		// For each compacted dir, collect all old part paths
		vector<string> old_files_to_delete;
		for (auto &group : bind.plan.groups) {
			for (auto &part : group.parts) {
				auto slash = part.path.find_last_of("/\\");
				string dir = slash == string::npos ? "" : part.path.substr(0, slash);
				if (compacted_dirs.count(dir)) {
					old_files_to_delete.push_back(part.path);
				}
			}
		}

		// === Phase 2: Commit — move all staged parts into place, then delete old ===
		for (auto &pm : pending_moves) {
			auto slash = pm.target_path.find_last_of("/\\");
			fs.CreateDirectoriesRecursive(pm.target_path.substr(0, slash));
			fs.MoveFile(pm.staged_path, pm.target_path);
		}
		// Delete old files (but skip any that happen to have the same name
		// as a new file — those were overwritten by the move)
		std::set<string> new_paths;
		for (auto &pm : pending_moves) {
			new_paths.insert(pm.target_path);
		}
		for (auto &old : old_files_to_delete) {
			if (new_paths.count(old)) {
				continue; // overwritten by move
			}
			fs.RemoveFile(old);
		}

		// Cleanup the staging tree
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
