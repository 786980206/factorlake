#include "compaction/aligned_compactor.hpp"

#include "catalog/manifest.hpp"
#include "catalog/write_lock.hpp"
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
#include <future>
#include <thread>
#include <atomic>
#include <mutex>

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
//! last has ≤ ALIGNED_DEFAULT_PART_ROWS. Any 0-row placeholder part means
//! the partition is NOT normalized — 0-row parts should be absorbed by
//! compaction (per AGENTS.md: "0-row placeholder parts are merged absorbed").
static bool IsAlreadyNormalized(const vector<const PartInfo *> &parts) {
	for (idx_t i = 0; i < parts.size(); i++) {
		idx_t rc = parts[i]->row_count;
		if (rc == 0) {
			// 0-row part present → not normalized, should be absorbed.
			return false;
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
			if (async_type == AsyncResultType::FINISHED) {
				break;
			}
			if (async_type == AsyncResultType::BLOCKED) {
				// Async not ready (e.g. object storage) — retry.
				continue;
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
// Parallel Phase 1 staging
//===----------------------------------------------------------------------===//

// One unit of staging work, collected up-front before any thread starts.
// All pointer fields point into the bind data's plan, which is immutable
// during Phase 1 (only read by workers), so sharing them across threads is
// safe. Each worker creates its own readers/writers/chunks/buffers.
struct StagingJob {
	string dir;                            // partition directory (source of parts)
	vector<const PartInfo *> parts;        // pointers into plan.groups[g].parts
	vector<string> columns;               // group schema column names
	vector<LogicalType> col_types;        // group schema column types
	string group_name;                    // for error messages
	string group_rel;                     // dir suffix past the group path
	string staged_dir;                     // <tmp_root>/<group>/<rel dir>
	idx_t group_index;                     // index into plan.groups (for old-path collection)
};

// Result of a single staging job: the staged/target paths (one per output
// part) plus the old source part paths to delete in Phase 2. Counters
// (parts_before, parts_after, dirs_compacted) are also returned per job so
// the main thread can aggregate them.
struct StagingResult {
	vector<string> staged_paths;
	vector<string> target_paths;
	vector<string> old_paths;     // source parts in this dir (to delete in Phase 2)
	idx_t parts_before_count = 0; // #source parts in this dir
	idx_t parts_after_count = 0;  // #output parts produced (0 if skipped)
	bool compacted = false;       // whether this dir was actually rewritten
	std::exception_ptr ep;        // set if the worker threw
};

//! Stage one directory's parts into _tmp, normalized to
//! ALIGNED_DEFAULT_PART_ROWS boundaries. Each worker owns its own
//! ParquetReader / ParquetWriter / ParquetReaderScanState / DataChunk /
//! ColumnDataCollection (never shared across threads). ClientContext and
//! FileSystem are documented as thread-safe for creating readers/writers.
static StagingResult StageOneJob(ClientContext &context, FileSystem &fs, const StagingJob &job) {
	StagingResult result;
	result.parts_before_count = job.parts.size();

	// Collect old source part paths for Phase 2 deletion.
	for (auto &p : job.parts) {
		result.old_paths.push_back(p->path);
	}

	// Skip if already normalized (all parts except last == threshold,
	// last <= threshold; 0-row parts force a rewrite to absorb them).
	if (IsAlreadyNormalized(job.parts)) {
		result.parts_after_count = job.parts.size();
		return result;
	}

	// Count non-zero rows.
	idx_t total_rows = 0;
	for (auto &p : job.parts) {
		total_rows += p->row_count;
	}

	// Target number of output parts (ceil(total_rows / threshold), min 1).
	idx_t num_parts = (total_rows + ALIGNED_DEFAULT_PART_ROWS - 1) / ALIGNED_DEFAULT_PART_ROWS;
	if (num_parts == 0) {
		num_parts = 1; // all 0-row → keep one 0-row part
	}

	// Find the first non-zero part as the schema reference. If all parts are
	// 0-row, keep them as-is (nothing to rewrite).
	const PartInfo *ref_part = nullptr;
	for (auto &p : job.parts) {
		if (p->row_count > 0) {
			ref_part = p;
			break;
		}
	}
	if (!ref_part) {
		result.parts_after_count = job.parts.size();
		return result;
	}

	// Validate contiguity (parts must be contiguous in row space).
	for (idx_t i = 1; i < job.parts.size(); i++) {
		if (job.parts[i]->start_row != job.parts[i - 1]->start_row + job.parts[i - 1]->row_count) {
			throw IOException("Aligned table: group '%s': cannot compact directory '%s' — parts are not "
			                  "contiguous (alignment violation)",
			                  job.group_name, job.dir);
		}
	}

	fs.CreateDirectoriesRecursive(job.staged_dir);

	vector<string> staged_paths;
	vector<idx_t> part_row_counts;

	if (num_parts == 1) {
		// Single output part — merge all source parts into one file.
		string staged_path = job.staged_dir + "/" + FormatPartName(0, total_rows);
		MergePartsToWriter(context, fs, job.parts, job.columns, job.col_types, staged_path);
		staged_paths.push_back(staged_path);
		part_row_counts.push_back(total_rows);
	} else {
		// Multiple output parts — read the stream and split at
		// ALIGNED_DEFAULT_PART_ROWS boundaries. One writer per output part,
		// all owned by this worker.
		idx_t rgs = ALIGNED_DEFAULT_RG_ROWS;
		unique_ptr<ParquetWriter> writer;
		unique_ptr<ParquetWriteTransformData> transform;
		auto buffer = make_uniq<ColumnDataCollection>(context, job.col_types);
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

		for (auto &part : job.parts) {
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
			chunk.Initialize(context, job.col_types);
			while (true) {
				auto res = part_reader->Scan(context, scan_state, chunk);
				auto async_type = res.GetResultType();
				if (async_type == AsyncResultType::FINISHED) {
					break;
				}
				if (async_type == AsyncResultType::BLOCKED) {
					// Async not ready (e.g. object storage) — retry.
					continue;
				}
				if (chunk.size() == 0) {
					continue;
				}

				// Append chunk to buffer, checking for part boundary.
				idx_t chunk_rows = chunk.size();
				idx_t chunk_consumed = 0;
				while (chunk_consumed < chunk_rows) {
					idx_t remaining_in_chunk = chunk_rows - chunk_consumed;
					idx_t rows_in_current_part = rows_written - current_part * ALIGNED_DEFAULT_PART_ROWS;
					idx_t capacity_in_current_part =
					    ALIGNED_DEFAULT_PART_ROWS - rows_in_current_part;
					idx_t to_take = MinValue<idx_t>(remaining_in_chunk, capacity_in_current_part);

					if (to_take > 0) {
						// Open writer for this part if not yet open.
						if (!writer) {
							idx_t this_part_rows = (current_part == num_parts - 1)
							                           ? (total_rows - current_part * ALIGNED_DEFAULT_PART_ROWS)
							                           : ALIGNED_DEFAULT_PART_ROWS;
							string staged_path =
							    job.staged_dir + "/" + FormatPartName(current_part, this_part_rows);
							writer = CreateParquetWriter(context, fs, staged_path, job.columns, job.col_types);
							staged_paths.push_back(staged_path);
							part_row_counts.push_back(this_part_rows);
						}
						// Append a slice of the chunk.
						if (to_take == chunk_rows) {
							buffer->Append(append_state, chunk);
						} else {
							SelectionVector sel(to_take);
							for (idx_t s = 0; s < to_take; s++) {
								sel.set_index(s, chunk_consumed + s);
							}
							DataChunk sliced;
							sliced.Initialize(context, job.col_types);
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

					// Check if we've filled the current part.
					rows_in_current_part = rows_written - current_part * ALIGNED_DEFAULT_PART_ROWS;
					if (rows_in_current_part >= ALIGNED_DEFAULT_PART_ROWS &&
					    current_part < num_parts - 1) {
						flush_current_part();
						current_part++;
					}
				}
			}
		}
		// Flush the last part.
		flush_current_part();
	}

	// Record target paths (one per output part).
	for (idx_t pi = 0; pi < staged_paths.size(); pi++) {
		string part_name = FormatPartName(pi, part_row_counts[pi]);
		result.target_paths.push_back(job.dir + "/" + part_name);
	}
	result.staged_paths = std::move(staged_paths);
	result.compacted = true;
	result.parts_after_count = num_parts;
	return result;
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
	// concurrent mutator / compactor invocations). The lock and the staged
	// transaction are held for both phases — no additional locking is needed
	// inside the parallel workers.
	TableWriteLock write_lock(fs, bind.plan.table_path);
	StagedTransaction txn(fs, bind.plan.table_path);
	const string &tmp_root = txn.tmp_root;

	// === Phase 1: Stage all normalized parts in _tmp (parallel) ===
	//
	// First collect every staging job up-front. Each job is independent: it
	// reads its own source parts and writes to its own staged directory under
	// _tmp. The jobs share no mutable state, so they can run concurrently.
	// ClientContext and FileSystem are thread-safe for creating readers and
	// writers; each worker owns its own ParquetReaderScanState / DataChunk /
	// ColumnDataCollection / ParquetWriter.

	vector<StagingJob> jobs;
	for (auto &group : bind.plan.groups) {
		// Group the parts by partition directory.
		std::map<string, vector<const PartInfo *>> by_dir;
		for (auto &part : group.parts) {
			auto slash = part.path.find_last_of("/\\");
			string dir = slash == string::npos ? "" : part.path.substr(0, slash);
			by_dir[dir].push_back(&part);
		}

		for (auto &kv : by_dir) {
			const auto &dir = kv.first;
			auto &parts = kv.second;

			// Read the reference schema from the first non-zero part once,
			// up-front (in the main thread), so workers don't race on a shared
			// reader. If there is no non-zero part, the job is a no-op skip.
			vector<string> columns;
			vector<LogicalType> col_types;
			const PartInfo *ref_part = nullptr;
			for (auto &p : parts) {
				if (p->row_count > 0) {
					ref_part = p;
					break;
				}
			}
			if (ref_part) {
				auto reader =
				    make_uniq<ParquetReader>(context, OpenFileInfo(ref_part->path), ParquetOptions(context));
				for (auto &rc : reader->columns) {
					columns.push_back(rc.name);
					col_types.push_back(rc.type);
				}
			}

			StagingJob job;
			job.dir = dir;
			job.parts = parts;
			job.columns = std::move(columns);
			job.col_types = std::move(col_types);
			job.group_name = group.manifest.group;
			if (!dir.empty() && dir.size() > group.group_path.size() &&
			    dir.compare(0, group.group_path.size(), group.group_path) == 0) {
				job.group_rel = dir.substr(group.group_path.size());
			} else {
				job.group_rel = "";
			}
			job.staged_dir = tmp_root + "/" + group.manifest.group + job.group_rel;
			jobs.push_back(std::move(job));
		}
	}

	// Run the jobs in parallel. Use a simple work queue dispatched to a
	// bounded number of worker threads (capped to the job count and to a
	// sane upper bound to avoid oversubscription on huge tables).
	const idx_t num_jobs = jobs.size();
	const unsigned hw = std::thread::hardware_concurrency();
	const idx_t num_threads = num_jobs == 0 ? 1
	    : MinValue<idx_t>(num_jobs, MaxValue<idx_t>(static_cast<idx_t>(hw ? hw : 4), static_cast<idx_t>(1)));

	vector<StagingResult> results(num_jobs);
	std::atomic<idx_t> next_job{0};

	auto worker = [&]() {
		for (;;) {
			idx_t i = next_job.fetch_add(1, std::memory_order_relaxed);
			if (i >= num_jobs) {
				return;
			}
			try {
				results[i] = StageOneJob(context, fs, jobs[i]);
			} catch (...) {
				results[i].ep = std::current_exception();
			}
		}
	};

	vector<std::thread> threads;
	threads.reserve(num_threads);
	for (idx_t t = 0; t < num_threads; t++) {
		threads.emplace_back(worker);
	}
	for (auto &th : threads) {
		th.join();
	}

	// Propagate the first worker exception (if any) in the main thread.
	idx_t dirs_compacted = 0;
	idx_t parts_before = 0;
	idx_t parts_after = 0;
	struct PendingMove {
		string staged_path;
		string target_path;
	};
	vector<PendingMove> pending_moves;
	vector<string> old_files_to_delete;

	for (auto &r : results) {
		if (r.ep) {
			std::rethrow_exception(r.ep);
		}
		parts_before += r.parts_before_count;
		parts_after += r.parts_after_count;
		if (r.compacted) {
			dirs_compacted++;
			for (idx_t pi = 0; pi < r.staged_paths.size(); pi++) {
				PendingMove pm;
				pm.staged_path = r.staged_paths[pi];
				pm.target_path = r.target_paths[pi];
				pending_moves.push_back(std::move(pm));
			}
			for (auto &old : r.old_paths) {
				old_files_to_delete.push_back(old);
			}
		}
	}

	// === Phase 2: Commit — move all staged parts into place, then delete old
	// (kept sequential; only Phase 1 was parallelized) ===
	for (auto &pm : pending_moves) {
		auto slash = pm.target_path.find_last_of("/\\");
		fs.CreateDirectoriesRecursive(pm.target_path.substr(0, slash));
		fs.MoveFile(pm.staged_path, pm.target_path);
	}
	// Delete old files (but skip any that happen to have the same name as a
	// new file — those were overwritten by the move).
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

	// RAII StagedTransaction destructor cleans up _tmp/transaction-<id>/
	// and the _tmp/ parent directory.

	gstate.dirs_compacted = dirs_compacted;
	gstate.parts_before = parts_before;
	gstate.parts_after = parts_after;
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.dirs_compacted)));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(gstate.parts_before)));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(gstate.parts_after)));
}

} // namespace duckdb
