#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/table_function.hpp"
#include "catalog/manifest.hpp"

#include <map>

namespace duckdb {

//! RAII file-based write lock for an AlignedTable directory. Creates a
//! `.aligned_write.lock` file in the table root; if the lock file already
//! exists, throws an error (another writer is active). The lock is released
//! by deleting the file in the destructor. This is advisory mutual exclusion
//! — it prevents concurrent aligned_upsert/aligned_delete/aligned_compact on
//! the same table from corrupting the staging + move protocol. It is NOT a
//! replacement for proper filesystem transactions (the staging + move is
//! already crash-safe: an interrupted writer leaves only a `_tmp/` tree that
//! readers never see).
class TableWriteLock {
public:
	TableWriteLock(FileSystem &fs, const string &table_path) : fs(fs), lock_path(table_path + "/.aligned_write.lock") {
		// Ensure the table directory exists (first write of an empty table
		// may not have created it yet).
		if (!fs.DirectoryExists(table_path)) {
			fs.CreateDirectoriesRecursive(table_path);
		}
		if (fs.FileExists(lock_path)) {
			throw IOException("Aligned table '%s': another write is in progress (lock file exists: %s). "
			                  "Retry after the current write completes or remove the stale lock file if the "
			                  "previous writer crashed.",
			                  table_path, lock_path);
		}
		auto handle = fs.OpenFile(lock_path, FileFlags::FILE_FLAGS_FILE_CREATE | FileFlags::FILE_FLAGS_WRITE);
		const char *msg = "locked\n";
		handle->Write(const_cast<char *>(msg), 7);
		handle->Close();
		locked = true;
	}
	~TableWriteLock() {
		if (locked) {
			try {
				fs.RemoveFile(lock_path);
			} catch (...) {
				// best-effort: a stale lock is harmless (next writer clears it)
			}
		}
	}
	TableWriteLock(const TableWriteLock &) = delete;
	TableWriteLock &operator=(const TableWriteLock &) = delete;
	TableWriteLock(TableWriteLock &&other) noexcept
	    : fs(other.fs), lock_path(std::move(other.lock_path)), locked(other.locked) {
		other.locked = false;
	}

private:
	FileSystem &fs;
	string lock_path;
	bool locked = false;
};

//! One target part of a mutation: either an existing part to rewrite or a
//! fresh part for a new partition.
struct MutateTarget {
	const PartInfo *part = nullptr; // nullptr: fresh part (new partition)
	string partition_key; // "date=2026-08-17"; "" for unpartitioned tables
	idx_t part_index = 0; // partition-local part index (rewritten: the old
	                      // part's index; fresh: 0)
	string staged_path;   // staged output path (set at execution time)
	idx_t new_row_count = 0; // result of RewritePart
	// Insert rows: ColumnDataCollection of [BIGINT pos, ...mapped columns].
	// Positions are part-local (the first old row whose symbol >= the key's
	// symbol; sequential for fresh parts), strictly ascending.
	unique_ptr<ColumnDataCollection> insert_buffer;
	// Update rows: ColumnDataCollection of [BIGINT row, ...mapped columns].
	unique_ptr<ColumnDataCollection> update_buffer;
	// Delete rows (ascending part-local rows; sorted at execution time).
	vector<idx_t> delete_rows;
	// The group's mapped columns (empty = NULL-row insert target).
	vector<string> mapped_names;
	vector<LogicalType> mapped_types;
	// Output schema: an existing part's own footer columns (read at target
	// creation) or the mapping columns for a fresh part.
	vector<string> out_names;
	vector<LogicalType> out_types;
	// Rows the target gains in this mutation (inserts + deletes, symmetric
	// across groups): a rewritten part's new row count = old - deletes +
	// inserts.
	idx_t inserts_count = 0;
	idx_t deletes_count = 0;
	// Delete-emptied single-part partition: the part is not rewritten; the
	// whole partition is removed from every group instead.
	bool removed = false;
	// Emptied part that is the group's HIGHEST index in its partition: the
	// part file is removed outright (remaining indexes stay consecutive);
	// unlike `removed` the partition directory itself survives.
	bool remove_part = false;
	// Synthesized part (UPDATE on keys that exist in index, but this group
	// has never seen that partition — e.g. M1 columns inserted first, M2
	// later): the part mirrors the index partition with R_i all-NULL rows;
	// keyed rows carry the mapped values. Filled after the key loop.
	bool synth = false;
	idx_t synth_rows = 0;
	std::map<idx_t, vector<Value>> synth_values;
	// Fresh-part insert position counter (positions are assigned sequentially
	// in sorted-key order).
	idx_t insert_next = 0;
	// Append states for the buffers (the collection does not own them).
	ColumnDataAppendState insert_append;
	ColumnDataAppendState update_append;
};

//! Shared bind data of aligned_upsert / aligned_delete.
struct MutateBindData : public TableFunctionData {
	string table_name;
	string source_path; // source parquet (upsert: data; delete: keys)
	TablePlan plan;
	bool is_delete = false;
	// Per plan-group mapping (upsert): the group's mapped source columns.
	struct GroupMapping {
		vector<string> col_names;
		vector<LogicalType> col_types;
		vector<idx_t> src_pos; // position in the needed columns
	};
	vector<GroupMapping> group_mapping; // aligned with plan.groups
	vector<string> needed_names;        // all needed source columns
	idx_t source_rows = 0;
	// Primary key columns (empty table: derived from the index mapping).
	string date_col;
	string symbol_col;
	bool empty_table = false; // index group has no parts (first write)
	// Function output schema.
	vector<LogicalType> types;
	vector<string> names;
};

struct MutateGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t rows_inserted = 0;
	idx_t rows_updated = 0;
	idx_t rows_deleted = 0;
	idx_t parts_rewritten = 0;
	idx_t parts_removed = 0;
	idx_t txid = 0;
};

//! aligned_upsert(table_name, source_path, mapping, root=...)
//! v7: upsert rows into an AlignedTable from a source parquet file.
//!  - mapping: "group:col1,col2;group2:col3,..." — which source columns each
//!    column group receives (the index group's mapping must include the
//!    primary key columns (date, symbol); on an empty table the first two
//!    index mapping columns ARE the primary key)
//!  - rows are placed by primary key (date, symbol): a key that exists is
//!    UPDATED (only the mapped columns are overwritten, everything else keeps
//!    its value); a new key is INSERTED at its sorted position (partition by
//!    date, symbol ascending within a partition — v7 sort contract)
//!  - partial writes: groups not in the mapping receive NULL rows (when their
//!    part exists) or nothing (when the partition/part is missing — the
//!    reader NULL-fills); a new partition creates fresh parts only in mapped
//!    groups
//!  - all affected parts are rewritten atomically under
//!    <table>/_tmp/transaction-<txid>/ and committed by moving parts into place
//! Returns one row: (rows_inserted, rows_updated, parts_rewritten, txid).
unique_ptr<FunctionData> AlignedUpsertBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names);

//! aligned_delete(table_name, keys_source, root=...)
//! v7: deletes rows by primary key from an AlignedTable. The keys source is a
//! parquet file with the (date, symbol) columns (matched by name). Deleting a
//! non-existent key is a no-op (idempotent). Every group with the affected
//! partition loses the same rows (row counts must stay aligned). Deleting all
//! rows of a multi-part partition is rejected; deleting the last part of a
//! single-part partition removes the partition from every group.
//! Returns one row: (rows_deleted, parts_rewritten, txid).
unique_ptr<FunctionData> AlignedDeleteBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> MutateInitGlobal(ClientContext &context, TableFunctionInitInput &input);

void AlignedUpsertFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

void AlignedDeleteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb