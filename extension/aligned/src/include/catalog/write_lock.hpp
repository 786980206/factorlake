#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

//! Shared transaction ID counter (process-wide). Used by aligned_create,
//! aligned_compact, and aligned_drop for `_tmp/transaction-<id>/` staging
//! directory names and the txid return value. Not persisted.
idx_t NextTransactionId();

//! RAII file-based write lock for an AlignedTable directory. Creates a
//! `.aligned_write.lock` file in the table root; if the lock file already
//! exists, throws an error (another writer is active). The lock is released
//! by deleting the file in the destructor. This is advisory mutual exclusion
//! — it prevents concurrent writers on the same table from corrupting the
//! staging + move protocol. It is NOT a replacement for proper filesystem
//! transactions (the staging + move is already crash-safe: an interrupted
//! writer leaves only a `_tmp/` tree that readers never see).
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

//! RAII staging transaction: acquires the table write lock, mints a txid
//! (shared via NextTransactionId), creates `_tmp/transaction-<id>/`, and on
//! destruction removes the staging tree. If the scope throws (unwinding),
//! the staging tree is also removed — so a crashed writer leaves no
//! half-written parts visible to readers (readers never see `_tmp/`).
//! On successful completion (normal destruction), the staging tree is
//! also removed (the parts have already been moved into place).
class StagedTransaction {
public:
	FileSystem &fs;
	const string table_path;
	const idx_t txid;
	const string tmp_root;
	bool committed = false;

	StagedTransaction(FileSystem &fs, const string &table_path)
	    : fs(fs), table_path(table_path), txid(NextTransactionId()),
	      tmp_root(table_path + "/_tmp/transaction-" + to_string(txid)) {
		fs.CreateDirectoriesRecursive(tmp_root);
	}

	~StagedTransaction() {
		try {
			if (fs.DirectoryExists(tmp_root)) {
				fs.RemoveDirectory(tmp_root);
			}
			string tmp_parent = table_path + "/_tmp";
			if (fs.DirectoryExists(tmp_parent)) {
				fs.RemoveDirectory(tmp_parent);
			}
		} catch (...) {
			// best-effort cleanup — readers never see _tmp/
		}
	}

	StagedTransaction(const StagedTransaction &) = delete;
	StagedTransaction &operator=(const StagedTransaction &) = delete;
	StagedTransaction(StagedTransaction &&) = delete;
};

} // namespace duckdb
