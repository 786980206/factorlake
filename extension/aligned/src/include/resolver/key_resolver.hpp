#pragma once

#include "duckdb.hpp"
#include "catalog/manifest.hpp"

#include <map>

namespace duckdb {

//! The resolved location of one primary key (date, symbol) within the index
//! group's row space (v7 primary-key contract).
struct KeyLocation {
	bool found = false; // key exists in the table (-> update/delete instead of insert)
	string partition_key; // "date=2026-08-17"; "" for unpartitioned tables; for a
	                      // key whose date lies outside all partitions this is the
	                      // NEW partition's key (a fresh part will be created)
	idx_t part_index = 0; // partition-local part index (PartInfo.partition_index);
	                      // 0 for a new partition (fresh part "0000-..."); for an
	                      // append at the partition end this is the NEW part's index
	idx_t part_local_row = 0; // row within the part (0-based). found: the key's
	                          // row; not found: the insertion position — the first
	                          // row of the part whose symbol is >= the key's symbol
	                          // (== part row count when appending at the part end)
	bool append_new_part = false; // not found and the key sorts after every symbol
	                              // of the partition: append at the partition end
	                              // creates a NEW part (index = part_index) instead
	                              // of growing the last part (keeps part sizes
	                              // bounded and allows schema evolution on appends)
};

//! Resolves primary keys (date, symbol) against the index group of a table.
//! Preconditions (fail-fast on violation):
//!  - within a partition, rows are ordered by symbol strictly ascending
//!    (v7 writer-side sort contract; also implies symbol uniqueness)
//! The symbol column is compared with Value ordering (VARCHAR: byte order).
//! Partitions are identified by evaluating the group's partition template
//! against the key's date (date part for TIMESTAMP sources). A key whose date
//! maps to no existing partition resolves to a new-partition location
//! (found=false, part_index=0, part_local_row=0).
class KeyResolver {
public:
	KeyResolver(ClientContext &context, const TablePlan &plan);

	//! Resolves one key. `date_value` is the raw DATE value (callers with a
	//! TIMESTAMP source pass Timestamp::GetDate first); `symbol_value` is the
	//! key's symbol column value.
	KeyLocation Resolve(date_t date_value, const Value &symbol_value);

private:
	ClientContext &context;
	const TablePlan &plan;
	const GroupPlan *index_group = nullptr;
	vector<PartitionTemplate> templates;

	// Per-partition cached symbol columns (lazy, whole partition in memory:
	// the index group is small; the cache makes batch resolution read each
	// partition's symbol column only once).
	struct PartitionCache {
		vector<Value> symbols;
		bool loaded = false;
	};
	std::map<string, PartitionCache> cache;

	//! Reads + validates the symbol column of every part of a partition into
	//! the cache (strictly ascending, fail-fast on violation).
	void LoadPartition(const GroupPartition &partition);
};

} // namespace duckdb