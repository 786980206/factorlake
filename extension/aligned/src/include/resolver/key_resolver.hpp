#pragma once

#include "duckdb.hpp"
#include "catalog/manifest.hpp"

#include <map>

namespace duckdb {

struct KeyLocation {
	bool found = false;
	string partition_key;
	idx_t part_index = 0;
	idx_t part_local_row = 0;
	bool append_new_part = false;
	bool append_to_last = false;
};

class KeyResolver {
public:
	KeyResolver(ClientContext &context, const TablePlan &plan);
	// date_value is an int64_t that holds either a date_t (for DATE columns)
	// or a timestamp_t (for TIMESTAMP columns). For TIMESTAMP, the full
	// timestamp value is used as the key — NOT truncated to date — so that
	// intraday rows (e.g. minute bars) with the same (symbol, date) but
	// different times are distinct keys.
	KeyLocation Resolve(int64_t date_value, const Value &symbol_value);

private:
	ClientContext &context;
	const TablePlan &plan;
	const GroupPlan *index_group = nullptr;
	vector<PartitionTemplate> templates;
	// True when the partition source column is TIMESTAMP (vs DATE).
	bool is_timestamp = false;

	struct PartitionCache {
		vector<Value> symbols;
		vector<int64_t> dates; // date_t or timestamp_t value
		bool loaded = false;
		vector<Value> part_sym_min;
		vector<Value> part_sym_max;
		bool boundary_loaded = false;
		vector<vector<Value>> part_symbols;
		vector<vector<int64_t>> part_dates; // date_t or timestamp_t value
		vector<bool> part_loaded;
	};
	std::map<string, PartitionCache> cache;

	void LoadPartition(const GroupPartition &partition);
	void LoadPartitionBoundaries(const GroupPartition &partition);
	void LoadSinglePart(const GroupPartition &partition, idx_t part_k);
	// Converts the raw int64_t key value to a date_t for partition template
	// evaluation. For TIMESTAMP columns, extracts the date part.
	date_t ToDate(int64_t value) const;
};

} // namespace duckdb
