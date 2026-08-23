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
	KeyLocation Resolve(date_t date_value, const Value &symbol_value);

private:
	ClientContext &context;
	const TablePlan &plan;
	const GroupPlan *index_group = nullptr;
	vector<PartitionTemplate> templates;

	struct PartitionCache {
		vector<Value> symbols;
		vector<date_t> dates;
		bool loaded = false;
		vector<Value> part_sym_min;
		vector<Value> part_sym_max;
		bool boundary_loaded = false;
		vector<vector<Value>> part_symbols;
		vector<vector<date_t>> part_dates;
		vector<bool> part_loaded;
	};
	std::map<string, PartitionCache> cache;

	void LoadPartition(const GroupPartition &partition);
	void LoadPartitionBoundaries(const GroupPartition &partition);
	void LoadSinglePart(const GroupPartition &partition, idx_t part_k);
};

} // namespace duckdb
