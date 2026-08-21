#include "resolver/key_resolver.hpp"

#include "resolver/partition_resolver.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

KeyResolver::KeyResolver(ClientContext &context_p, const TablePlan &plan_p) : context(context_p), plan(plan_p) {
	// The index group is the first plan group (BuildTablePlan inserts it at
	// position 0). An empty table (no index parts) cannot resolve keys — the
	// caller must handle the first-write case separately.
	if (plan.groups.empty()) {
		throw IOException("Aligned table: no column groups to resolve keys against");
	}
	index_group = &plan.groups[0];
	if (!StringUtil::CIEquals(index_group->manifest.group, "index")) {
		throw IOException("Aligned table: internal error — the index group is not the first plan group");
	}
	// The partition template of the index group: exactly one single-level
	// template (or none for an unpartitioned table).
	if (!index_group->manifest.partitioning.empty()) {
		templates = index_group->manifest.partitioning;
	}
}

void KeyResolver::LoadPartition(const GroupPartition &partition) {
	auto &entry = cache[partition.key];
	if (entry.loaded) {
		return;
	}
	entry.loaded = true;

	Value prev;
	bool has_prev = false;
	for (idx_t k = 0; k < partition.part_count; k++) {
		auto &part = index_group->parts[partition.first_part + k];
		auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(part.path), ParquetOptions(context));
		// The symbol column must exist in every index part (it is a primary-key
		// column; schema evolution cannot drop it).
		idx_t symbol_pos = DConstants::INVALID_INDEX;
		for (idx_t c = 0; c < reader->columns.size(); c++) {
			if (StringUtil::CIEquals(reader->columns[c].name, index_group->symbol_column)) {
				symbol_pos = c;
				break;
			}
		}
		if (symbol_pos == DConstants::INVALID_INDEX) {
			throw IOException("Aligned table '%s' group 'index' partition '%s': part '%s' has no symbol column "
			                  "'%s' (v7 primary-key contract)",
			                  plan.table.name.empty() ? plan.table_path : plan.table.name, partition.key,
			                  part.part_name, index_group->symbol_column);
		}
		reader->column_ids.push_back(MultiFileLocalColumnId(symbol_pos));

		vector<PartitionStatistics> rg_stats;
		reader->GetPartitionStats(rg_stats);
		vector<idx_t> all_rgs;
		for (idx_t i = 0; i < rg_stats.size(); i++) {
			all_rgs.push_back(i);
		}
		ParquetReaderScanState scan_state;
		reader->InitializeScan(context, scan_state, all_rgs);
		DataChunk chunk;
		chunk.Initialize(context, {reader->columns[symbol_pos].type});
		while (true) {
			auto res = reader->Scan(context, scan_state, chunk);
			auto async_type = res.GetResultType();
			if (async_type == AsyncResultType::FINISHED || async_type == AsyncResultType::BLOCKED) {
				break;
			}
			for (idx_t r = 0; r < chunk.size(); r++) {
				Value v = chunk.GetValue(0, r);
				// v7 sort contract: within a partition, symbols are strictly
				// ascending (this is what makes binary search and the
				// insertion-position semantics correct).
				if (has_prev && !(prev < v)) {
					throw IOException("Aligned table '%s' group 'index' partition '%s': rows are not strictly "
					                  "sorted by symbol '%s' (v7 sort contract; duplicates or descending order)",
					                  plan.table.name.empty() ? plan.table_path : plan.table.name, partition.key,
					                  index_group->symbol_column);
				}
				prev = v;
				has_prev = true;
				entry.symbols.push_back(std::move(v));
			}
		}
	}
}

KeyLocation KeyResolver::Resolve(date_t date_value, const Value &symbol_value) {
	KeyLocation loc;
	string key;
	if (!templates.empty()) {
		if (!EvaluatePartitionTemplate(templates[0].template_str, date_value, key)) {
			throw IOException("Aligned table '%s': cannot evaluate partition template '%s' for key resolution",
			                  plan.table.name.empty() ? plan.table_path : plan.table.name,
			                  templates[0].template_str);
		}
	}

	// Find the partition by key (index partitions are key-ordered; the key
	// strings are lexicographically ordered because the template is the same).
	for (auto &partition : index_group->partitions) {
		if (partition.key != key) {
			continue;
		}
		LoadPartition(partition);
		auto &syms = cache[partition.key].symbols;

		// Binary search: the first symbol >= the key's symbol (partition rows
		// are strictly ascending, so this is also the unique insertion point).
		idx_t lo = 0;
		idx_t hi = syms.size();
		while (lo < hi) {
			idx_t mid = lo + (hi - lo) / 2;
			if (syms[mid] < symbol_value) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		idx_t p = lo;

		// Locate the part containing position p (an append at the partition end
		// creates a NEW part instead of growing the last one).
		idx_t off = 0;
		idx_t chosen = partition.part_count - 1;
		bool in_range = false;
		for (idx_t k = 0; k < partition.part_count; k++) {
			auto &part = index_group->parts[partition.first_part + k];
			if (p < off + part.row_count) {
				chosen = k;
				in_range = true;
				break;
			}
			off += part.row_count;
		}
		auto &part = index_group->parts[partition.first_part + chosen];

		loc.partition_key = key;
		loc.part_index = part.partition_index;
		if (in_range) {
			loc.part_local_row = p - off;
		} else {
			// p == partition row count: the key sorts after every symbol of the
			// partition. The caller appends a NEW part (index = the next free
			// partition-local index) holding only the new rows — the existing
			// parts are not rewritten.
			idx_t max_index = part.partition_index;
			for (idx_t k = 0; k < partition.part_count; k++) {
				auto &pk = index_group->parts[partition.first_part + k];
				if (pk.partition_index > max_index) {
					max_index = pk.partition_index;
				}
			}
			loc.part_index = max_index + 1;
			loc.part_local_row = 0;
			loc.append_new_part = true;
		}
		loc.found = p < syms.size() && !(symbol_value < syms[p]);
		return loc;
	}

	// New partition (the key's date maps to no existing partition directory):
	// a fresh part "0000-..." is created by the caller; the reader derives its
	// start row from the partition ordering automatically.
	loc.partition_key = key;
	loc.part_index = 0;
	loc.part_local_row = 0;
	loc.found = false;
	return loc;
}

} // namespace duckdb