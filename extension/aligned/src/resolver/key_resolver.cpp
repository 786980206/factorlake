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
	// template (or none for an unpartitioned table). For an empty table (first
	// write), default to "month=%Y-%m" — the mutator uses the same default.
	if (!index_group->manifest.partitioning.empty()) {
		templates = index_group->manifest.partitioning;
	} else if (index_group->parts.empty()) {
		templates.push_back({"month=%Y-%m", ""});
	}
}

void KeyResolver::LoadPartition(const GroupPartition &partition) {
	auto &entry = cache[partition.key];
	if (entry.loaded) {
		return;
	}
	entry.loaded = true;

	// v8 contract: within a partition, rows are sorted by (symbol, date)
	// strictly ascending. We read both columns to validate and cache them.
	Value prev_sym;
	date_t prev_date {};
	bool has_prev = false;
	for (idx_t k = 0; k < partition.part_count; k++) {
		auto &part = index_group->parts[partition.first_part + k];
		auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(part.path), ParquetOptions(context));
		// The key columns (symbol, date) must exist in every index part.
		idx_t symbol_pos = DConstants::INVALID_INDEX;
		idx_t date_pos = DConstants::INVALID_INDEX;
		for (idx_t c = 0; c < reader->columns.size(); c++) {
			if (StringUtil::CIEquals(reader->columns[c].name, index_group->symbol_column)) {
				symbol_pos = c;
			}
			if (StringUtil::CIEquals(reader->columns[c].name, index_group->partition_source)) {
				date_pos = c;
			}
		}
		if (symbol_pos == DConstants::INVALID_INDEX) {
			throw IOException("Aligned table '%s' group 'index' partition '%s': part '%s' has no symbol column "
			                  "'%s' (v8 primary-key contract)",
			                  plan.table_name, partition.key,
			                  part.part_name, index_group->symbol_column);
		}
		if (date_pos == DConstants::INVALID_INDEX) {
			throw IOException("Aligned table '%s' group 'index' partition '%s': part '%s' has no date column "
			                  "'%s' (v8 primary-key contract)",
			                  plan.table_name, partition.key,
			                  part.part_name, index_group->partition_source);
		}
		reader->column_ids.push_back(MultiFileLocalColumnId(symbol_pos));
		reader->column_ids.push_back(MultiFileLocalColumnId(date_pos));

		vector<PartitionStatistics> rg_stats;
		reader->GetPartitionStats(rg_stats);
		vector<idx_t> all_rgs;
		for (idx_t i = 0; i < rg_stats.size(); i++) {
			all_rgs.push_back(i);
		}
		ParquetReaderScanState scan_state;
		reader->InitializeScan(context, scan_state, all_rgs);
		DataChunk chunk;
		chunk.Initialize(context, {reader->columns[symbol_pos].type, reader->columns[date_pos].type});
		bool is_timestamp = reader->columns[date_pos].type.id() == LogicalTypeId::TIMESTAMP;
		while (true) {
			auto res = reader->Scan(context, scan_state, chunk);
			auto async_type = res.GetResultType();
			if (async_type == AsyncResultType::FINISHED || async_type == AsyncResultType::BLOCKED) {
				break;
			}
			UnifiedVectorFormat sv, dv;
			chunk.data[0].ToUnifiedFormat(chunk.size(), sv);
			chunk.data[1].ToUnifiedFormat(chunk.size(), dv);
			auto sptr = UnifiedVectorFormat::GetData<string_t>(sv);
			for (idx_t r = 0; r < chunk.size(); r++) {
				auto si = sv.sel->get_index(r);
				auto di = dv.sel->get_index(r);
				Value sym_val = chunk.GetValue(0, r);
				// Extract the date value: DATE = int32 days since epoch;
				// TIMESTAMP = int64 nanoseconds → use Timestamp::GetDate.
				date_t date_val;
				if (is_timestamp) {
					auto tptr = UnifiedVectorFormat::GetData<int64_t>(dv);
					date_val = Timestamp::GetDate(timestamp_t(tptr[di]));
				} else {
					auto dptr = UnifiedVectorFormat::GetData<int32_t>(dv);
					date_val = date_t(dptr[di]);
				}
				// v8 sort contract: (symbol, date) strictly ascending.
				if (has_prev) {
					bool same_sym = (prev_sym == sym_val);
					bool ascending = same_sym ? (date_val > prev_date)
					                          : (prev_sym < sym_val);
					if (!ascending) {
						throw IOException("Aligned table '%s' group 'index' partition '%s': rows are not strictly "
						                  "sorted by (symbol, date) (v8 sort contract; duplicates or descending order)",
						                  plan.table_name, partition.key);
					}
				}
				prev_sym = sym_val;
				prev_date = date_val;
				has_prev = true;
				entry.symbols.push_back(std::move(sym_val));
				entry.dates.push_back(date_val);
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
			                  plan.table_name,
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
		auto &cache_entry = cache[partition.key];
		auto &syms = cache_entry.symbols;
		auto &dates = cache_entry.dates;

		// Binary search for the composite key (symbol, date). The cached
		// arrays are sorted by (symbol, date) strictly ascending. We find
		// the first position where (symbol, date) >= (key_sym, key_date).
		idx_t lo = 0;
		idx_t hi = syms.size();
		while (lo < hi) {
			idx_t mid = lo + (hi - lo) / 2;
			bool lt = syms[mid] < symbol_value ||
			          (syms[mid] == symbol_value && dates[mid] < date_value);
			if (lt) {
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
			// p == partition row count: the key sorts after every (symbol, date)
			// of the partition. The caller appends a NEW part holding only the
			// new rows — the existing parts are not rewritten. The new part
			// index must be free across ALL groups (not just the index group),
			// otherwise it would collide with another group's existing part at
			// the same index and violate the v6 "shared index row counts must
			// agree" contract. Use the partition-wide maximum index + 1.
			idx_t max_index = 0;
			for (auto &group : plan.groups) {
				for (auto &gp : group.partitions) {
					if (gp.key != key) {
						continue;
					}
					for (idx_t k = 0; k < gp.part_count; k++) {
						auto &pk = group.parts[gp.first_part + k];
						if (pk.partition_index > max_index) {
							max_index = pk.partition_index;
						}
					}
					break;
				}
			}
			loc.part_index = max_index + 1;
			loc.part_local_row = 0;
			loc.append_new_part = true;
		}
		loc.found = p < syms.size() &&
		            syms[p] == symbol_value && dates[p] == date_value;
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