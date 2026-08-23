#include "resolver/key_resolver.hpp"

#include "resolver/partition_resolver.hpp"
#include "io/parquet_io.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

KeyResolver::KeyResolver(ClientContext &context_p, const TablePlan &plan_p) : context(context_p), plan(plan_p) {
	// The index group is the first plan group (BuildTablePlan inserts it at
	// position 0). An empty table (no index parts) cannot resolve keys — the
	// caller must handle the first-write case separately.
	if (plan.groups.empty()) {
		throw IOException("Aligned table: no column groups to resolve keys against");
	}
	index_group = &IndexGroup(plan);
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
		// The key columns (symbol, date) must exist in every index part.
		vector<LogicalType> key_types;
		ParquetReaderScanState scan_state;
		auto reader = OpenPartReaderNamedColumns(context, part.path,
		                                          {index_group->symbol_column, index_group->partition_source},
		                                          key_types, scan_state);
		DataChunk chunk;
		chunk.Initialize(context, key_types);
		bool is_timestamp = key_types[1].id() == LogicalTypeId::TIMESTAMP;
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

void KeyResolver::LoadPartitionBoundaries(const GroupPartition &partition) {
	auto &entry = cache[partition.key];
	if (entry.boundary_loaded) {
		return;
	}
	entry.boundary_loaded = true;

	// Build a lightweight symbol min/max index per part from Parquet Row Group
	// statistics — no data read. This allows Resolve to fast-reject keys
	// whose symbol is outside the partition's symbol range.
	for (idx_t k = 0; k < partition.part_count; k++) {
		auto &part = index_group->parts[partition.first_part + k];
		auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(part.path), ParquetOptions(context));
		// Find the symbol column index
		idx_t symbol_pos = DConstants::INVALID_INDEX;
		for (idx_t c = 0; c < reader->columns.size(); c++) {
			if (StringUtil::CIEquals(reader->columns[c].name, index_group->symbol_column)) {
				symbol_pos = c;
				break;
			}
		}
		if (symbol_pos == DConstants::INVALID_INDEX) {
			// Skip boundary loading if the column is missing (will be caught
			// by the full LoadPartition later).
			entry.part_sym_min.push_back(Value());
			entry.part_sym_max.push_back(Value());
			continue;
		}
		vector<PartitionStatistics> rg_stats;
		reader->GetPartitionStats(rg_stats);
		// The symbol column stats min/max across all RGs gives the part's
		// symbol range. Since rows are sorted by (symbol, date), the first
		// RG's min is the part's min and the last RG's max is the part's max.
		Value part_min, part_max;
		bool have_stats = false;
		for (idx_t rg = 0; rg < rg_stats.size(); rg++) {
			auto &stats = rg_stats[rg];
			if (!stats.partition_row_group) {
				continue;
			}
			auto base_stats = stats.partition_row_group->GetColumnStatistics(StorageIndex(symbol_pos));
			if (!base_stats) {
				continue;
			}
			Value rg_min, rg_max;
			if (base_stats->GetType().id() == LogicalTypeId::VARCHAR) {
				rg_min = Value(StringStats::Min(*base_stats));
				rg_max = Value(StringStats::Max(*base_stats));
			} else {
				rg_min = NumericStats::Min(*base_stats);
				rg_max = NumericStats::Max(*base_stats);
			}
			if (!have_stats) {
				part_min = rg_min;
				part_max = rg_max;
				have_stats = true;
			} else {
				if (rg_min < part_min) {
					part_min = rg_min;
				}
				if (rg_max > part_max) {
					part_max = rg_max;
				}
			}
		}
		entry.part_sym_min.push_back(std::move(part_min));
		entry.part_sym_max.push_back(std::move(part_max));
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
		// Optimization: first load the lightweight symbol boundary index
		// (from Parquet RG stats, no data read). If the key's symbol is
		// outside the partition's [min, max] symbol range, the key is an
		// append-at-end — we can skip the full partition data load entirely.
		LoadPartitionBoundaries(partition);
		auto &cache_entry = cache[partition.key];
		auto &bounds = cache_entry;

		// Fast path: check if the symbol is outside the partition's range.
		if (!bounds.part_sym_min.empty() && !bounds.part_sym_min[0].IsNull()) {
			Value &part_min = bounds.part_sym_min.front();
			Value &part_max = bounds.part_sym_max.back();
			if (symbol_value < part_min || symbol_value > part_max) {
				// Symbol is outside the partition's symbol range → append at end.
				// Determine append_to_last vs append_new_part using the last
				// part's row count (already known from the plan, no data read).
				idx_t last_k = partition.part_count - 1;
				auto &last_part = index_group->parts[partition.first_part + last_k];
				loc.partition_key = key;
				if (last_part.row_count < ALIGNED_DEFAULT_PART_ROWS) {
					loc.part_index = last_part.partition_index;
					loc.part_local_row = last_part.row_count;
					loc.append_to_last = true;
				} else {
					loc.part_index = NextPartIndexForPartition(plan, key);
					loc.part_local_row = 0;
					loc.append_new_part = true;
				}
				loc.found = false;
				return loc;
			}
		}

		// Slow path: symbol is within range, need full binary search.
		LoadPartition(partition);
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
			// of the partition. Two strategies:
			//  (a) If the partition's last existing part is below
			//      ALIGNED_DEFAULT_PART_ROWS, grow it in-place (append_to_last).
			//      The rewriter merges the old rows with the appended rows; the
			//      mutator re-validates across all groups (schema evolution +
			//      threshold) and may fall back to (b).
			//  (b) Otherwise, or when the mutator's pre-check rejects (a),
			//      append a NEW part holding only the new rows — the existing
			//      parts are not rewritten. The new part index must be free
			//      across ALL groups (not just the index group), otherwise it
			//      would collide with another group's existing part at the same
			//      index and violate the v6 "shared index row counts must agree"
			//      contract.
			if (part.row_count < ALIGNED_DEFAULT_PART_ROWS) {
				loc.part_index = part.partition_index;
				loc.part_local_row = part.row_count;
				loc.append_to_last = true;
			} else {
				loc.part_index = NextPartIndexForPartition(plan, key);
				loc.part_local_row = 0;
				loc.append_new_part = true;
			}
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