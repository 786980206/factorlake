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
	if (plan.groups.empty()) {
		throw IOException("Aligned table: no column groups to resolve keys against");
	}
	index_group = &IndexGroup(plan);
	if (!index_group->manifest.partitioning.empty()) {
		templates = index_group->manifest.partitioning;
	} else if (index_group->parts.empty()) {
		templates.push_back({"month=%Y-%m", ""});
	}
	// Determine if the partition source column is TIMESTAMP (vs DATE).
	// When TIMESTAMP, the full timestamp value is used as the key so that
	// intraday rows with the same date but different times are distinct.
	if (!index_group->parts.empty()) {
		vector<LogicalType> key_types;
		ParquetReaderScanState scan_state;
		auto reader = OpenPartReaderNamedColumns(context, index_group->parts[0].path,
		                                          {index_group->symbol_column, index_group->partition_source},
		                                          key_types, scan_state);
		is_timestamp = key_types[1].id() == LogicalTypeId::TIMESTAMP;
	}
}

date_t KeyResolver::ToDate(int64_t value) const {
	if (is_timestamp) {
		return Timestamp::GetDate(timestamp_t(value));
	}
	return date_t(static_cast<int32_t>(value));
}

void KeyResolver::LoadPartition(const GroupPartition &partition) {
	auto &entry = cache[partition.key];
	if (entry.loaded) {
		return;
	}
	entry.loaded = true;

	Value prev_sym;
	int64_t prev_date {};
	bool has_prev = false;
	for (idx_t k = 0; k < partition.part_count; k++) {
		auto &part = index_group->parts[partition.first_part + k];
		vector<LogicalType> key_types;
		ParquetReaderScanState scan_state;
		auto reader = OpenPartReaderNamedColumns(context, part.path,
		                                          {index_group->symbol_column, index_group->partition_source},
		                                          key_types, scan_state);
		DataChunk chunk;
		chunk.Initialize(context, key_types);
		bool local_is_timestamp = key_types[1].id() == LogicalTypeId::TIMESTAMP;
		while (true) {
			auto res = reader->Scan(context, scan_state, chunk);
			auto async_type = res.GetResultType();
			if (async_type == AsyncResultType::FINISHED || async_type == AsyncResultType::BLOCKED) {
				break;
			}
			UnifiedVectorFormat sv, dv;
			chunk.data[0].ToUnifiedFormat(chunk.size(), sv);
			chunk.data[1].ToUnifiedFormat(chunk.size(), dv);
			auto sym_data = UnifiedVectorFormat::GetData<string_t>(sv);
			for (idx_t r = 0; r < chunk.size(); r++) {
				auto si = sv.sel->get_index(r);
				auto di = dv.sel->get_index(r);
				if (!sv.validity.RowIsValid(si)) {
					throw IOException("Aligned table '%s' group 'index' partition '%s': NULL symbol at row %llu",
					                  plan.table_name, partition.key, (unsigned long long)entry.symbols.size());
				}
				// Read symbol as string_t to avoid per-row Value allocation.
				string_t sym_str = sym_data[si];
				Value sym_val(sym_str.GetString());
				int64_t date_val;
				if (local_is_timestamp) {
					auto tptr = UnifiedVectorFormat::GetData<int64_t>(dv);
					date_val = tptr[di]; // full timestamp value
				} else {
					auto dptr = UnifiedVectorFormat::GetData<int32_t>(dv);
					date_val = static_cast<int64_t>(dptr[di]);
				}
				if (has_prev) {
					bool same_sym = (prev_sym == sym_val);
					bool ascending = same_sym ? (date_val > prev_date) : (prev_sym < sym_val);
					if (!ascending) {
						throw IOException("Aligned table '%s' group 'index' partition '%s': rows are not strictly "
						                  "sorted by (symbol, date) (v8 sort contract)",
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

	for (idx_t k = 0; k < partition.part_count; k++) {
		auto &part = index_group->parts[partition.first_part + k];
		auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(part.path), ParquetOptions(context));
		idx_t symbol_pos = DConstants::INVALID_INDEX;
		for (idx_t c = 0; c < reader->columns.size(); c++) {
			if (StringUtil::CIEquals(reader->columns[c].name, index_group->symbol_column)) {
				symbol_pos = c;
				break;
			}
		}
		if (symbol_pos == DConstants::INVALID_INDEX) {
			entry.part_sym_min.push_back(Value());
			entry.part_sym_max.push_back(Value());
			continue;
		}
		vector<PartitionStatistics> rg_stats;
		reader->GetPartitionStats(rg_stats);
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

void KeyResolver::LoadSinglePart(const GroupPartition &partition, idx_t part_k) {
	auto &entry = cache[partition.key];
	if (entry.part_symbols.size() != partition.part_count) {
		entry.part_symbols.resize(partition.part_count);
		entry.part_dates.resize(partition.part_count);
		entry.part_loaded.resize(partition.part_count, false);
	}
	if (entry.part_loaded[part_k]) {
		return;
	}
	entry.part_loaded[part_k] = true;

	auto &part = index_group->parts[partition.first_part + part_k];
	vector<LogicalType> key_types;
	ParquetReaderScanState scan_state;
	auto reader = OpenPartReaderNamedColumns(context, part.path,
	                                          {index_group->symbol_column, index_group->partition_source},
	                                          key_types, scan_state);
	DataChunk chunk;
	chunk.Initialize(context, key_types);
	bool local_is_timestamp = key_types[1].id() == LogicalTypeId::TIMESTAMP;
	auto &syms = entry.part_symbols[part_k];
	auto &dates = entry.part_dates[part_k];
	while (true) {
		auto res = reader->Scan(context, scan_state, chunk);
		auto async_type = res.GetResultType();
		if (async_type == AsyncResultType::FINISHED || async_type == AsyncResultType::BLOCKED) {
			break;
		}
		UnifiedVectorFormat sv, dv;
		chunk.data[0].ToUnifiedFormat(chunk.size(), sv);
		chunk.data[1].ToUnifiedFormat(chunk.size(), dv);
		auto sym_data = UnifiedVectorFormat::GetData<string_t>(sv);
		for (idx_t r = 0; r < chunk.size(); r++) {
			auto si = sv.sel->get_index(r);
			auto di = dv.sel->get_index(r);
			if (!sv.validity.RowIsValid(si)) {
				throw IOException("Aligned table '%s' group 'index': NULL symbol in part '%s'",
				                  plan.table_name, part.part_name);
			}
			string_t sym_str = sym_data[si];
			Value sym_val(sym_str.GetString());
			int64_t date_val;
			if (local_is_timestamp) {
				auto tptr = UnifiedVectorFormat::GetData<int64_t>(dv);
				date_val = tptr[di]; // full timestamp value
			} else {
				auto dptr = UnifiedVectorFormat::GetData<int32_t>(dv);
				date_val = static_cast<int64_t>(dptr[di]);
			}
			syms.push_back(std::move(sym_val));
			dates.push_back(date_val);
		}
	}
}

KeyLocation KeyResolver::Resolve(int64_t date_value, const Value &symbol_value) {
	KeyLocation loc;
	string key;
	if (!templates.empty()) {
		if (!EvaluatePartitionTemplate(templates[0].template_str, date_value, key)) {
			throw IOException("Aligned table '%s': cannot evaluate partition template '%s' for key resolution",
			                  plan.table_name, templates[0].template_str);
		}
	}

	for (auto &partition : index_group->partitions) {
		if (partition.key != key) {
			continue;
		}
		LoadPartitionBoundaries(partition);
		auto &cache_entry = cache[partition.key];
		auto &bounds = cache_entry;

		// Fast path: symbol outside the partition's overall [min, max] -> append.
		if (!bounds.part_sym_min.empty() && !bounds.part_sym_min[0].IsNull()) {
			Value &part_min = bounds.part_sym_min.front();
			Value &part_max = bounds.part_sym_max.back();
			if (symbol_value < part_min || symbol_value > part_max) {
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

		// Part-level binary search: find which part file's symbol range
		// contains symbol_value. The rewrite granularity is a part file, so
		// part-level resolution is sufficient — no need to load all parts'
		// data. Parts are ordered by symbol_min ascending (rows sorted by
		// symbol within partition).
		idx_t part_k = 0;
		bool part_found = false;
		if (bounds.part_sym_min.size() == partition.part_count) {
			idx_t lo = 0, hi = partition.part_count;
			while (lo < hi) {
				idx_t mid = lo + (hi - lo) / 2;
				if (!bounds.part_sym_min[mid].IsNull() && bounds.part_sym_min[mid] <= symbol_value) {
					lo = mid + 1;
				} else {
					hi = mid;
				}
			}
			if (lo > 0) {
				lo--;
			}
			if (lo < partition.part_count) {
				auto &pmin = bounds.part_sym_min[lo];
				auto &pmax = bounds.part_sym_max[lo];
				if (!pmin.IsNull() && !pmax.IsNull() &&
				    symbol_value >= pmin && symbol_value <= pmax) {
					part_k = lo;
					part_found = true;
				}
			}
		}

		if (part_found) {
			// Load only THIS part's data (not the whole partition) for the
			// final per-row binary search to determine insert vs update.
			auto &part = index_group->parts[partition.first_part + part_k];
			LoadSinglePart(partition, part_k);
			auto &pc = cache[partition.key];
			auto &syms = pc.part_symbols[part_k];
			auto &dates = pc.part_dates[part_k];

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

			loc.partition_key = key;
			loc.part_index = part.partition_index;

			if (p < syms.size() && syms[p] == symbol_value && dates[p] == date_value) {
				loc.found = true;
				loc.part_local_row = p;
				return loc;
			}

			if (p >= syms.size() && part_k == partition.part_count - 1) {
				if (part.row_count < ALIGNED_DEFAULT_PART_ROWS) {
					loc.part_local_row = part.row_count;
					loc.append_to_last = true;
				} else {
					loc.part_index = NextPartIndexForPartition(plan, key);
					loc.part_local_row = 0;
					loc.append_new_part = true;
				}
			} else {
				loc.part_local_row = p;
			}
			loc.found = false;
			return loc;
		}

		// Fallback: part stats missing or symbol between parts.
		LoadPartition(partition);
		auto &syms = cache_entry.symbols;
		auto &dates = cache_entry.dates;

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

	loc.partition_key = key;
	loc.part_index = 0;
	loc.part_local_row = 0;
	loc.found = false;
	return loc;
}

} // namespace duckdb
