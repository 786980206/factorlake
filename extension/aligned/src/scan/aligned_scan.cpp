#include "scan/aligned_scan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "parquet_reader.hpp"
#include "resolver/partition_resolver.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Bind data / scan states
//===----------------------------------------------------------------------===//

struct AlignedTableBindData : public TableFunctionData {
	TablePlan plan;
	vector<string> names;
	vector<LogicalType> types;
	idx_t total_rows = 0;
};

//! Per-group, per-thread scan state. Row windows are driven by the global
//! cursor; all groups are read in lockstep over the same logical row range
//! (the core alignment invariant — never verified by key comparison).
struct AlignedGroupScanState {
	idx_t part_idx = 0;
	bool part_ready = false;
	unique_ptr<ParquetReader> reader;
	// Fresh scan state per row-group window: duckdb's parquet scan state is
	// designed for a single InitializeScan + repeated Scan lifecycle; reusing
	// one state across windows re-initializes it, which is not a supported
	// pattern and crashes on the second data read.
	unique_ptr<ParquetReaderScanState> scan_state;
	vector<PartitionStatistics> rg_stats; // cached per part
	// Current row-group window (part-local row coordinates)
	vector<idx_t> rg_window; // row group indices
	idx_t rg_window_start = 0; // part-local row where the window begins
	idx_t rg_window_rows = 0;  // total rows in the window
	idx_t rg_window_pos = 0;   // rows consumed from the window (window-local)
	// Read mapping for the current part (columns present in this part only)
	vector<idx_t> read_cols;     // file column index per read column
	vector<idx_t> out_positions; // table output position per read column
	vector<LogicalType> read_types;
	vector<idx_t> missing_positions; // table output positions absent from this part (NULL fill)
	// Parquet output chunk (read columns only). Held by unique_ptr because
	// DataChunk is neither copyable nor movable (which would make this state
	// immovable and break vector<AlignedGroupScanState> reallocation).
	unique_ptr<DataChunk> chunk;
	// Phase 3: row-group plan of the current window. RGs whose statistics
	// contradict a pushed-down filter are skipped (their rows are NULL-filled —
	// they can never match the filter) instead of being read from parquet.
	// Coordinates: "flow" positions are row offsets in the parquet stream of
	// the read row groups (each RG contributes its FULL row count, including
	// rows that lie outside the wanted window); "win" positions are window
	// coordinates relative to rg_window_start.
	struct RgPlanSeg {
		idx_t flow_start; // parquet stream position where this RG starts
		idx_t flow_len;   // full row count of this RG
		idx_t flow_off;   // offset within the RG where the wanted segment starts
		idx_t win_start;  // window coordinate of the segment start
		idx_t win_len;    // number of wanted rows in this segment
	};
	vector<RgPlanSeg> rg_plan;              // read segments (mapped into the parquet window)
	vector<pair<idx_t, idx_t>> rg_skip;     // skipped segments (win_start, win_len) — NULL filled
	idx_t rg_plan_rows = 0;                 // total stream rows of the read segments
	idx_t parquet_pos = 0;                  // rows consumed from the parquet stream (current window)
	idx_t rg_seg_idx = 0;                   // current read segment index
	bool rg_window_valid = false;           // the current window still covers the wanted range
};

//! A pushed-down filter applied to one of this table's columns.
struct AlignedRowFilter {
	idx_t projected_pos; // position in the assembled (scanned) chunk
	const TableFilter *filter;
	unique_ptr<TableFilterState> state;
};

//! A pushed-down filter on one group's column, used for row-group pruning.
struct AlignedGroupFilter {
	string column_name;
	const TableFilter *filter;
};

struct AlignedScanGlobalState : public GlobalTableFunctionState {
	idx_t total_rows = 0;
	idx_t next_row = 0; // cursor within the current active interval
	// Projection pushdown (Phase 2): full schema position -> output chunk position
	vector<idx_t> projected_pos;
	// Per-group flag: does this group contribute any requested column?
	vector<bool> group_active;
	// Filters (Phase 3)
	vector<AlignedRowFilter> row_filters;
	vector<vector<AlignedGroupFilter>> group_filters; // per group
	// Partition pruning (Phase 3): kept parts per group; empty = keep all from bind
	vector<vector<PartInfo>> kept_parts;
	// Active row intervals: intersection of kept-part intervals over active
	// groups. The scan cursor only walks these intervals (pruned partitions
	// are skipped entirely — all groups agree on the inactive ranges).
	vector<pair<idx_t, idx_t>> active_intervals;
	idx_t interval_idx = 0;
	// Filter-column removal (projection_ids): which scanned columns the final
	// output keeps (indexes into the scanned column list)
	vector<idx_t> projection_ids;
};

struct AlignedScanLocalState : public LocalTableFunctionState {
	vector<AlignedGroupScanState> groups;
	// Scratch chunk for the filter/projection path (assemble -> filter -> reference)
	unique_ptr<DataChunk> scratch;
};

//===----------------------------------------------------------------------===//
// Column type resolution (bind time)
//===----------------------------------------------------------------------===//

namespace {

//! Opens a parquet file and validates the sidecar-declared column order against
//! the actual file schema (contract §7 / §10). Returns the reader.
unique_ptr<ParquetReader> OpenPartReader(ClientContext &context, const PartInfo &part, const string &table_name,
                                         const string &group_name) {
	auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(part.path), ParquetOptions(context));
	if (reader->columns.size() != part.columns.size()) {
		throw IOException("Aligned table '%s' group '%s' part '%s': sidecar declares %llu columns but the file has "
		                  "%llu (schema mismatch)",
		                  table_name, group_name, part.part_name, part.columns.size(), reader->columns.size());
	}
	for (idx_t i = 0; i < part.columns.size(); i++) {
		if (reader->columns[i].name != part.columns[i]) {
			throw IOException("Aligned table '%s' group '%s' part '%s': sidecar column %llu is '%s' but the file has "
		                      "'%s' (column order mismatch)",
		                      table_name, group_name, part.part_name, i, part.columns[i], reader->columns[i].name);
		}
	}
	return reader;
}

} // namespace

//! Builds the table schema (names/types in table order) and fills each group's
//! output_positions. Types are resolved from the first part containing a column.
//! Column-name rules (contract §2.2), implemented in two passes:
//!  pass 1: count which (non-index) groups contain each bare column name;
//!  pass 2: register columns —
//!   - index columns: bare names (index is authoritative);
//!   - non-index columns whose name also exists in index: ignored entirely;
//!   - non-index columns whose name exists in >= 2 non-index groups: registered
//!     under the qualified name "lv1.lv2.col_name" in EVERY such group (the
//!     bare name is not registered at all → querying it reports "column not
//!     found");
//!   - other non-index columns: bare names.
static void ResolveColumnTypes(ClientContext &context, TablePlan &plan, vector<string> &names,
                               vector<LogicalType> &types) {
	// Find the mandatory 'index' group (validated in BuildTablePlan)
	idx_t index_group = DConstants::INVALID_INDEX;
	for (idx_t gi = 0; gi < plan.groups.size(); gi++) {
		if (StringUtil::CIEquals(plan.groups[gi].manifest.group, "index")) {
			index_group = gi;
			break;
		}
	}
	// Pass 1: which groups contain each bare column name
	case_insensitive_set_t index_columns;
	case_insensitive_map_t<vector<idx_t>> col_groups; // bare name -> non-index groups
	for (idx_t gi = 0; gi < plan.groups.size(); gi++) {
		auto &group = plan.groups[gi];
		for (auto &col : group.column_order) {
			if (gi == index_group) {
				index_columns.insert(col);
			} else {
				auto &owners = col_groups[col];
				if (std::find(owners.begin(), owners.end(), gi) == owners.end()) {
					owners.push_back(gi);
				}
			}
		}
	}

	// Process index first, then the remaining groups in manifest order
	vector<idx_t> order;
	if (index_group != DConstants::INVALID_INDEX) {
		order.push_back(index_group);
	}
	for (idx_t gi = 0; gi < plan.groups.size(); gi++) {
		if (gi != index_group) {
			order.push_back(gi);
		}
	}

	for (auto gi : order) {
		auto &group = plan.groups[gi];
		// Open the first part once (if any) as the type source for most columns
		unique_ptr<ParquetReader> first_reader;
		if (!group.parts.empty()) {
			first_reader = OpenPartReader(context, group.parts[0], plan.table.name, group.manifest.group);
		}
		// Resolves a column's type from the first part containing it
		// (schema evolution: later parts may introduce new columns)
		auto resolve_type = [&](const string &col) -> LogicalType {
			if (first_reader) {
				auto it = std::find(group.parts[0].columns.begin(), group.parts[0].columns.end(), col);
				if (it != group.parts[0].columns.end()) {
					return first_reader->columns[it - group.parts[0].columns.begin()].type;
				}
			}
			for (idx_t pi = 1; pi < group.parts.size(); pi++) {
				auto &part = group.parts[pi];
				auto it = std::find(part.columns.begin(), part.columns.end(), col);
				if (it == part.columns.end()) {
					continue;
				}
				auto reader = OpenPartReader(context, part, plan.table.name, group.manifest.group);
				return reader->columns[it - part.columns.begin()].type;
			}
			throw IOException("Aligned table '%s' group '%s': column '%s' is declared but not found in any part",
			                  plan.table.name, group.manifest.group, col);
		};
		for (auto &col : group.column_order) {
			if (gi == index_group) {
				// index columns are authoritative: bare names
				group.output_positions.push_back(names.size());
				names.push_back(col);
				types.push_back(resolve_type(col));
				continue;
			}
			if (index_columns.count(col) > 0) {
				// Contract §2.2e.1: duplicate of an index column — ignored
				group.output_positions.push_back(DConstants::INVALID_INDEX);
				continue;
			}
			auto owners = col_groups.find(col);
			bool duplicated = owners != col_groups.end() && owners->second.size() > 1;
			if (duplicated) {
				// Contract §2.2e.2: duplicated across non-index groups — the
				// qualified name is the ONLY way to reference it
				auto qualified = group.lv1 + "." + group.lv2 + "." + col;
				group.output_positions.push_back(names.size());
				names.push_back(qualified);
				types.push_back(resolve_type(col));
			} else {
				group.output_positions.push_back(names.size());
				names.push_back(col);
				types.push_back(resolve_type(col));
			}
		}
	}
}

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> AlignedBindInternal(ClientContext &context, const string &root, const string &table,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AlignedTableBindData>();
	BuildTablePlan(context, root, table, result->plan);
	ResolveColumnTypes(context, result->plan, result->names, result->types);
	result->total_rows = result->plan.table.row_count;
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

unique_ptr<FunctionData> AlignedBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	string root;
	string table;
	if (input.inputs.size() >= 2) {
		// aligned_scan(root, table)
		root = StringValue::Get(input.inputs[0]);
		table = StringValue::Get(input.inputs[1]);
	} else {
		// aligned_table(table, root=...)
		table = StringValue::Get(input.inputs[0]);
		auto entry = input.named_parameters.find("root");
		if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
			root = StringValue::Get(entry->second);
		} else {
			Value setting_value;
			auto lookup = context.TryGetCurrentSetting("aligned_data_root", setting_value);
			if (!lookup) {
				throw BinderException(
				    "aligned_table: no data root configured. Use aligned_table('name', root='...') or "
				    "SET aligned_data_root = '...'");
			}
			root = StringValue::Get(setting_value);
		}
	}
	return AlignedBindInternal(context, root, table, return_types, names);
}

//===----------------------------------------------------------------------===//
// Filter handling (Phase 3)
//===----------------------------------------------------------------------===//

//! Collects the constant-comparison leaves of a filter tree (AND conjunctions
//! are unwrapped; OR / null / other filters are left for row-level filtering).
static void CollectConstantFilters(const TableFilter &filter, vector<const ConstantFilter *> &out) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
		out.push_back(&filter.Cast<ConstantFilter>());
		break;
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		for (auto &child : and_filter.child_filters) {
			CollectConstantFilters(*child, out);
		}
		break;
	}
	default:
		break;
	}
}

//! Prunes the kept parts of a group by a pushed-down filter on a partition
//! source column (contract §4). Only safe when the filter's column is a
//! partition source AND its templates form a prefix of the partitioning list.
static void ApplyPartitionPruning(const AlignedTableBindData &bind, idx_t gi, const string &column_name,
                                  const TableFilter &filter, vector<PartInfo> &kept) {
	auto &group = bind.plan.groups[gi];

	vector<const ConstantFilter *> constants;
	CollectConstantFilters(filter, constants);
	for (auto cf : constants) {
		// The partition templates sourced from this column must form a prefix
		// of the partitioning list (so the evaluated dirs are a path prefix)
		vector<PartitionTemplate> matching;
		bool prefix_ok = true;
		bool seen_other = false;
		for (auto &t : group.manifest.partitioning) {
			if (StringUtil::CIEquals(t.source, column_name)) {
				if (seen_other) {
					prefix_ok = false;
					break;
				}
				matching.push_back(t);
			} else {
				seen_other = true;
			}
		}
		if (!prefix_ok || matching.empty()) {
			continue; // cannot prune by this filter
		}
		const auto &base = kept.empty() ? group.parts : kept;
		kept = PrunePartsByFilter(base, matching, *cf);
	}
}

//! Builds the merged row intervals covered by a part list.
static vector<pair<idx_t, idx_t>> BuildIntervals(const vector<PartInfo> &parts) {
	vector<pair<idx_t, idx_t>> intervals;
	for (auto &part : parts) {
		if (part.row_count == 0) {
			continue;
		}
		idx_t start = part.start_row;
		idx_t end = part.start_row + part.row_count;
		if (!intervals.empty() && intervals.back().second == start) {
			intervals.back().second = end;
		} else {
			intervals.emplace_back(start, end);
		}
	}
	return intervals;
}

//! Intersects two sorted, disjoint interval lists.
static vector<pair<idx_t, idx_t>> IntersectIntervals(const vector<pair<idx_t, idx_t>> &a,
                                                     const vector<pair<idx_t, idx_t>> &b) {
	vector<pair<idx_t, idx_t>> result;
	idx_t i = 0;
	idx_t j = 0;
	while (i < a.size() && j < b.size()) {
		idx_t s = MaxValue<idx_t>(a[i].first, b[j].first);
		idx_t e = MinValue<idx_t>(a[i].second, b[j].second);
		if (s < e) {
			if (!result.empty() && result.back().second == s) {
				result.back().second = e;
			} else {
				result.emplace_back(s, e);
			}
		}
		if (a[i].second < b[j].second) {
			i++;
		} else {
			j++;
		}
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Global / local state
//===----------------------------------------------------------------------===//

unique_ptr<GlobalTableFunctionState> AlignedInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<AlignedTableBindData>();
	auto result = make_uniq<AlignedScanGlobalState>();
	result->total_rows = bind.total_rows;
	result->projection_ids = input.projection_ids;

	// Projection pushdown: input.column_ids are the requested columns (indexes
	// into the full bind schema). An empty list means no columns at all are
	// requested (e.g. count(*)) — the output chunk then has no vectors and the
	// scan only reports cardinality.
	result->projected_pos.assign(bind.names.size(), DConstants::INVALID_INDEX);
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		auto col_id = input.column_ids[i];
		if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
			throw NotImplementedException("aligned_table: virtual row id column is not supported");
		}
		if (col_id >= bind.names.size()) {
			throw InternalException("aligned_table: column id %llu out of range (schema has %llu columns)", col_id,
			                        bind.names.size());
		}
		result->projected_pos[col_id] = i;
	}

	// Determine which groups actually need to be opened
	result->group_active.assign(bind.plan.groups.size(), false);
	for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
		auto &group = bind.plan.groups[gi];
		for (auto full_pos : group.output_positions) {
			if (full_pos != DConstants::INVALID_INDEX && result->projected_pos[full_pos] != DConstants::INVALID_INDEX) {
				result->group_active[gi] = true;
				break;
			}
		}
	}

	// Filters (Phase 3): input.filters keys are projected positions
	result->group_filters.resize(bind.plan.groups.size());
	result->kept_parts.resize(bind.plan.groups.size());
	if (input.filters) {
		for (auto &entry : input.filters->filters) {
			auto key = entry.first;
			auto &filter = *entry.second;
			if (key >= input.column_ids.size()) {
				throw InternalException("aligned_table: filter column id %llu out of range", key);
			}
			auto full_col = input.column_ids[key];
			auto state = TableFilterState::Initialize(context, filter);
			result->row_filters.push_back({key, &filter, std::move(state)});

			// Find the group owning this column (for RG pruning + partition pruning)
			for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
				auto &group = bind.plan.groups[gi];
				for (idx_t li = 0; li < group.column_order.size(); li++) {
					if (group.output_positions[li] == full_col) {
						result->group_filters[gi].push_back({group.column_order[li], &filter});
						ApplyPartitionPruning(bind, gi, group.column_order[li], filter, result->kept_parts[gi]);
						break;
					}
				}
			}
		}
	}

	// Active row intervals = intersection of kept-part intervals over active groups
	vector<pair<idx_t, idx_t>> active;
	bool any_active = false;
	for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
		if (!result->group_active[gi]) {
			continue;
		}
		auto &group = bind.plan.groups[gi];
		const auto &parts = result->kept_parts[gi].empty() ? group.parts : result->kept_parts[gi];
		auto intervals = BuildIntervals(parts);
		if (!any_active) {
			active = std::move(intervals);
			any_active = true;
		} else {
			active = IntersectIntervals(active, intervals);
		}
		if (active.empty()) {
			break;
		}
	}
	if (!any_active) {
		// No columns requested at all (e.g. count(*) without filters):
		// cardinality-only scan over the full row space
		active = {{0, bind.total_rows}};
	}
	result->active_intervals = std::move(active);
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> AlignedInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *gstate) {
	auto &bind = input.bind_data->Cast<AlignedTableBindData>();
	auto result = make_uniq<AlignedScanLocalState>();
	result->groups.resize(bind.plan.groups.size());
	// Scratch chunk for the filter/projection path: one vector per scanned column
	if (!input.projection_ids.empty() || input.filters) {
		vector<LogicalType> scanned_types;
		for (auto col_id : input.column_ids) {
			scanned_types.push_back(bind.types[col_id]);
		}
		result->scratch = make_uniq<DataChunk>();
		result->scratch->Initialize(context.client, scanned_types);
	}
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Per-part setup
//===----------------------------------------------------------------------===//

static void OpenPart(ClientContext &context, const AlignedTableBindData &bind, idx_t group_idx, idx_t part_idx,
                     AlignedGroupScanState &g, const vector<idx_t> &projected_pos, const vector<PartInfo> &parts) {
	auto &group = bind.plan.groups[group_idx];
	auto &part = parts[part_idx];

	g.part_idx = part_idx;
	g.reader = OpenPartReader(context, part, bind.plan.table.name, group.manifest.group);

	// Build the read mapping in group column order; only requested columns
	// are read from the parquet file (projection pushdown)
	g.read_cols.clear();
	g.out_positions.clear();
	g.read_types.clear();
	g.missing_positions.clear();
	for (idx_t i = 0; i < group.column_order.size(); i++) {
		if (group.output_positions[i] == DConstants::INVALID_INDEX) {
			// Shadowed duplicate column (see ResolveColumnTypes): skip entirely
			continue;
		}
		auto projected = projected_pos[group.output_positions[i]];
		if (projected == DConstants::INVALID_INDEX) {
			// Column not requested by this query: do not read it
			continue;
		}
		auto &col = group.column_order[i];
		auto it = std::find(part.columns.begin(), part.columns.end(), col);
		if (it == part.columns.end()) {
			g.missing_positions.push_back(projected);
			continue;
		}
		auto file_idx = it - part.columns.begin();
		g.read_cols.push_back(file_idx);
		g.out_positions.push_back(projected);
		g.read_types.push_back(g.reader->columns[file_idx].type);
	}

	// Columns to read from the parquet file (file-local indices)
	for (auto file_idx : g.read_cols) {
		g.reader->column_ids.push_back(MultiFileLocalColumnId(file_idx));
	}

	// Row group statistics (exact per-RG row counts)
	g.rg_stats.clear();
	g.reader->GetPartitionStats(g.rg_stats);
	if (g.rg_stats.empty()) {
		throw IOException("Aligned table '%s' group '%s' part '%s': file contains no row groups",
		                  bind.plan.table.name, group.manifest.group, part.part_name);
	}
	g.rg_window.clear();
	g.rg_plan.clear();
	g.rg_skip.clear();
	g.rg_window_start = 0;
	g.rg_window_rows = 0;
	g.rg_plan_rows = 0;
	g.parquet_pos = 0;
	g.rg_seg_idx = 0;
	g.rg_window_valid = false;
	g.part_ready = true;
}

//! Computes the row-group window covering [local_start, local_end) of the
//! current part and initializes the parquet scan on the row groups that pass
//! the pushed-down filters' statistics. Row groups whose statistics prove the
//! filters can never match are skipped (their rows are NULL-filled by the
//! caller — they are guaranteed to be rejected by the row-level filters).
static void ComputeRowGroupWindow(ClientContext &context, AlignedGroupScanState &g, idx_t local_start,
                                  idx_t local_end, const PartInfo &part,
                                  const vector<AlignedGroupFilter> &group_filters) {
	g.rg_window.clear();
	g.rg_plan.clear();
	g.rg_skip.clear();
	idx_t offset = 0;
	idx_t window_start = DConstants::INVALID_INDEX;
	g.rg_window_rows = 0;
	idx_t pq = 0; // parquet stream position (accumulated over READ row groups only)
	for (idx_t i = 0; i < g.rg_stats.size(); i++) {
		auto &rg = g.rg_stats[i];
		idx_t rg_start = rg.row_start.IsValid() ? rg.row_start.GetIndex() : offset;
		offset = rg_start + rg.count;
		if (rg_start + rg.count > local_start && rg_start < local_end) {
			if (window_start == DConstants::INVALID_INDEX) {
				window_start = rg_start;
			}
			g.rg_window_rows += rg.count;
			idx_t seg_start = MaxValue<idx_t>(rg_start, local_start) - window_start;
			idx_t seg_end = MinValue<idx_t>(rg_start + rg.count, local_end) - window_start;
			bool skip = false;
			if (rg.partition_row_group) {
				for (auto &gf : group_filters) {
					auto it = std::find(part.columns.begin(), part.columns.end(), gf.column_name);
					if (it == part.columns.end()) {
						// Column absent in this part (schema evolution): no pruning
						continue;
					}
					auto file_idx = it - part.columns.begin();
					auto stats = rg.partition_row_group->GetColumnStatistics(StorageIndex(file_idx));
					if (!stats) {
						continue;
					}
					auto res = gf.filter->CheckStatistics(*stats);
					if (res == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
					    res == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
						skip = true;
						break;
					}
				}
			}
			if (skip) {
				g.rg_skip.emplace_back(seg_start, seg_end - seg_start);
			} else {
				g.rg_window.push_back(i);
				// flow_off = offset within the RG where the wanted segment starts
				idx_t flow_off = MaxValue<idx_t>(rg_start, local_start) - rg_start;
				g.rg_plan.push_back({pq, rg.count, flow_off, seg_start, seg_end - seg_start});
				pq += rg.count;
			}
		}
	}
	g.rg_plan_rows = pq;
	if (g.rg_window.empty()) {
		if (g.rg_skip.empty()) {
			throw IOException("Aligned table: no row groups cover rows [%llu, %llu) of the current part (alignment "
			                  "violation)",
			                  local_start, local_end);
		}
		// Every row group in this window is stats-skipped: no parquet scan needed
		g.scan_state.reset();
		g.rg_window_start = window_start;
		g.parquet_pos = 0;
		g.rg_seg_idx = 0;
		g.rg_window_valid = true;
		return;
	}
	g.rg_window_start = window_start;
	g.parquet_pos = 0;
	g.rg_seg_idx = 0;
	g.scan_state = make_uniq<ParquetReaderScanState>();
	g.reader->InitializeScan(context, *g.scan_state, g.rg_window);
	// Fresh chunk per window: parquet strings are zero-copy references into the
	// window's page buffers. Reusing a chunk across windows (with its VectorCache
	// Reset cycle) leaves stale string_t pointers that crash on refill.
	g.chunk = make_uniq<DataChunk>();
	g.chunk->Initialize(context, g.read_types);
	g.rg_window_valid = true;
}

//! Fills rows [window_start, window_start + count) of one group directly into
//! the output DataChunk at [output_offset, output_offset + count).
static void ScanGroupWindow(ClientContext &context, const AlignedTableBindData &bind, idx_t group_idx,
                            AlignedScanLocalState &lstate, idx_t window_start, idx_t count, DataChunk &output,
                            idx_t output_offset, const vector<idx_t> &projected_pos, const vector<PartInfo> &parts,
                            const vector<AlignedGroupFilter> &group_filters) {
	auto &group = bind.plan.groups[group_idx];
	auto &g = lstate.groups[group_idx];

	idx_t placed = 0;
	idx_t cursor = window_start;
	while (placed < count) {
		if (g.part_idx >= parts.size()) {
			throw IOException("Aligned table '%s': group '%s' has no data at row %llu but the table declares %llu "
			                  "rows (alignment violation)",
			                  bind.plan.table.name, group.manifest.group, cursor, bind.total_rows);
		}
		auto &part = parts[g.part_idx];
		idx_t part_end = part.start_row + part.row_count;
		if (cursor >= part_end) {
			// Move to the next part (zero-row parts are skipped here); the
			// row-group window belongs to this part and must not be reused
			g.part_idx++;
			g.part_ready = false;
			g.rg_window_valid = false;
			continue;
		}
		if (!g.part_ready) {
			OpenPart(context, bind, group_idx, g.part_idx, g, projected_pos, parts);
		}

		idx_t local_start = cursor - part.start_row;
		idx_t need = MinValue<idx_t>(count - placed, part_end - cursor);
		idx_t local_end = local_start + need;

		// Recompute the row-group window only when the current one no longer
		// covers the range or its parquet stream is exhausted
		if (!g.rg_window_valid || (g.rg_plan_rows > 0 && g.parquet_pos >= g.rg_plan_rows) ||
		    local_start < g.rg_window_start || local_end > g.rg_window_start + g.rg_window_rows) {
			ComputeRowGroupWindow(context, g, local_start, local_end, part, group_filters);
		}

		idx_t w_start = local_start - g.rg_window_start;
		idx_t w_end = local_end - g.rg_window_start;

		// NULL-fill the rows of stats-skipped row groups within [w_start, w_end).
		// These rows can never match the pushed-down filters, so their values
		// are irrelevant (they are rejected by the row-level filters later).
		idx_t skip_rows = 0;
		for (auto &skip : g.rg_skip) {
			idx_t fill_from = MaxValue<idx_t>(skip.first, w_start);
			idx_t fill_to = MinValue<idx_t>(skip.first + skip.second, w_end);
			if (fill_from >= fill_to) {
				continue;
			}
			skip_rows += fill_to - fill_from;
			for (auto out_pos : g.out_positions) {
				auto &vec = output.data[out_pos];
				vec.SetVectorType(VectorType::FLAT_VECTOR);
				auto &mask = FlatVector::Validity(vec);
				for (idx_t r = fill_from; r < fill_to; r++) {
					mask.SetInvalid(output_offset + placed + (r - w_start));
				}
			}
		}
		idx_t read_need = need - skip_rows;

		// Read rows from the parquet window and copy vectors into the output chunk.
		// Chunks cover consecutive parquet-window rows [pos, pos + c); the plan
		// maps them to window-local rows (skipped segments are not in the stream).
		idx_t segment_pos = 0;
		while (segment_pos < read_need) {
			auto res = g.reader->Scan(context, *g.scan_state, *g.chunk);
			auto async_type = res.GetResultType();
			if (async_type == AsyncResultType::FINISHED || async_type == AsyncResultType::BLOCKED) {
				throw IOException("Aligned table '%s' group '%s': parquet scan ended early at row %llu (alignment "
				                  "violation)",
				                  bind.plan.table.name, group.manifest.group, cursor + segment_pos);
			}
			idx_t chunk_rows = g.chunk->size();
			if (chunk_rows == 0) {
				// Parquet Scan returns an empty HAVE_MORE_OUTPUT chunk once per
				// row-group switch (setup call) — skip it, not an error
				continue;
			}
			// Map the parquet stream position to the window position via the
			// read plan. A chunk never crosses a row-group boundary, hence
			// never crosses a plan segment. Rows of the RG that lie outside
			// the wanted window (before flow_off) are not part of the segment.
			while (g.rg_seg_idx < g.rg_plan.size() &&
			       g.parquet_pos >= g.rg_plan[g.rg_seg_idx].flow_start + g.rg_plan[g.rg_seg_idx].flow_len) {
				g.rg_seg_idx++;
			}
			if (g.rg_seg_idx >= g.rg_plan.size()) {
				throw IOException("Aligned table '%s' group '%s': parquet scan produced rows beyond the planned "
				                  "window (alignment violation)",
				                  bind.plan.table.name, group.manifest.group);
			}
			auto &seg = g.rg_plan[g.rg_seg_idx];
			idx_t rg_off = g.parquet_pos - seg.flow_start; // offset within the RG
			idx_t valid_from = MaxValue<idx_t>(seg.flow_off, rg_off);
			idx_t valid_to = MinValue<idx_t>(seg.flow_off + seg.win_len, rg_off + chunk_rows);
			if (valid_from >= valid_to) {
				// Entire chunk lies outside the wanted window — discard
				g.parquet_pos += chunk_rows;
				continue;
			}
			idx_t win_pos = seg.win_start + (valid_from - seg.flow_off);
			idx_t copy_from = MaxValue<idx_t>(w_start, win_pos);
			if (copy_from >= win_pos + (valid_to - valid_from)) {
				// Entire chunk lies before the wanted range — discard
				g.parquet_pos += chunk_rows;
				continue;
			}
			idx_t copy_count = MinValue<idx_t>(w_end, win_pos + (valid_to - valid_from)) - copy_from;
			copy_count = MinValue<idx_t>(copy_count, read_need - segment_pos);
			if (copy_count == 0) {
				g.parquet_pos += chunk_rows;
				continue;
			}
			// Chunk-local source offset: window row `copy_from` maps to RG row
			// flow_off + (copy_from - win_start), and the chunk starts at RG row
			// rg_off. (copy_from - win_pos) is only equal when the window starts
			// at the RG start; for mid-RG windows (flow_off > 0) it copies the
			// wrong rows — e.g. window [1096,2048) of a 2048-row RG with
			// rg_off=0 must start at chunk row 1096, not 0.
			idx_t src_offset = seg.flow_off + (copy_from - seg.win_start) - rg_off;
			idx_t dst_offset = output_offset + placed + (copy_from - w_start);
			for (idx_t i = 0; i < g.read_cols.size(); i++) {
				// NOTE: the 5-arg VectorOperations::Copy signature is
				// (source, target, source_count, source_offset, target_offset)
				// where source_count is the EXCLUSIVE end index and the number
				// of copied rows is source_count - source_offset.
				VectorOperations::Copy(g.chunk->data[i], output.data[g.out_positions[i]], src_offset + copy_count,
				                       src_offset, dst_offset);
			}
			segment_pos += copy_count;
			g.parquet_pos += chunk_rows;
		}

		// Columns absent from this part read as NULL (schema evolution, contract §8)
		for (auto out_pos : g.missing_positions) {
			auto &vec = output.data[out_pos];
			vec.SetVectorType(VectorType::FLAT_VECTOR);
			auto &mask = FlatVector::Validity(vec);
			for (idx_t r = 0; r < need; r++) {
				mask.SetInvalid(output_offset + placed + r);
			}
		}

		placed += need;
		cursor += need;
	}
}

//===----------------------------------------------------------------------===//
// Row-level filter application (Phase 3)
//===----------------------------------------------------------------------===//

static void ApplyRowFilters(ClientContext &context, DataChunk &chunk, vector<AlignedRowFilter> &filters) {
	if (filters.empty()) {
		return;
	}
	idx_t count = chunk.size();
	if (count == 0) {
		return;
	}
	SelectionVector sel;
	sel.Initialize(count);
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(i, i);
	}
	idx_t approved = count;
	for (auto &rf : filters) {
		UnifiedVectorFormat vdata;
		chunk.data[rf.projected_pos].ToUnifiedFormat(count, vdata);
		ColumnSegment::FilterSelection(sel, chunk.data[rf.projected_pos], vdata, *rf.filter, *rf.state, count,
		                               approved);
		if (approved == 0) {
			break;
		}
	}
	if (approved != count) {
		chunk.Slice(sel, approved);
	}
}

//===----------------------------------------------------------------------===//
// Scan
//===----------------------------------------------------------------------===//

void AlignedScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<AlignedTableBindData>();
	auto &gstate = data.global_state->Cast<AlignedScanGlobalState>();
	auto &lstate = data.local_state->Cast<AlignedScanLocalState>();

	// Advance to the next active interval when the current one is exhausted
	// (partition pruning, Phase 3: pruned partitions are skipped entirely)
	while (gstate.interval_idx < gstate.active_intervals.size() &&
	       gstate.next_row >= gstate.active_intervals[gstate.interval_idx].second) {
		gstate.interval_idx++;
		gstate.next_row = gstate.interval_idx < gstate.active_intervals.size()
		                      ? gstate.active_intervals[gstate.interval_idx].first
		                      : gstate.next_row;
	}
	if (gstate.interval_idx >= gstate.active_intervals.size()) {
		output.SetCardinality(0);
		return;
	}
	idx_t interval_end = gstate.active_intervals[gstate.interval_idx].second;
	idx_t chunk_rows = MinValue<idx_t>(STANDARD_VECTOR_SIZE, interval_end - gstate.next_row);

	// With pushed-down filters or filter-column removal we assemble into a
	// scratch chunk (which we own and reset), then reference it into the output.
	// The executor reuses the output chunk across calls; slicing it in place
	// would leave dictionary vectors behind and corrupt the next call.
	bool use_scratch = !gstate.row_filters.empty() || !gstate.projection_ids.empty();
	auto &target = use_scratch ? *lstate.scratch : output;
	if (use_scratch) {
		target.Reset();
	}
	target.SetCardinality(chunk_rows);

	// All active groups fill their requested columns for the same logical row
	// range (no JOIN, no concat). Inactive groups are never opened (projection
	// pushdown, Phase 2).
	for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
		if (!gstate.group_active[gi]) {
			continue;
		}
		if (bind.plan.groups[gi].parts.empty()) {
			// Empty group: contributes no columns
			continue;
		}
		const auto &parts = gstate.kept_parts[gi].empty() ? bind.plan.groups[gi].parts : gstate.kept_parts[gi];
		ScanGroupWindow(context, bind, gi, lstate, gstate.next_row, chunk_rows, target, 0, gstate.projected_pos,
		                parts, gstate.group_filters[gi]);
	}
	gstate.next_row += chunk_rows;

	// Apply the pushed-down filters to the assembled chunk
	if (!gstate.row_filters.empty()) {
		ApplyRowFilters(context, target, gstate.row_filters);
	}

	// Produce the final output chunk
	if (use_scratch) {
		if (gstate.projection_ids.empty()) {
			output.Reference(target);
		} else {
			output.ReferenceColumns(target, gstate.projection_ids);
		}
	}
}

unique_ptr<NodeStatistics> AlignedCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = bind_data->Cast<AlignedTableBindData>();
	return make_uniq<NodeStatistics>(data.total_rows, data.total_rows);
}

} // namespace duckdb
