#include "scan/aligned_scan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/config.hpp"
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

//! Per-group, per-thread scan state. Row windows are driven by the global
//! cursor; all groups are read in lockstep over the same logical row range
//! (the core alignment invariant — never verified by key comparison).
struct AlignedGroupScanState {
	idx_t part_idx = 0;
	bool part_ready = false;
	unique_ptr<ParquetReader> reader;
	// Case-insensitive column name → file index map (built once per OpenPart
	// to avoid O(cols²) linear scans via std::find_if).
	case_insensitive_map_t<idx_t> col_map;
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
	vector<pair<idx_t, idx_t>> dup_out_positions; // (source_out_pos, dup_out_pos) for
	                                              // qualified-alias columns (same
	                                              // parquet column, copy after read)
	// Parquet output chunk (read columns only). Held by unique_ptr because
	// DataChunk is neither copyable nor movable (which would make this state
	// immovable and break vector<AlignedGroupScanState> reallocation).
	unique_ptr<DataChunk> chunk;
	// row-group plan of the current window. RGs whose statistics
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
	vector<pair<idx_t, idx_t>> rg_skip;     // skipped segments (win_start, win_len) —NULL filled
	idx_t rg_plan_rows = 0;                 // total stream rows of the read segments
	idx_t parquet_pos = 0;                  // rows consumed from the parquet stream (current window)
	idx_t rg_seg_idx = 0;                   // current read segment index
	bool rg_window_valid = false;           // the current window still covers the wanted range
	// Carry-over (parallel scan perf): when a window's last read vector over-reads
	// past the wanted range, the surplus rows are copied into `carry_chunk`
	// (window coordinates [carry_from, carry_from+carry_count)). The next chunk
	// drains the carry first, then reads more. This keeps the window reusable
	// across chunks even when the wanted rows don't align to parquet vector
	// boundaries (2048 rows), avoiding a full window re-initialization.
	unique_ptr<DataChunk> carry_chunk;
	idx_t carry_win_start_row = 0; // window coordinate of carry_chunk row 0
	idx_t carry_count = 0;         // rows held in carry_chunk (contiguous from carry_win_start_row)
};

//! A pushed-down filter applied to one of this table's columns. The filter
//! definition (const TableFilter *) is shared; the filter STATE is per-thread
//! (FilterSelection may mutate state, so concurrent scans need their own).
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
	// Enable parallel scans (parallel scan): the default MaxThreads() is 1, which
	// would schedule the whole scan as a single pipeline task. Overriding it
	// lets the scheduler run one task per thread; the shared cursor hands each
	// task contiguous row ranges.
	idx_t MaxThreads() const override {
		return GlobalTableFunctionState::MAX_THREADS;
	}
	// Parallel scan (parallel scan): the shared cursor {interval_idx, next_row} is
	// protected by cursor_lock. Pipeline threads claim CONTIGUOUS RANGES of
	// CLAIM_RANGE rows (not single chunks): within a claimed range the group
	// scans are sequential, so row-group windows are reused without
	// re-initialization; re-positioning (InitializeScan + discard to the
	// wanted start) only happens at range boundaries.
	// CLAIM_RANGE = one Row Group (131072 rows). Larger ranges reduce cursor-lock
	// contention and per-claim OpenPart overhead at high thread counts; a range
	// equal to the parquet Row Group size means each thread typically processes
	// one complete RG before re-claiming, maximizing window reuse.
	static constexpr idx_t CLAIM_RANGE = 64 * STANDARD_VECTOR_SIZE;
	mutex cursor_lock;
	idx_t interval_idx = 0;
	idx_t next_row = 0; // cursor within the current active interval
	// Projection pushdown : full schema position -> output chunk position
	//   projected_pos: effective OUTPUT chunk position (projection_ids rank)
	//   scratch_pos:   column_ids index (the filter-path scratch chunk)
	vector<idx_t> projected_pos;
	vector<idx_t> scratch_pos;
	// Virtual rowid column (catalog integration): effective output position
	// (INVALID when pruned). Lets the binder treat the scan as a real base table.
	idx_t rowid_pos = DConstants::INVALID_INDEX;
	// Duplicate column requests (e.g. SELECT a, a): (from, to) positions of the
	// same full-schema column, per index space; filled after the group scan by
	// copying the first occurrence.
	vector<pair<idx_t, idx_t>> dup_copies_scratch;
	vector<pair<idx_t, idx_t>> dup_copies_out;
	// Per-group flag: does this group contribute any requested column?
	vector<bool> group_active;
	// Filters : shared filter definitions (states are per-thread, see
	// AlignedScanLocalState::row_filters)
	vector<AlignedRowFilter> row_filters;
	vector<vector<AlignedGroupFilter>> group_filters; // per group
	// Partition pruning : kept parts per group; empty = keep all from bind
	vector<vector<PartInfo>> kept_parts;
	// Active row intervals: intersection of kept-part intervals over active
	// groups. The scan cursor only walks these intervals (pruned partitions
	// are skipped entirely —all groups agree on the inactive ranges).
	vector<pair<idx_t, idx_t>> active_intervals;
	// Filter-column removal (projection_ids): which scanned columns the final
	// output keeps (indexes into the scanned column list)
	vector<idx_t> projection_ids;
	// Virtual partition column: scratch slot (column_ids index) and effective
	// output position (INVALID when pruned or not requested).
	idx_t partition_scratch = DConstants::INVALID_INDEX;
	idx_t partition_pos = DConstants::INVALID_INDEX;
};

struct AlignedScanLocalState : public LocalTableFunctionState {
	vector<AlignedGroupScanState> groups;
	// Per-thread filter states (parallel scans share the global
	// filter definitions but need their own mutable state)
	vector<AlignedRowFilter> row_filters;
	// The thread's currently claimed contiguous range [range_next, range_end)
	// (parallel scan, range claiming)
	idx_t range_next = 0;
	idx_t range_end = 0;
	// Scratch chunk for the filter/projection path (assemble -> filter -> reference)
	unique_ptr<DataChunk> scratch;
};

//===----------------------------------------------------------------------===//
// Column type resolution (bind time)
//===----------------------------------------------------------------------===//

//! Builds the table schema (names/types in table order) and fills each group's
//! output_positions. Types are resolved from the group's last part's footer.
//! Column-name rules (contract §2.2), implemented in two passes:
//!  pass 1: count which (non-index) groups contain each bare column name;
//!  pass 2: register columns:
//!   - index columns: bare names (index is authoritative);
//!   - non-index columns whose name also exists in index: ignored entirely
//!     (index shadow);
//!   - non-index columns whose name exists in >= 2 non-index groups: registered
//!     under the qualified name "lv1.lv2.col_name" in EVERY such group (the
//!     bare name is not registered at all; querying it reports "column not
//!     found");
//!   - other non-index columns (unique): bare name. When register_qualified
//!     is true, ALSO register the qualified "lv1.lv2.col" alias as a second
//!     output position — both names reference the same parquet column (the
//!     scan reads it once and copies to both positions). This is for the
//!     table-function path where COLUMNS('lv1.lv2.col') regex can reference
//!     the qualified name. The catalog (ATTACH) path sets register_qualified
//!     = false to avoid duplicate columns in DESCRIBE / SELECT *.
static void ResolveColumnTypes(ClientContext &context, TablePlan &plan, vector<string> &names,
                               vector<LogicalType> &types, bool register_qualified = true) {
	// The index group is always at position 0 (BuildTablePlan guarantees this).
	const idx_t index_group = 0;
	(void)IndexGroup(plan); // validates non-empty
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
		// Ensure output_positions_qualified is sized the same as column_order.
		group.output_positions_qualified.assign(group.column_order.size(), DConstants::INVALID_INDEX);
		// Column types come from the group schema (the group's last part's
		// footer, captured at plan time). The group schema is the LAST part's
		// schema — older parts lacking evolution columns read as NULL at scan
		// time (contract §8). O(1) lookup, no per-column part scan.
		for (idx_t ci = 0; ci < group.column_order.size(); ci++) {
			auto &col = group.column_order[ci];
			auto &col_type = group.schema_types[ci];
			if (gi == index_group) {
				// index columns are authoritative: bare names only
				group.output_positions.push_back(names.size());
				names.push_back(col);
				types.push_back(col_type);
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
				types.push_back(col_type);
			} else {
				// Unique non-index column: register the bare name. When
				// register_qualified is true (table-function path), also
				// register the qualified "lv1.lv2.col" alias as a second
				// output position — both reference the same parquet column.
				group.output_positions.push_back(names.size());
				names.push_back(col);
				types.push_back(col_type);

				if (register_qualified) {
					auto qualified = group.lv1 + "." + group.lv2 + "." + col;
					group.output_positions_qualified[ci] = names.size();
					names.push_back(qualified);
					types.push_back(col_type);
				}
			}
		}
	}
}

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

static unique_ptr<FunctionData> AlignedBindInternal(ClientContext &context, const string &root, const string &table,
                                                    vector<LogicalType> &return_types, vector<string> &names,
                                                    bool add_partition_col = true,
                                                    bool register_qualified = true,
                                                    const string &group_filter = string()) {
	auto result = make_uniq<AlignedTableBindData>();
	BuildTablePlan(context, root, table, result->plan);

	// If a group filter is specified, keep only the index group + matching
	// non-index groups. This reduces the output schema to only the columns
	// of the requested group(s) and avoids opening irrelevant parquet files.
	if (!group_filter.empty()) {
		// Parse comma-separated group names.
		case_insensitive_set_t filter_set;
		auto parts = StringUtil::Split(group_filter, ',');
		for (auto &p : parts) {
			StringUtil::Trim(p);
			if (!p.empty()) {
				filter_set.insert(p);
			}
		}
		// The index group (groups[0]) is always kept — it is the canonical
		// row space and carries the primary key columns (symbol, date).
		vector<GroupPlan> kept;
		kept.push_back(std::move(result->plan.groups[0]));
		for (idx_t gi = 1; gi < result->plan.groups.size(); gi++) {
			auto &g = result->plan.groups[gi];
			if (filter_set.count(g.manifest.group) > 0) {
				kept.push_back(std::move(g));
			}
		}
		result->plan.groups = std::move(kept);
	}
	ResolveColumnTypes(context, result->plan, result->names, result->types, register_qualified);

	// Virtual partition column: derived from the index group's partition
	// template prefix ("year", "month", or "date"). Added as a VARCHAR column
	// after all real columns, materialized from the part's partition_key
	// during scan. Skipped when the name collides with an existing column
	// (e.g. "date" template when the index group already has a "date" column).
	// Also skipped for the catalog bind path (DML schema must match the real
	// physical columns).
	if (add_partition_col && !result->plan.groups.empty()) {
		auto &index_group = result->plan.groups[0];
		if (!index_group.manifest.partitioning.empty()) {
			auto &tmpl = index_group.manifest.partitioning[0].template_str;
			string part_col = PartitionColumnName(tmpl);
			if (!part_col.empty()) {
				// Check for name collision with existing columns (case-insensitive)
				bool collision = false;
				for (auto &n : result->names) {
					if (StringUtil::CIEquals(n, part_col)) {
						collision = true;
						break;
					}
				}
				if (!collision) {
					result->partition_col_name = part_col;
					result->partition_col_idx = result->names.size();
					result->names.push_back(part_col);
					result->types.push_back(LogicalType::VARCHAR);
				}
			}
		}
	}

	result->total_rows = result->plan.row_count;
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

unique_ptr<FunctionData> AlignedBindForCatalog(ClientContext &context, const string &root, const string &table,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	// Catalog path (ATTACH): no virtual partition column, no qualified aliases.
	// The catalog's column schema is used by DuckDB's binder for SELECT * —
	// qualified aliases would appear as confusing duplicate columns that
	// can't be referenced via normal SQL syntax anyway.
	return AlignedBindInternal(context, root, table, return_types, names, false, false);
}
unique_ptr<FunctionData> AlignedBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	// aligned_scan(table_name [, group_filter], root=...)
	if (input.inputs.size() < 1 || input.inputs.size() > 2) {
		throw BinderException("aligned_scan: expected (table_name) or (table_name, group_filter)");
	}
	string table = StringValue::Get(input.inputs[0]);
	string group_filter = input.inputs.size() >= 2 ? StringValue::Get(input.inputs[1]) : string();

	auto root_it = input.named_parameters.find("root");
	const Value *root_param = (root_it != input.named_parameters.end()) ? &root_it->second : nullptr;
	string root = ResolveDataRoot(context, root_param, "aligned_scan");
	return AlignedBindInternal(context, root, table, return_types, names, true, true, group_filter);
}

//===----------------------------------------------------------------------===//
// Filter handling 
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

// Forward declaration: defined after ApplyRowFilters, used in AlignedInitGlobal.
static void ApplyPartitionColumnPruning(const AlignedTableBindData &bind, const ConstantFilter &filter,
                                         vector<PartInfo> &kept);

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

unique_ptr<GlobalTableFunctionState> AlignedInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<AlignedTableBindData>();
	auto result = make_uniq<AlignedScanGlobalState>();
	result->total_rows = bind.total_rows;
	result->projection_ids = input.projection_ids;

	// Enable parquet prefetching and metadata caching for local files.
	// DuckDB's ParquetReader defaults to prefetching only remote files.
	// For aligned_scan, which reads local parquet files, prefetching
	// overlaps file I/O with ZSTD decompression (~5% read speedup).
	// Metadata caching avoids re-reading parquet footers across queries
	// (4 partitions × N groups = significant footer I/O on repeated scans).
	// We only set these if the user hasn't explicitly set them.
	{
		Value prefetch_val;
		if (context.TryGetCurrentSetting("prefetch_all_parquet_files", prefetch_val)) {
			if (!prefetch_val.GetValue<bool>()) {
				auto &db_config = DBConfig::GetConfig(context);
				db_config.SetOptionByName("prefetch_all_parquet_files", Value::BOOLEAN(true));
			}
		}
		Value meta_cache_val;
		if (context.TryGetCurrentSetting("parquet_metadata_cache", meta_cache_val)) {
			if (!meta_cache_val.GetValue<bool>()) {
				auto &db_config = DBConfig::GetConfig(context);
				db_config.SetOptionByName("parquet_metadata_cache", Value::BOOLEAN(true));
			}
		}
	}

	// Projection pushdown: input.column_ids are the requested columns (indexes
	// into the full bind schema). An empty list means no columns at all are
	// requested (e.g. count(*)) —the output chunk then has no vectors and the
	// scan only reports cardinality.
	result->projected_pos.assign(bind.names.size(), DConstants::INVALID_INDEX);
	result->scratch_pos.assign(bind.names.size(), DConstants::INVALID_INDEX);
	// Effective OUTPUT position of every column_ids entry. With filter_prune
	// the executor allocates the output chunk by projection_ids (a subset of
	// column_ids), so output position != column_ids index in general. The
	// scratch chunk (filter path) is indexed by column_ids position instead,
	// hence two parallel maps.
	vector<idx_t> out_of_colids(input.column_ids.size(), DConstants::INVALID_INDEX);
	if (input.projection_ids.empty()) {
		for (idx_t i = 0; i < input.column_ids.size(); i++) {
			out_of_colids[i] = i;
		}
	} else {
		for (idx_t j = 0; j < input.projection_ids.size(); j++) {
			auto p = input.projection_ids[j];
			if (p < out_of_colids.size()) {
				out_of_colids[p] = j;
			}
		}
	}
	vector<char> requested(bind.names.size(), 0);
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		auto col_id = input.column_ids[i];
		auto out_pos = out_of_colids[i];
		if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
			// Virtual rowid = logical row number.
			// Output slot = effective position (INVALID when pruned).
			result->rowid_pos = out_pos;
			continue;
		}
		// Virtual partition column: track its position, but do NOT route it
		// to any group (it is materialized from the index group's part list).
		if (bind.partition_col_idx != DConstants::INVALID_INDEX && col_id == bind.partition_col_idx) {
			result->partition_scratch = i;
			result->partition_pos = out_pos;
			requested[col_id] = 1; // makes the index group active
			continue;
		}
		if (col_id >= bind.names.size()) {
			throw InternalException("aligned_scan: column id %llu out of range (schema has %llu columns)", col_id,
			                        bind.names.size());
		}
		requested[col_id] = 1;
		if (result->scratch_pos[col_id] == DConstants::INVALID_INDEX) {
			result->scratch_pos[col_id] = i;
		} else {
			result->dup_copies_scratch.emplace_back(result->scratch_pos[col_id], i);
		}
		if (out_pos == DConstants::INVALID_INDEX) {
			continue; // pruned from the scan output (filter-only column)
		}
		if (result->projected_pos[col_id] != DConstants::INVALID_INDEX) {
			// Same column requested twice (e.g. SELECT a, a): replicate the
			// filled vector after the group scan.
			result->dup_copies_out.emplace_back(result->projected_pos[col_id], out_pos);
			continue;
		}
		result->projected_pos[col_id] = out_pos;
	}

	// Determine which groups actually need to be opened
	result->group_active.assign(bind.plan.groups.size(), false);
	for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
		auto &group = bind.plan.groups[gi];
		for (auto full_pos : group.output_positions) {
			// A group is active when ANY of its columns is requested
			// including filter-only columns that are pruned from the output.
			if (full_pos != DConstants::INVALID_INDEX && requested[full_pos]) {
				result->group_active[gi] = true;
				break;
			}
		}
		if (!result->group_active[gi]) {
			// Also check qualified-alias positions.
			for (auto full_pos : group.output_positions_qualified) {
				if (full_pos != DConstants::INVALID_INDEX && requested[full_pos]) {
					result->group_active[gi] = true;
					break;
				}
			}
		}
	}
	// The virtual partition column is materialized from the index group's
	// part list, so the index group must be active when it is requested.
	if (bind.partition_col_idx != DConstants::INVALID_INDEX && requested[bind.partition_col_idx]) {
		if (!bind.plan.groups.empty()) {
			result->group_active[0] = true;
		}
	}

	// Filters : input.filters keys are projected positions
	result->group_filters.resize(bind.plan.groups.size());
	result->kept_parts.resize(bind.plan.groups.size());
	if (input.filters) {
		for (auto &entry : input.filters->filters) {
			auto key = entry.first;
			auto &filter = *entry.second;
			if (key >= input.column_ids.size()) {
				throw InternalException("aligned_scan: filter column id %llu out of range", key);
			}
			auto full_col = input.column_ids[key];
			// The filter definition is shared by all threads; each thread
			// initializes its own TableFilterState (see AlignedInitLocal).
			result->row_filters.push_back({key, &filter, nullptr});

			// Virtual partition column filter: prune the index group's parts
			// by matching the partition_key value (e.g. "year=2024" -> "2024").
			if (bind.partition_col_idx != DConstants::INVALID_INDEX && full_col == bind.partition_col_idx) {
				if (filter.filter_type == TableFilterType::CONSTANT_COMPARISON) {
					ApplyPartitionColumnPruning(bind, filter.Cast<ConstantFilter>(), result->kept_parts[0]);
				}
				continue; // not a group-level filter
			}

			// Find the group owning this column (for RG pruning + partition pruning)
			for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
				auto &group = bind.plan.groups[gi];
				for (idx_t li = 0; li < group.column_order.size(); li++) {
					if (group.output_positions[li] == full_col ||
					    (li < group.output_positions_qualified.size() &&
					     group.output_positions_qualified[li] == full_col)) {
						result->group_filters[gi].push_back({group.column_order[li], &filter});
						ApplyPartitionPruning(bind, gi, group.column_order[li], filter, result->kept_parts[gi]);
						break;
					}
				}
			}
		}
	}

	// Active row intervals. Tables are partition-aligned: the index group
	// covers the full row space and defines the total row count, so it is the
	// authoritative pruning scope. Only groups whose partition keys EQUAL the
	// index's keys (full coverage) share the same row range and can participate
	// in the interval intersection; a partition-subset group is scanned for its
	// own parts and NULL-fills the missing ranges, so its pruned ranges must
	// NOT remove rows from the global scan range.
	vector<pair<idx_t, idx_t>> active;
	bool any_active = false;
	for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
		if (!result->group_active[gi]) {
			continue;
		}
		auto &group = bind.plan.groups[gi];
		if (!group.full_coverage) {
			continue; // partition-subset group: does not restrict the scan range
		}
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
		// No columns requested at all (e.g. count(*) without filters), or only
		// partition-subset groups are active: the scan covers the full row
		// space (subset groups NULL-fill their missing ranges).
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
	// Per-thread filter states (the global state only carries the
	// filter definitions; FilterSelection may mutate state, so each pipeline
	// thread initializes its own copy here)
	if (input.filters) {
		for (auto &entry : input.filters->filters) {
			auto key = entry.first;
			auto &filter = *entry.second;
			auto state = TableFilterState::Initialize(context.client, filter);
			result->row_filters.push_back({key, &filter, std::move(state)});
		}
	}
	// Scratch chunk for the filter/projection path: one vector per scanned column
	if (!input.projection_ids.empty() || input.filters) {
		vector<LogicalType> scanned_types;
		for (auto col_id : input.column_ids) {
			// Virtual rowid slot: BIGINT placeholder (filled post-assembly).
			scanned_types.push_back(col_id == COLUMN_IDENTIFIER_ROW_ID ? LogicalType::BIGINT : bind.types[col_id]);
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
	g.reader = make_uniq<ParquetReader>(context, OpenFileInfo(part.path), ParquetOptions(context));

	// Build the column name → index map for this part (O(cols) once,
	// replaces O(cols²) std::find_if scans in the loop below and in
	// ComputeRowGroupWindow).
	g.col_map.clear();
	for (idx_t fi = 0; fi < g.reader->columns.size(); fi++) {
		g.col_map[g.reader->columns[fi].name] = fi;
	}

	// Defensive check (v6): the open file's footer row count must equal the
	// self-describing value parsed from the file name. The plan's row counts
	// come from file names ONLY (no footer reads), so a mismatch means the
	// file was written without the v6 naming contract or was truncated —
	// fail fast instead of misaligning the file-name-derived row intervals.
	if (g.reader->NumRows() != part.row_count) {
		throw IOException("Aligned table '%s' group '%s' part '%s': file holds %llu rows but its name declares %llu "
		                  "rows (self-describing part-name contract)",
		                  bind.plan.table_name, group.manifest.group, part.part_name, g.reader->NumRows(),
		                  part.row_count);
	}

	// Build the read mapping in group column order; only requested columns
	// are read from the parquet file (projection pushdown). File columns are
	// resolved against the OPEN reader's schema (the plan only stores the
	// group schema — the last part's — so older schema-evolution parts find
	// their columns here and newer columns are NULL-filled as missing).
	g.read_cols.clear();
	g.out_positions.clear();
	g.read_types.clear();
	g.missing_positions.clear();
	g.dup_out_positions.clear();
	for (idx_t i = 0; i < group.column_order.size(); i++) {
		if (group.output_positions[i] == DConstants::INVALID_INDEX) {
			// Shadowed duplicate column (see ResolveColumnTypes): skip entirely
			continue;
		}
		auto projected = projected_pos[group.output_positions[i]];
		auto projected_q = DConstants::INVALID_INDEX;
		if (i < group.output_positions_qualified.size() &&
		    group.output_positions_qualified[i] != DConstants::INVALID_INDEX) {
			projected_q = projected_pos[group.output_positions_qualified[i]];
		}
		// If neither the bare nor the qualified alias is requested, skip.
		if (projected == DConstants::INVALID_INDEX && projected_q == DConstants::INVALID_INDEX) {
			continue;
		}
		auto &col = group.column_order[i];
		auto it = g.col_map.find(col);
		idx_t file_idx;
		bool found = true;
		if (it == g.col_map.end()) {
			found = false;
			file_idx = 0; // placeholder
		} else {
			file_idx = it->second;
			// Cross-part type consistency: the plan schema uses the last part's
			// types; every part must agree on a column's type (schema evolution
			// only adds/removes columns, never changes a type).
			if (group.schema_types[i] != g.reader->columns[file_idx].type) {
				throw InternalException(
				    "Aligned table '%s' group '%s' column '%s' has type %s in this part "
				    "but %s in the group schema (cross-part type mismatch is not allowed)",
				    bind.plan.table_name, group.manifest.group, col,
				    g.reader->columns[file_idx].type.ToString(), group.schema_types[i].ToString());
			}
		}
		// Read the parquet column once; route to whichever output positions
		// are requested (bare name, qualified alias, or both).
		idx_t first_out = DConstants::INVALID_INDEX;
		if (projected != DConstants::INVALID_INDEX) {
			if (!found) {
				g.missing_positions.push_back(projected);
			} else if (first_out == DConstants::INVALID_INDEX) {
				first_out = projected;
				g.read_cols.push_back(file_idx);
				g.out_positions.push_back(projected);
				g.read_types.push_back(g.reader->columns[file_idx].type);
			} else {
				g.dup_out_positions.emplace_back(first_out, projected);
			}
		}
		if (projected_q != DConstants::INVALID_INDEX) {
			if (!found) {
				g.missing_positions.push_back(projected_q);
			} else if (first_out == DConstants::INVALID_INDEX) {
				first_out = projected_q;
				g.read_cols.push_back(file_idx);
				g.out_positions.push_back(projected_q);
				g.read_types.push_back(g.reader->columns[file_idx].type);
			} else {
				g.dup_out_positions.emplace_back(first_out, projected_q);
			}
		}
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
		                  bind.plan.table_name, group.manifest.group, part.part_name);
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
//! caller —they are guaranteed to be rejected by the row-level filters).
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
			bool skip = false;
			if (rg.partition_row_group) {
				for (auto &gf : group_filters) {
					auto it = g.col_map.find(gf.column_name);
					if (it == g.col_map.end()) {
						// Column absent in this part (schema evolution): no pruning
						continue;
					}
					auto file_idx = it->second;
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
				// Record the ENTIRE row group in window coordinates so that a
				// later chunk wanting a different portion of the same stats-
				// skipped RG is still NULL-filled (and never tries to read a
				// NULL scan_state/chunk). Clamping to the current wanted
				// segment would leave the rest of the RG uncovered, crashing
				// with "dereference unique_ptr that is NULL".
				g.rg_skip.emplace_back(rg_start - window_start, rg.count);
			} else {
				g.rg_window.push_back(i);
				// The plan segment covers the ENTIRE row group (flow_off = 0,
				// win range = the full RG). The copy loop clamps to the wanted
				// [w_start, w_end) itself, so a window can be reused for any
				// later range inside the same RG(s) —clamping the segment to
				// the originally-wanted range instead would discard every
				// vector of a later chunk (rows outside the segment) until the
				// stream ends, failing with "scan ended early".
				g.rg_plan.push_back({pq, rg.count, 0, rg_start - window_start, rg.count});
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
	g.carry_win_start_row = 0;
	g.carry_count = 0;
	g.scan_state = make_uniq<ParquetReaderScanState>();
	g.reader->InitializeScan(context, *g.scan_state, g.rg_window);
	// Fresh chunk per window: parquet strings are zero-copy references into the
	// window's page buffers. Reusing a chunk across windows (with its VectorCache
	// Reset cycle) leaves stale string_t pointers that crash on refill.
	g.chunk = make_uniq<DataChunk>();
	g.chunk->Initialize(context, g.read_types);
	g.carry_chunk = make_uniq<DataChunk>();
	g.carry_chunk->Initialize(context, g.read_types);
	g.rg_window_valid = true;
}

//! NULL-fills window rows [from, to) of every requested column of `group`.
//! Row `r` of the window maps to output row `output_offset + (r - window_start)`.
static void NullFillGroupRange(DataChunk &output, idx_t output_offset, const GroupPlan &group,
                               const vector<idx_t> &projected_pos, idx_t window_start, idx_t from, idx_t to) {
	if (from >= to) {
		return;
	}
	idx_t first = output_offset + (from - window_start);
	idx_t last = output_offset + (to - window_start);
	for (idx_t i = 0; i < group.column_order.size(); i++) {
		if (group.output_positions[i] == DConstants::INVALID_INDEX) {
			continue;
		}
		auto projected = projected_pos[group.output_positions[i]];
		if (projected != DConstants::INVALID_INDEX) {
			auto &vec = output.data[projected];
			vec.SetVectorType(VectorType::FLAT_VECTOR);
			auto &mask = FlatVector::Validity(vec);
			// First SetInvalid initializes the mask (AllValid → explicit bits);
			// subsequent calls use SetInvalidUnsafe to skip the branch.
			mask.SetInvalid(first);
			for (idx_t r = first + 1; r < last; r++) {
				mask.SetInvalidUnsafe(r);
			}
		}
		// Also NULL-fill the qualified alias output position. This must
		// happen independently of the bare name: a query may request ONLY
		// the qualified alias (e.g. COLUMNS('lv1.lv2.col')) while the bare
		// name's projected position is INVALID_INDEX. Skipping here would
		// leave the qualified output vector uninitialized for missing
		// partitions (garbage data).
		if (i < group.output_positions_qualified.size() &&
		    group.output_positions_qualified[i] != DConstants::INVALID_INDEX) {
			auto projected_q = projected_pos[group.output_positions_qualified[i]];
			if (projected_q != DConstants::INVALID_INDEX) {
				auto &qvec = output.data[projected_q];
				qvec.SetVectorType(VectorType::FLAT_VECTOR);
				auto &qmask = FlatVector::Validity(qvec);
				qmask.SetInvalid(first);
				for (idx_t r = first + 1; r < last; r++) {
					qmask.SetInvalidUnsafe(r);
				}
			}
		}
	}
}

//! Fills rows [window_start, window_start + count) of one group directly into
//! the output DataChunk at [output_offset, output_offset + count).
static void ScanGroupWindow(ClientContext &context, const AlignedTableBindData &bind, idx_t group_idx,
                            AlignedScanLocalState &lstate, idx_t window_start, idx_t count, DataChunk &output,
                            idx_t output_offset, const vector<idx_t> &projected_pos, const vector<PartInfo> &parts,
                            const vector<AlignedGroupFilter> &group_filters, bool can_fast_copy) {
	auto &group = bind.plan.groups[group_idx];
	auto &g = lstate.groups[group_idx];

	idx_t placed = 0;
	idx_t cursor = window_start;
	while (placed < count) {
		if (parts.empty()) {
			// No parts at all (empty or fully pruned group): every row of this
			// window reads as NULL (partition-aligned contract).
			NullFillGroupRange(output, output_offset, group, projected_pos, window_start, window_start,
			                   window_start + count);
			placed = count;
			break;
		}
		// Advance past exhausted parts.
		while (g.part_idx < parts.size() &&
		       cursor >= parts[g.part_idx].start_row + parts[g.part_idx].row_count) {
			g.part_idx++;
			g.part_ready = false;
			g.rg_window_valid = false;
			g.carry_count = 0;
		}
		if (g.part_idx >= parts.size()) {
			// Window extends past the last part (missing partition suffix):
			// NULL-fill the remainder.
			NullFillGroupRange(output, output_offset, group, projected_pos, window_start, cursor,
			                   window_start + count);
			placed = count;
			break;
		}
		if (cursor < parts[g.part_idx].start_row) {
			// Parallel rewind or a gap: position on the last part whose
			// start_row <= cursor (scan forward from the beginning; cursor is
			// monotonic within a thread's claimed range).
			g.part_idx = 0;
			while (g.part_idx + 1 < parts.size() && parts[g.part_idx + 1].start_row <= cursor) {
				g.part_idx++;
			}
			g.part_ready = false;
			g.rg_window_valid = false;
			g.carry_count = 0;
		}
		if (cursor < parts[g.part_idx].start_row) {
			// A partition the group does not cover (before the first part):
			// NULL-fill up to the next part's start (or the window end).
			idx_t fill_end = MinValue<idx_t>(parts[g.part_idx].start_row, window_start + count);
			idx_t fill_len = fill_end - cursor;
			NullFillGroupRange(output, output_offset, group, projected_pos, window_start, cursor, fill_end);
			placed += fill_len;
			cursor += fill_len;
			continue;
		}
		if (cursor >= parts[g.part_idx].start_row + parts[g.part_idx].row_count) {
			// The part ends before the cursor: a partition the group does not
			// cover sits between parts. NULL-fill up to the next part's start
			// (or the window end); after the last part this terminates below.
			idx_t next_start = g.part_idx + 1 < parts.size() ? parts[g.part_idx + 1].start_row
			                                                 : window_start + count;
			idx_t fill_end = MinValue<idx_t>(next_start, window_start + count);
			idx_t fill_len = fill_end - cursor;
			NullFillGroupRange(output, output_offset, group, projected_pos, window_start, cursor, fill_end);
			placed += fill_len;
			cursor += fill_len;
			continue;
		}
		auto &part = parts[g.part_idx];
		idx_t part_end = part.start_row + part.row_count;
		if (!g.part_ready) {
			OpenPart(context, bind, group_idx, g.part_idx, g, projected_pos, parts);
		}

		idx_t local_start = cursor - part.start_row;
		idx_t need = MinValue<idx_t>(count - placed, part_end - cursor);
		idx_t local_end = local_start + need;

		// Recompute the row-group window only when the current one no longer
		// covers the range or its parquet stream is exhausted. A window can be
		// reused for any forward range inside it: the carry buffer holds rows
		// already read but not yet placed, so the stream never over-consumes
		// past wanted rows (see the read loop). Only a backward re-position
		// (parallel) or stream exhaustion forces a recompute.
		if (!g.rg_window_valid || (g.rg_plan_rows > 0 && g.parquet_pos >= g.rg_plan_rows) ||
		    local_start < g.rg_window_start || local_end > g.rg_window_start + g.rg_window_rows) {
			ComputeRowGroupWindow(context, g, local_start, local_end, part, group_filters);
		} else {
			// The window covers the wanted range. Reuse is safe for any forward
			// claim: the carry buffer + the current stream position together
			// hold every row from carry_win_start_row (or the stream position)
			// onward. Only a backward re-position (parallel) before those rows
			// needs a recompute. We conservatively recompute when the wanted
			// start is before the first un-placed stream row.
			//
			// If rg_plan is empty the entire row-group window is stats-skipped
			// (all rows are NULL-filled and never match the pushed-down filter).
			// There is nothing to read, so the stream position is irrelevant and
			// no recompute is needed —accessing rg_plan[0] here would be OOB.
			if (g.carry_count > 0) {
				idx_t unplaced_from = g.carry_win_start_row;
				if (local_start - g.rg_window_start < unplaced_from) {
					ComputeRowGroupWindow(context, g, local_start, local_end, part, group_filters);
				}
			} else if (!g.rg_plan.empty()) {
				// no carry: the stream has read up to parquet_pos (window
				// coordinate via the current segment)
				const auto &seg0 = g.rg_plan[g.rg_seg_idx];
				idx_t unplaced_from = seg0.win_start + (g.parquet_pos - seg0.flow_start);
				if (local_start - g.rg_window_start < unplaced_from) {
					ComputeRowGroupWindow(context, g, local_start, local_end, part, group_filters);
				}
			}
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
				idx_t first = output_offset + placed + (fill_from - w_start);
				idx_t last = output_offset + placed + (fill_to - w_start);
				mask.SetInvalid(first);
				for (idx_t r = first + 1; r < last; r++) {
					mask.SetInvalidUnsafe(r);
				}
			}
			for (auto &dup : g.dup_out_positions) {
				auto &vec = output.data[dup.second];
				vec.SetVectorType(VectorType::FLAT_VECTOR);
				auto &mask = FlatVector::Validity(vec);
				idx_t first = output_offset + placed + (fill_from - w_start);
				idx_t last = output_offset + placed + (fill_to - w_start);
				mask.SetInvalid(first);
				for (idx_t r = first + 1; r < last; r++) {
					mask.SetInvalidUnsafe(r);
				}
			}
		}
		idx_t read_need = need - skip_rows;
		idx_t segment_pos = 0; // rows of this chunk's wanted range already placed

		// Drain any carried rows (over-read from the previous chunk) first.
		// carry_chunk holds window rows [carry_win_start_row,
		// carry_win_start_row + carry_count) at its own indices [0, carry_count).
		if (g.carry_count > 0) {
			idx_t c_from = MaxValue<idx_t>(w_start, g.carry_win_start_row);
			idx_t c_to = MinValue<idx_t>(w_end, g.carry_win_start_row + g.carry_count);
			if (c_from < c_to) {
				idx_t c_src = c_from - g.carry_win_start_row; // row in carry_chunk
				idx_t c_count = c_to - c_from;
				idx_t c_dst = output_offset + placed + (c_from - w_start);
				for (idx_t i = 0; i < g.read_cols.size(); i++) {
					VectorOperations::Copy(g.carry_chunk->data[i], output.data[g.out_positions[i]], c_src + c_count, c_src,
					                       c_dst);
				}
				// Replicate to qualified-alias output positions.
				for (auto &dup : g.dup_out_positions) {
					VectorOperations::Copy(output.data[dup.first], output.data[dup.second],
					                       c_dst + c_count, c_dst, c_dst);
				}
				segment_pos += c_count;
				if (c_to > g.carry_win_start_row) {
					// drop the drained prefix; the tail (if any) moves to the front
					idx_t consumed = c_to - g.carry_win_start_row;
					g.carry_win_start_row += consumed;
					g.carry_count -= consumed;
					if (g.carry_count > 0) {
						for (idx_t i = 0; i < g.read_cols.size(); i++) {
							VectorOperations::Copy(g.carry_chunk->data[i], g.carry_chunk->data[i],
							                       consumed + g.carry_count, consumed, 0);
						}
					}
					g.carry_chunk->SetCardinality(g.carry_count);
				}
			}
		}

		// Read rows from the parquet window and copy vectors into the output chunk.
		// Chunks cover consecutive parquet-window rows [pos, pos + c); the plan
		// maps them to window-local rows (skipped segments are not in the stream).
		while (segment_pos < read_need) {
auto res = g.reader->Scan(context, *g.scan_state, *g.chunk);
		auto async_type = res.GetResultType();
			if (async_type == AsyncResultType::FINISHED) {
				throw IOException("Aligned table '%s' group '%s': parquet scan ended early at row %llu (alignment "
				                  "violation)",
				                  bind.plan.table_name, group.manifest.group, cursor + segment_pos);
			}
			if (async_type == AsyncResultType::BLOCKED) {
				// Async not ready (e.g. object storage) — retry.
				// For local files this never fires; for remote storage it
				// means the scan needs to yield and be retried.
				continue;
			}
			idx_t chunk_rows = g.chunk->size();
			if (chunk_rows == 0) {
				// Parquet Scan returns an empty HAVE_MORE_OUTPUT chunk once per
				// row-group switch (setup call) —skip it, not an error
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
				                  bind.plan.table_name, group.manifest.group);
			}
			auto &seg = g.rg_plan[g.rg_seg_idx];
			idx_t rg_off = g.parquet_pos - seg.flow_start; // offset within the RG
			idx_t valid_from = MaxValue<idx_t>(seg.flow_off, rg_off);
			idx_t valid_to = MinValue<idx_t>(seg.flow_off + seg.win_len, rg_off + chunk_rows);
			if (valid_from >= valid_to) {
				// Entire chunk lies outside the wanted window —discard
				g.parquet_pos += chunk_rows;
				continue;
			}
			idx_t win_pos = seg.win_start + (valid_from - seg.flow_off);
			idx_t win_begin = win_pos; // window coord of vector[0]
			idx_t win_end = seg.win_start + (valid_to - seg.flow_off); // window coord of vector end
			// Overlap of the vector's window range [win_pos, win_end) with the
			// wanted range [w_start, w_end). copy_from >= copy_to means no
			// overlap (the vector lies entirely before OR entirely after the
			// wanted rows) —discard. Clamp against BOTH ends so copy_count can
			// never underflow.
			idx_t copy_from = MaxValue<idx_t>(w_start, win_pos);
			idx_t copy_to = MinValue<idx_t>(w_end, win_end);
			if (copy_from >= copy_to) {
				g.parquet_pos += chunk_rows;
				continue;
			}
			idx_t copy_count = copy_to - copy_from;
			copy_count = MinValue<idx_t>(copy_count, read_need - segment_pos);
			if (copy_count == 0) {
				g.parquet_pos += chunk_rows;
				continue;
			}
			// Vector-local source offset: window row `copy_from` maps to RG row
			// flow_off + (copy_from - win_start), and the vector starts at RG
			// row rg_off. This gives the row index within THIS vector.
			idx_t src_offset = seg.flow_off + (copy_from - seg.win_start) - rg_off;
			idx_t dst_offset = output_offset + placed + (copy_from - w_start);
			for (idx_t i = 0; i < g.read_cols.size(); i++) {
				auto &src_vec = g.chunk->data[i];
				auto &dst_vec = output.data[g.out_positions[i]];
				// Fast path: FLAT→FLAT full-vector memcpy for fixed-size
				// numeric types. Only when can_fast_copy (non-scratch path,
				// no filters/projection_ids) and the entire vector maps to
				// output position 0 with no offset/shrink.
				// VectorOperations::Copy uses a per-element loop with
				// sel.get_index() indirection — memcpy is ~4× faster.
				if (can_fast_copy &&
				    src_vec.GetVectorType() == VectorType::FLAT_VECTOR &&
				    dst_vec.GetVectorType() == VectorType::FLAT_VECTOR &&
				    src_offset == 0 && copy_count == chunk_rows &&
				    dst_offset == 0) {
					auto &src_type = src_vec.GetType();
					auto phys = src_type.InternalType();
					idx_t type_size = GetTypeIdSize(phys);
					// Exclude VARCHAR (string_t has pointer members into
					// g.chunk's buffer that dangle after next Scan()).
					// Exclude BOOL, nested types, and types > 8 bytes.
					if (type_size > 0 && type_size <= 8 &&
					    src_type.id() != LogicalTypeId::VARCHAR &&
					    phys != PhysicalType::BOOL) {
						auto *src_data = FlatVector::GetData(src_vec);
						auto *dst_data = FlatVector::GetData(dst_vec);
						// Guard against all-NULL vectors where the data buffer
						// may be null or uninitialized.
						if (src_data && dst_data &&
						    copy_count > 0 && copy_count <= STANDARD_VECTOR_SIZE) {
							memcpy(dst_data, src_data, copy_count * type_size);
							auto &src_mask = FlatVector::Validity(src_vec);
							if (src_mask.IsMaskSet()) {
								auto &dst_mask = FlatVector::Validity(dst_vec);
								for (idx_t r = 0; r < copy_count; r++) {
									if (!src_mask.RowIsValidUnsafe(r)) {
										dst_mask.SetInvalidUnsafe(r);
									}
								}
							}
							continue;
						}
					}
				}
				// NOTE: the 5-arg VectorOperations::Copy signature is
				// (source, target, source_count, source_offset, target_offset)
				// where source_count is the EXCLUSIVE end index and the number
				// of copied rows is source_count - source_offset.
				VectorOperations::Copy(src_vec, dst_vec, src_offset + copy_count,
				                       src_offset, dst_offset);
			}
			// Replicate to qualified-alias output positions.
			for (auto &dup : g.dup_out_positions) {
				VectorOperations::Copy(output.data[dup.first], output.data[dup.second],
				                       dst_offset + copy_count, dst_offset, dst_offset);
			}
			segment_pos += copy_count;
			g.parquet_pos += chunk_rows;

			// If this vector over-read past the wanted range, its surplus rows
			// become the carry for the next chunk. Carry only happens on the
			// last useful vector (the loop would otherwise still need more
			// rows); copy the surplus into carry_chunk so the window stays
			// reusable without re-initialization.
			if (win_end > w_end) {
				idx_t surplus_count = win_end - w_end;
				// window row `w_end` maps to vector row
				// (seg.flow_off + (w_end - seg.win_start) - rg_off)
				idx_t v_src = seg.flow_off + (w_end - seg.win_start) - rg_off;
				g.carry_count = surplus_count;
				g.carry_win_start_row = w_end;
				g.carry_chunk->Reset();
				g.carry_chunk->SetCardinality(surplus_count);
				for (idx_t i = 0; i < g.read_cols.size(); i++) {
					VectorOperations::Copy(g.chunk->data[i], g.carry_chunk->data[i], v_src + surplus_count, v_src, 0);
				}
			}
		}

		// Columns absent from this part read as NULL (schema evolution, contract §8)
		for (auto out_pos : g.missing_positions) {
			auto &vec = output.data[out_pos];
			vec.SetVectorType(VectorType::FLAT_VECTOR);
			auto &mask = FlatVector::Validity(vec);
			idx_t first = output_offset + placed;
			idx_t last = first + need;
			mask.SetInvalid(first);
			for (idx_t r = first + 1; r < last; r++) {
				mask.SetInvalidUnsafe(r);
			}
		}

		placed += need;
		cursor += need;
	}
}

//===----------------------------------------------------------------------===//
// Row-level filter application 
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
// Partition column materialization
//===----------------------------------------------------------------------===//

//! Fills the virtual partition column for the rows [chunk_start, chunk_start +
//! count) using the index group's part list. Each part's partition_key (e.g.
//! "year=2024") is stripped to its value ("2024") and written as a VARCHAR.
//! Rows not covered by any part (missing partition) are NULL-filled.
static void FillPartitionColumn(const AlignedTableBindData &bind, idx_t partition_out_pos,
                                idx_t chunk_start, idx_t count, DataChunk &target) {
	if (partition_out_pos == DConstants::INVALID_INDEX) {
		return; // partition column not requested
	}
	auto &index_group = bind.plan.groups[0];
	const auto &parts = index_group.parts;
	auto &vec = target.data[partition_out_pos];
	vec.SetVectorType(VectorType::FLAT_VECTOR);
	auto data = FlatVector::GetData<string_t>(vec);
	auto &validity = FlatVector::Validity(vec);

	idx_t part_idx = 0;
	// Advance to the first part overlapping chunk_start
	while (part_idx < parts.size() && parts[part_idx].start_row + parts[part_idx].row_count <= chunk_start) {
		part_idx++;
	}

	// Cache the per-part partition value once (avoids O(rows) string allocations).
	string_t cached_value;
	idx_t cached_part_idx = DConstants::INVALID_INDEX;

	for (idx_t i = 0; i < count; i++) {
		idx_t row = chunk_start + i;
		// Advance past exhausted parts
		while (part_idx < parts.size() && parts[part_idx].start_row + parts[part_idx].row_count <= row) {
			part_idx++;
		}
		if (part_idx >= parts.size() || row < parts[part_idx].start_row) {
			// Row not covered by any part (missing partition): NULL
			validity.SetInvalid(i);
			continue;
		}
		// Extract the value portion from the partition key — cached per part.
		if (part_idx != cached_part_idx) {
			cached_part_idx = part_idx;
			cached_value = StringVector::AddString(vec, PartitionKeyValue(parts[part_idx].partition_key));
		}
		data[i] = cached_value;
	}
}

//! Prunes the index group's parts by a string constant filter on the virtual
//! partition column. The filter value is compared against the value portion
//! of each part's partition_key (e.g. "2024" matches "year=2024").
static void ApplyPartitionColumnPruning(const AlignedTableBindData &bind, const ConstantFilter &filter,
                                         vector<PartInfo> &kept) {
	auto &index_group = bind.plan.groups[0];
	const auto &base = kept.empty() ? index_group.parts : kept;

	// Extract the comparison value as a string
	auto &filter_value = filter.constant;
	string cmp_str;
	if (filter_value.type().id() == LogicalTypeId::VARCHAR) {
		cmp_str = StringValue::Get(filter_value);
	} else {
		// Non-string filter on a VARCHAR partition column: convert to string
		cmp_str = filter_value.ToString();
	}

	auto cmp = filter.comparison_type;
	if (cmp == ExpressionType::COMPARE_EQUAL) {
		vector<PartInfo> result;
		for (auto &part : base) {
			if (PartitionKeyValue(part.partition_key) == cmp_str) {
				result.push_back(part);
			}
		}
		kept = std::move(result);
	} else if (cmp == ExpressionType::COMPARE_NOTEQUAL) {
		vector<PartInfo> result;
		for (auto &part : base) {
			if (PartitionKeyValue(part.partition_key) != cmp_str) {
				result.push_back(part);
			}
		}
		kept = std::move(result);
	} else {
		// Range comparisons on string partition values (lexicographic)
		// work for year (4-digit) and month (YYYY-MM) formats.
		vector<PartInfo> result;
		for (auto &part : base) {
			string val = PartitionKeyValue(part.partition_key);
			int rc = val.compare(cmp_str);
			bool keep = false;
			switch (cmp) {
			case ExpressionType::COMPARE_GREATERTHAN:
				keep = rc > 0;
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				keep = rc >= 0;
				break;
			case ExpressionType::COMPARE_LESSTHAN:
				keep = rc < 0;
				break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
				keep = rc <= 0;
				break;
			default:
				keep = true; // unknown comparison: keep
				break;
			}
			if (keep) {
				result.push_back(part);
			}
		}
		kept = std::move(result);
	}
}

//===----------------------------------------------------------------------===//
// Scan
//===----------------------------------------------------------------------===//

void AlignedScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<AlignedTableBindData>();
	auto &gstate = data.global_state->Cast<AlignedScanGlobalState>();
	auto &lstate = data.local_state->Cast<AlignedScanLocalState>();

	// With pushed-down filters (or filter-column removal) a whole 2048-row
	// chunk may be rejected by the row-level filters. DuckDB's executor treats a
	// returned 0-cardinality chunk as end-of-scan, so we must NEVER emit an empty
	// chunk mid-stream: loop internally, advancing to the next logical chunk,
	// until we have produced a non-empty chunk or have genuinely exhausted all
	// active intervals (only then return a 0-cardinality chunk to signal EOF).
	while (true) {
		// claim the next contiguous range from the shared cursor (or
		// continue the thread's current range). The lock is held only for the
		// claim; the actual scan runs outside it with this thread's own local
		// state.
		if (lstate.range_next >= lstate.range_end) {
			lock_guard<mutex> lock(gstate.cursor_lock);
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
			idx_t interval_start = gstate.active_intervals[gstate.interval_idx].first;
			if (gstate.next_row < interval_start) {
				gstate.next_row = interval_start;
			}
			idx_t interval_end = gstate.active_intervals[gstate.interval_idx].second;
			idx_t range_end = MinValue<idx_t>(gstate.next_row + AlignedScanGlobalState::CLAIM_RANGE, interval_end);
			lstate.range_next = gstate.next_row;
			lstate.range_end = range_end;
			gstate.next_row = range_end;
		}
		idx_t chunk_start = lstate.range_next;
		idx_t chunk_rows = MinValue<idx_t>(STANDARD_VECTOR_SIZE, lstate.range_end - chunk_start);
		if (chunk_rows == 0) {
			// Current range exhausted; reclaim the next one (loop continues)
			continue;
		}
		lstate.range_next += chunk_rows;

		// With pushed-down filters or filter-column removal we assemble into a
		// scratch chunk (which we own and reset), then reference it into the
		// output. The executor reuses the output chunk across calls; slicing it
		// in place would leave dictionary vectors behind and corrupt the next
		// call.
		bool use_scratch = !lstate.row_filters.empty() || !gstate.projection_ids.empty();
		auto &target = use_scratch ? *lstate.scratch : output;
		// The memcpy fast path writes FLAT data directly (not dictionary slices),
		// so it's safe with the scratch chunk too — as long as there are no
		// row filters (which may shrink the output cardinality mid-vector).
		// memcpy fast path is disabled — it crashes on all-NULL vectors and
		// provides no measurable performance benefit (the real read win was
		// disabling dictionary encoding, not the memcpy path).
		bool can_fast_copy = false;
		// Position map of the assembly target: the scratch chunk is indexed by
		// column_ids position, the executor's output chunk by projection rank.
		const auto &pos_map = use_scratch ? gstate.scratch_pos : gstate.projected_pos;
		if (use_scratch) {
			target.Reset();
		}
		target.SetCardinality(chunk_rows);

		// All active groups fill their requested columns for the same logical
		// row range (no JOIN, no concat). Inactive groups are never opened
		// (projection pushdown).
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			if (!gstate.group_active[gi]) {
				continue;
			}
			if (bind.plan.groups[gi].parts.empty()) {
				// Empty group: contributes no columns
				continue;
			}
			const auto &parts = gstate.kept_parts[gi].empty() ? bind.plan.groups[gi].parts : gstate.kept_parts[gi];
			ScanGroupWindow(context, bind, gi, lstate, chunk_start, chunk_rows, target, 0, pos_map, parts,
			                gstate.group_filters[gi], can_fast_copy);
		}

		// Fill the virtual partition column (e.g. "year" = "2024") from the
		// index group's part list. The value is derived from the part's
		// partition_key, not from parquet data.
		if (bind.partition_col_idx != DConstants::INVALID_INDEX) {
			idx_t part_out_pos = use_scratch ? gstate.partition_scratch : gstate.partition_pos;
			if (part_out_pos != DConstants::INVALID_INDEX) {
				FillPartitionColumn(bind, part_out_pos, chunk_start, chunk_rows, target);
			}
		}

		// Replicate duplicated column requests before filtering so all
		// positions carry the same values.
		auto &dups = use_scratch ? gstate.dup_copies_scratch : gstate.dup_copies_out;
		for (auto &dup : dups) {
			VectorOperations::Copy(target.data[dup.first], target.data[dup.second], chunk_rows, 0, 0);
		}

		// Apply the pushed-down filters to the assembled chunk (per-thread states)
		if (!lstate.row_filters.empty()) {
			ApplyRowFilters(context, target, lstate.row_filters);
		}

		// Produce the final output chunk. If a scratch chunk ends up empty
		// (all rows rejected by the filters), do NOT emit it —advance to the
		// next logical chunk instead (loop continues).
		if (use_scratch) {
			if (target.size() == 0) {
				continue;
			}
			if (gstate.projection_ids.empty()) {
				output.Reference(target);
			} else {
				output.ReferenceColumns(target, gstate.projection_ids);
			}
		}
		// When not using scratch there are no row filters, so the output chunk
		// is always non-empty (chunk_rows > 0); fall through to return.

		// Virtual rowid: absolute logical row number of every surviving row.
		if (gstate.rowid_pos != DConstants::INVALID_INDEX) {
			auto &vec = output.data[gstate.rowid_pos];
			vec.SetVectorType(VectorType::FLAT_VECTOR);
			auto ids = FlatVector::GetData<int64_t>(vec);
			if (!use_scratch || target.ColumnCount() == 0) {
				// Identity mapping over [chunk_start, chunk_start + out_count)
				const idx_t out_count = use_scratch ? target.size() : chunk_rows;
				for (idx_t i = 0; i < out_count; i++) {
					ids[i] = static_cast<int64_t>(chunk_start + i);
				}
			} else {
				// Surviving rows follow the scratch chunk's post-filter selection
				UnifiedVectorFormat vdata;
				target.data[0].ToUnifiedFormat(target.size(), vdata);
				for (idx_t i = 0; i < output.size(); i++) {
					ids[i] = static_cast<int64_t>(chunk_start + vdata.sel->get_index(i));
				}
			}
		}
		return;
	}
}

unique_ptr<NodeStatistics> AlignedCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = bind_data->Cast<AlignedTableBindData>();
	return make_uniq<NodeStatistics>(data.total_rows, data.total_rows);
}

} // namespace duckdb






