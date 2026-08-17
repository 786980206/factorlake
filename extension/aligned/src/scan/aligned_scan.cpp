#include "scan/aligned_scan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "parquet_reader.hpp"

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
	ParquetReaderScanState scan_state;
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
	DataChunk chunk;                 // parquet output chunk (read columns only)
};

struct AlignedScanGlobalState : public GlobalTableFunctionState {
	idx_t total_rows = 0;
	idx_t next_row = 0; // sequential cursor (Phase 4 replaces this with an atomic task cursor)
};

struct AlignedScanLocalState : public LocalTableFunctionState {
	vector<AlignedGroupScanState> groups;
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
static void ResolveColumnTypes(ClientContext &context, TablePlan &plan, vector<string> &names,
                               vector<LogicalType> &types) {
	for (auto &group : plan.groups) {
		// Open the first part once (if any) as the type source for most columns
		unique_ptr<ParquetReader> first_reader;
		if (!group.parts.empty()) {
			first_reader = OpenPartReader(context, group.parts[0], plan.table.name, group.manifest.group);
		}
		for (auto &col : group.column_order) {
			LogicalType col_type;
			bool found = false;
			if (first_reader) {
				auto it = std::find(group.parts[0].columns.begin(), group.parts[0].columns.end(), col);
				if (it != group.parts[0].columns.end()) {
					col_type = first_reader->columns[it - group.parts[0].columns.begin()].type;
					found = true;
				}
			}
			if (!found) {
				// Schema evolution: the column only exists in later parts
				for (idx_t pi = 1; pi < group.parts.size() && !found; pi++) {
					auto &part = group.parts[pi];
					auto it = std::find(part.columns.begin(), part.columns.end(), col);
					if (it == part.columns.end()) {
						continue;
					}
					auto reader = OpenPartReader(context, part, plan.table.name, group.manifest.group);
					col_type = reader->columns[it - part.columns.begin()].type;
					found = true;
				}
			}
			if (!found) {
				throw IOException("Aligned table '%s' group '%s': column '%s' is declared but not found in any part",
				                  plan.table.name, group.manifest.group, col);
			}
			group.output_positions.push_back(names.size());
			names.push_back(col);
			types.push_back(col_type);
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
// Global / local state
//===----------------------------------------------------------------------===//

unique_ptr<GlobalTableFunctionState> AlignedInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<AlignedTableBindData>();
	auto result = make_uniq<AlignedScanGlobalState>();
	result->total_rows = bind.total_rows;
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> AlignedInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *gstate) {
	auto &bind = input.bind_data->Cast<AlignedTableBindData>();
	auto result = make_uniq<AlignedScanLocalState>();
	result->groups.resize(bind.plan.groups.size());
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Per-part setup
//===----------------------------------------------------------------------===//

static void OpenPart(ClientContext &context, AlignedTableBindData &bind, idx_t group_idx, idx_t part_idx,
                     AlignedGroupScanState &g) {
	auto &group = bind.plan.groups[group_idx];
	auto &part = group.parts[part_idx];

	g.part_idx = part_idx;
	g.reader = OpenPartReader(context, part, bind.plan.table.name, group.manifest.group);

	// Build the read mapping in group column order
	g.read_cols.clear();
	g.out_positions.clear();
	g.read_types.clear();
	g.missing_positions.clear();
	for (idx_t i = 0; i < group.column_order.size(); i++) {
		auto &col = group.column_order[i];
		auto it = std::find(part.columns.begin(), part.columns.end(), col);
		if (it == part.columns.end()) {
			g.missing_positions.push_back(group.output_positions[i]);
			continue;
		}
		auto file_idx = it - part.columns.begin();
		g.read_cols.push_back(file_idx);
		g.out_positions.push_back(group.output_positions[i]);
		g.read_types.push_back(g.reader->columns[file_idx].type);
	}

	// Columns to read from the parquet file (file-local indices)
	for (auto file_idx : g.read_cols) {
		g.reader->column_ids.push_back(MultiFileLocalColumnId(file_idx));
	}

	g.chunk.Initialize(context, g.read_types);

	// Row group statistics (exact per-RG row counts)
	g.rg_stats.clear();
	g.reader->GetPartitionStats(g.rg_stats);
	if (g.rg_stats.empty()) {
		throw IOException("Aligned table '%s' group '%s' part '%s': file contains no row groups",
		                  bind.plan.table.name, group.manifest.group, part.part_name);
	}
	g.rg_window.clear();
	g.rg_window_start = 0;
	g.rg_window_rows = 0;
	g.rg_window_pos = 0;
	g.part_ready = true;
}

//! Computes the row-group window covering [local_start, local_end) of the
//! current part and initializes the parquet scan on exactly those row groups.
static void ComputeRowGroupWindow(ClientContext &context, AlignedGroupScanState &g, idx_t local_start,
                                  idx_t local_end) {
	g.rg_window.clear();
	idx_t offset = 0;
	idx_t window_start = DConstants::INVALID_INDEX;
	g.rg_window_rows = 0;
	for (idx_t i = 0; i < g.rg_stats.size(); i++) {
		auto &rg = g.rg_stats[i];
		idx_t rg_start = rg.row_start.IsValid() ? rg.row_start.GetIndex() : offset;
		offset = rg_start + rg.count;
		if (rg_start + rg.count > local_start && rg_start < local_end) {
			if (window_start == DConstants::INVALID_INDEX) {
				window_start = rg_start;
			}
			g.rg_window.push_back(i);
			g.rg_window_rows += rg.count;
		}
	}
	if (g.rg_window.empty()) {
		throw IOException("Aligned table: no row groups cover rows [%llu, %llu) of the current part (alignment "
		                  "violation)",
		                  local_start, local_end);
	}
	g.rg_window_start = window_start;
	g.rg_window_pos = 0;
	g.reader->InitializeScan(context, g.scan_state, g.rg_window);
}

//! Fills rows [window_start, window_start + count) of one group directly into
//! the output DataChunk at [output_offset, output_offset + count).
static void ScanGroupWindow(ClientContext &context, AlignedTableBindData &bind, idx_t group_idx,
                            AlignedScanLocalState &lstate, idx_t window_start, idx_t count, DataChunk &output,
                            idx_t output_offset) {
	auto &group = bind.plan.groups[group_idx];
	auto &g = lstate.groups[group_idx];

	idx_t placed = 0;
	idx_t cursor = window_start;
	while (placed < count) {
		if (g.part_idx >= group.parts.size()) {
			throw IOException("Aligned table '%s': group '%s' has no data at row %llu but the table declares %llu "
			                  "rows (alignment violation)",
			                  bind.plan.table.name, group.manifest.group, cursor, bind.total_rows);
		}
		auto &part = group.parts[g.part_idx];
		idx_t part_end = part.start_row + part.row_count;
		if (cursor >= part_end) {
			// Move to the next part (zero-row parts are skipped here)
			g.part_idx++;
			g.part_ready = false;
			continue;
		}
		if (!g.part_ready) {
			OpenPart(context, bind, group_idx, g.part_idx, g);
		}

		idx_t local_start = cursor - part.start_row;
		idx_t need = MinValue<idx_t>(count - placed, part_end - cursor);
		idx_t local_end = local_start + need;

		// Recompute the row-group window only when the current one no longer covers the range
		if (g.rg_window.empty() || g.rg_window_pos >= g.rg_window_rows || local_start < g.rg_window_start ||
		    local_end > g.rg_window_start + g.rg_window_rows) {
			ComputeRowGroupWindow(context, g, local_start, local_end);
		}

		// Read rows [local_start, local_end) from the window and copy vectors into the output chunk.
		// Chunks from the parquet reader cover consecutive window-local rows [pos, pos + c).
		idx_t segment_pos = 0;
		while (segment_pos < need) {
			auto res = g.reader->Scan(context, g.scan_state, g.chunk);
			if (res != SourceResultType::HAVE_MORE_OUTPUT) {
				throw IOException("Aligned table '%s' group '%s': parquet scan ended early at row %llu (alignment "
				                  "violation)",
				                  bind.plan.table.name, group.manifest.group, cursor + segment_pos);
			}
			idx_t chunk_rows = g.chunk.size();
			idx_t w_start = local_start - g.rg_window_start;
			idx_t w_end = local_end - g.rg_window_start;
			idx_t copy_from = MaxValue<idx_t>(w_start, g.rg_window_pos);
			if (copy_from >= g.rg_window_pos + chunk_rows) {
				// Entire chunk lies before the wanted range — discard
				g.rg_window_pos += chunk_rows;
				continue;
			}
			idx_t copy_count = MinValue<idx_t>(w_end, g.rg_window_pos + chunk_rows) - copy_from;
			copy_count = MinValue<idx_t>(copy_count, need - segment_pos);
			if (copy_count == 0) {
				g.rg_window_pos += chunk_rows;
				continue;
			}
			idx_t src_offset = copy_from - g.rg_window_pos;
			idx_t dst_offset = output_offset + placed + segment_pos;
			for (idx_t i = 0; i < g.read_cols.size(); i++) {
				VectorOperations::Copy(g.chunk.data[i], output.data[g.out_positions[i]], copy_count, src_offset,
				                       dst_offset);
			}
			segment_pos += copy_count;
			g.rg_window_pos += chunk_rows;
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
// Scan
//===----------------------------------------------------------------------===//

OperatorResultType AlignedScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<AlignedTableBindData>();
	auto &gstate = data.global_state->Cast<AlignedScanGlobalState>();
	auto &lstate = data.local_state->Cast<AlignedScanLocalState>();

	if (gstate.next_row >= gstate.total_rows) {
		return OperatorResultType::FINISHED;
	}
	idx_t chunk_rows = MinValue<idx_t>(STANDARD_VECTOR_SIZE, gstate.total_rows - gstate.next_row);
	output.SetCardinality(chunk_rows);

	// All groups fill their columns for the same logical row range (no JOIN, no concat)
	for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
		if (bind.plan.groups[gi].parts.empty()) {
			// Empty group: contributes no columns
			continue;
		}
		ScanGroupWindow(context, bind, gi, lstate, gstate.next_row, chunk_rows, output, 0);
	}
	gstate.next_row += chunk_rows;
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

unique_ptr<NodeStatistics> AlignedCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = bind_data->Cast<AlignedTableBindData>();
	return make_uniq<NodeStatistics>(data.total_rows, data.total_rows);
}

} // namespace duckdb
