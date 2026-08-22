#include "mutator/aligned_mutator.hpp"

#include "catalog/manifest.hpp"
#include "resolver/key_resolver.hpp"
#include "resolver/partition_resolver.hpp"
#include "rewriter/part_rewriter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "parquet_reader.hpp"

#include <algorithm>
#include <map>
#include <set>
#include <utility>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Small JSON / file helpers (same as aligned_writer / aligned_compactor)
//===----------------------------------------------------------------------===//

static void WriteTextFile(FileSystem &fs, const string &path, const string &content) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	// FILE_FLAGS_FILE_CREATE does not truncate an existing file: the old tail
	// would remain past the new content (invalid JSON on rewrite).
	handle->Truncate(0);
	handle->Write(const_cast<char *>(content.c_str()), content.size());
	handle->Sync();
	handle->Close();
}

static string JsonEscape(const string &s) {
	string out;
	for (auto c : s) {
		if (c == '"' || c == '\\') {
			out += '\\';
			out += c;
		} else if (c == '\n') {
			out += "\\n";
		} else {
			out += c;
		}
	}
	return out;
}

static string JsonStringArray(const vector<string> &items) {
	string out = "[";
	for (idx_t i = 0; i < items.size(); i++) {
		if (i > 0) {
			out += ",";
		}
		out += "\"" + JsonEscape(items[i]) + "\"";
	}
	out += "]";
	return out;
}

//! Writes _table.json with only the bootstrap config (groups + partitioning).
//! The manifest is optional and only used for empty-table bootstrap; the
//! writer still writes it back after each commit to preserve the group list
//! and partition templates for the next bootstrap.
static void WriteManifest(FileSystem &fs, const TablePlan &plan) {
	auto &table = plan.table;
	string partitioning;
	if (!table.partitioning.empty()) {
		partitioning = ",\"partitioning\":{";
		bool first_group = true;
		for (auto &entry : table.partitioning) {
			if (!first_group) {
				partitioning += ",";
			}
			first_group = false;
			partitioning += "\"" + JsonEscape(entry.first) + "\":[";
			for (idx_t i = 0; i < entry.second.size(); i++) {
				if (i > 0) {
					partitioning += ",";
				}
				partitioning += "{\"template\":\"" + JsonEscape(entry.second[i].template_str) + "\",\"source\":\"" +
				                JsonEscape(entry.second[i].source) + "\"}";
			}
			partitioning += "]";
		}
		partitioning += "}";
	}
	string manifest = "{\"groups\":" + JsonStringArray(table.groups) + partitioning + "}";
	WriteTextFile(fs, plan.table_path + "/_table.json", manifest);
}

//! In-process transaction counter (starts at 1, increments each call). Not
//! persisted — last_txid was removed from the manifest; the counter is only
//! used for the txid return value and the _tmp/transaction-<txid>/ staging
//! directory name (both transient).
static idx_t NextTransactionId() {
	static idx_t counter = 0;
	return ++counter;
}

//! "group:col1,col2;group2:col3" -> map. Throws on malformed input.
static void ParseMapping(const string &mapping, const string &fn, case_insensitive_map_t<vector<string>> &out) {
	auto groups = StringUtil::Split(mapping, ';');
	for (auto &entry : groups) {
		if (entry.empty()) {
			continue;
		}
		auto colon = entry.find(':');
		if (colon == string::npos) {
			throw BinderException("%s: invalid mapping entry '%s' (expected 'group:col1,col2')", fn, entry);
		}
		string group_name = entry.substr(0, colon);
		StringUtil::Trim(group_name);
		auto columns = StringUtil::Split(entry.substr(colon + 1), ',');
		vector<string> cols;
		for (auto &c : columns) {
			string trimmed = c;
			StringUtil::Trim(trimmed);
			if (!trimmed.empty()) {
				cols.push_back(trimmed);
			}
		}
		if (group_name.empty() || cols.empty()) {
			throw BinderException("%s: invalid mapping entry '%s' (expected 'group:col1,col2')", fn, entry);
		}
		out[group_name] = std::move(cols);
	}
}

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

static void ResolveRoot(ClientContext &context, TableFunctionBindInput &input, const string &fn, string &root) {
	auto entry = input.named_parameters.find("root");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		root = StringValue::Get(entry->second);
	} else {
		Value setting_value;
		if (!context.TryGetCurrentSetting("aligned_data_root", setting_value)) {
			throw BinderException("%s: no data root configured. Pass root='...' or SET aligned_data_root", fn);
		}
		root = StringValue::Get(setting_value);
	}
}

static unique_ptr<FunctionData> MutateBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names,
                                           bool is_delete) {
	auto result = make_uniq<MutateBindData>();
	result->is_delete = is_delete;
	const char *fn = is_delete ? "aligned_delete" : "aligned_upsert";
	size_t expected = is_delete ? 2 : 3;
	if (!is_delete && input.inputs.size() == 2) {
		expected = 2; // mapping is optional: auto-derive from the table schema
	}
	if (input.inputs.size() != expected) {
		throw BinderException("%s: expected (%s)", fn,
		                      is_delete ? "table_name, keys_source"
		                                : "table_name, source_path [, mapping]");
	}
	result->table_name = StringValue::Get(input.inputs[0]);
	result->source_path = StringValue::Get(input.inputs[1]);
	string mapping_str = (is_delete || input.inputs.size() < 3) ? string() : StringValue::Get(input.inputs[2]);

	string root;
	ResolveRoot(context, input, fn, root);
	BuildTablePlan(context, root, result->table_name, result->plan);

	// Materialize manifest-declared groups that have no parts yet (declared in
	// _table.json at create time, first written later). Without this the
	// mutator would reject them as "unknown group"; with it, an UPDATE for
	// existing keys synthesizes the group's first aligned part (all-NULL rows
	// plus the mapped values). The reader keeps ignoring part-less groups
	// until that first write.
	bool table_has_parts = !result->plan.groups.empty() && !result->plan.groups[0].parts.empty();
	if (table_has_parts) {
		for (auto &decl : result->plan.table.groups) {
			bool found = false;
			for (auto &g : result->plan.groups) {
				if (StringUtil::CIEquals(g.manifest.group, decl)) {
					found = true;
					break;
				}
			}
			if (!found) {
				GroupPlan gp;
				gp.manifest.group = decl;
				gp.group_path = result->plan.table_path + "/" + decl;
				result->plan.groups.push_back(std::move(gp));
			}
		}
	}


	// The index group is the first plan group; its primary key columns define
	// the (date, symbol) contract (v7).
	auto &index_group = result->plan.groups[0];
	bool empty_table = index_group.parts.empty();
	result->empty_table = empty_table;


	// Resolve the group mapping (upsert only). When `mapping` is omitted the
	// columns are auto-assigned to the group that already owns them (by name),
	// so the user only needs to pass it for the first write of an empty table
	// or to override the default placement.
	case_insensitive_map_t<vector<string>> mapping;
	result->group_mapping.resize(result->plan.groups.size());
	if (!is_delete) {
		if (mapping_str.empty()) {
			if (empty_table) {
				throw BinderException("%s: mapping is required for the first write of an empty table "
				                      "(it defines the Column Group structure); e.g. "
				                      "'index:date,symbol,close;factor/alpha101:alpha001'",
				                      fn);
			}
			// Auto-derive: each source column → the group whose schema owns it.
			auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(result->source_path), ParquetOptions(context));
			for (auto &col : reader->columns) {
				for (idx_t gi = 0; gi < result->plan.groups.size(); gi++) {
					auto &group = result->plan.groups[gi];
					bool owned = std::any_of(group.column_order.begin(), group.column_order.end(),
					                         [&](const string &n) { return StringUtil::CIEquals(n, col.name); });
					if (owned) {
						result->group_mapping[gi].col_names.push_back(col.name);
						break;
					}
				}
			}
		} else {
			ParseMapping(mapping_str, fn, mapping);
			for (idx_t gi = 0; gi < result->plan.groups.size(); gi++) {
				auto &gm = result->group_mapping[gi];
				auto &group = result->plan.groups[gi];
				auto it = mapping.find(group.manifest.group);
				if (it == mapping.end()) {
					continue; // unmapped group (upsert: NULL rows)
				}
				gm.col_names = it->second;
			}
		}
	}
	// Every mapping entry must name a real group (typos fail fast instead of
	// silently writing nothing for that group).
	for (auto &kv : mapping) {
		bool found = false;
		for (auto &group : result->plan.groups) {
			if (StringUtil::CIEquals(kv.first, group.manifest.group)) {
				found = true;
				break;
			}
		}
		if (!found) {
			throw BinderException("%s: unknown group '%s' in mapping", fn, kv.first);
		}
	}

	// Primary key columns.
	if (empty_table) {
		if (is_delete) {
			throw BinderException("%s: the table '%s' is empty (no index parts) — nothing to delete", fn,
			                      result->table_name);
		}
		// First write: the index mapping's first two columns ARE the key.
		auto &gm = result->group_mapping[0];
		if (gm.col_names.size() < 2) {
			throw BinderException("%s: the index group's mapping must have at least two columns (the primary key "
			                      "date, symbol) for the first write",
			                      fn);
		}
		// The types are only known from the source (no parts yet).
		auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(result->source_path), ParquetOptions(context));
		case_insensitive_map_t<LogicalType> source_schema;
		for (auto &col : reader->columns) {
			source_schema[col.name] = col.type;
		}
		string date_col, symbol_col;
		idx_t date_fields = 0;
		for (idx_t c = 0; c < 2; c++) {
			auto it = source_schema.find(gm.col_names[c]);
			if (it == source_schema.end()) {
				throw BinderException("%s: column '%s' (index group) not found in source '%s'", fn, gm.col_names[c],
				                      result->source_path);
			}
			if (it->second.id() == LogicalTypeId::DATE || it->second.id() == LogicalTypeId::TIMESTAMP) {
				date_fields++;
				date_col = gm.col_names[c];
			} else {
				symbol_col = gm.col_names[c];
			}
		}
		if (date_fields != 1 || symbol_col.empty()) {
			throw BinderException("%s: the index group's first two mapping columns must be the primary key "
			                      "'(date, symbol)' — exactly one DATE/TIMESTAMP field and one symbol column",
			                      fn);
		}
		result->date_col = date_col;
		result->symbol_col = symbol_col;
	} else {
		result->date_col = index_group.partition_source;
		result->symbol_col = index_group.symbol_column;
		if (is_delete) {
			// keys source must carry the key columns (matched by name)
			auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(result->source_path), ParquetOptions(context));
			bool has_date = false, has_symbol = false;
			for (auto &col : reader->columns) {
				if (StringUtil::CIEquals(col.name, result->date_col)) {
					has_date = true;
				}
				if (StringUtil::CIEquals(col.name, result->symbol_col)) {
					has_symbol = true;
				}
			}
			if (!has_date || !has_symbol) {
				throw BinderException("%s: keys source '%s' must contain the primary key columns ('%s', '%s')", fn,
				                      result->source_path, result->date_col, result->symbol_col);
			}
			result->source_rows = reader->NumRows();
		} else {
			// index mapping must include the key columns
			auto &gm = result->group_mapping[0];
			bool has_date = false, has_symbol = false;
			for (auto &col : gm.col_names) {
				if (StringUtil::CIEquals(col, result->date_col)) {
					has_date = true;
				}
				if (StringUtil::CIEquals(col, result->symbol_col)) {
					has_symbol = true;
				}
			}
			if (!has_date || !has_symbol) {
				throw BinderException("%s: the index group's mapping must include the primary key columns ('%s', "
				                      "'%s')",
				                      fn, result->date_col, result->symbol_col);
			}
		}
	}

	if (!is_delete) {
		// Validate the source parquet: all mapped columns exist; the partition
		// source column must be DATE or TIMESTAMP (v6 contract).
		auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(result->source_path), ParquetOptions(context));
		case_insensitive_map_t<LogicalType> source_schema;
		for (auto &col : reader->columns) {
			source_schema[col.name] = col.type;
		}
		for (idx_t gi = 0; gi < result->plan.groups.size(); gi++) {
			auto &gm = result->group_mapping[gi];
			auto &group = result->plan.groups[gi];
			// Mapped columns must be written with the types the group ALREADY
			// stores (schema evolution / new partitions must not change a
			// column's type across parts — the reader plan uses the last part's
			// types). Fall back to the source types only on the first write,
			// when the group has no schema yet.
			case_insensitive_map_t<LogicalType> group_types;
			for (idx_t ci = 0; ci < group.column_order.size(); ci++) {
				group_types[group.column_order[ci]] = group.schema_types[ci];
			}
			for (auto &col : gm.col_names) {
				auto it = source_schema.find(col);
				if (it == source_schema.end()) {
					throw BinderException("%s: column '%s' (group '%s') not found in source '%s'", fn, col,
					                      result->plan.groups[gi].manifest.group, result->source_path);
				}
				auto git = group_types.find(col);
				gm.col_types.push_back(git != group_types.end() ? git->second : it->second);
			}
			// Only the index group maps the partition source column.
			if (gi == 0) {
				auto it = source_schema.find(result->date_col);
				if (it == source_schema.end()) {
					throw BinderException("%s: partition column '%s' not found in source '%s'", fn, result->date_col,
					                      result->source_path);
				}
				if (it->second.id() != LogicalTypeId::DATE && it->second.id() != LogicalTypeId::TIMESTAMP) {
					throw BinderException("%s: partition column '%s' must be DATE or TIMESTAMP (got %s)", fn,
					                      result->date_col, it->second.ToString());
				}
			}
		}
		result->source_rows = reader->NumRows();
	}

	// Needed source columns: all mapped columns (+ the key columns for delete).
	{
		case_insensitive_map_t<idx_t> needed_pos;
		for (idx_t gi = 0; gi < result->plan.groups.size(); gi++) {
			auto &gm = result->group_mapping[gi];
			for (idx_t c = 0; c < gm.col_names.size(); c++) {
				auto it = needed_pos.find(gm.col_names[c]);
				if (it == needed_pos.end()) {
				needed_pos[gm.col_names[c]] = result->needed_names.size();
				result->needed_names.push_back(gm.col_names[c]);
				}
				gm.src_pos.push_back(needed_pos[gm.col_names[c]]);
			}
		}
		if (is_delete) {
			// the keys source's two columns
			auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(result->source_path), ParquetOptions(context));
			for (auto &col : reader->columns) {
				if (StringUtil::CIEquals(col.name, result->date_col) ||
				    StringUtil::CIEquals(col.name, result->symbol_col)) {
					needed_pos[col.name] = result->needed_names.size();
					result->needed_names.push_back(col.name);
				}
			}
		}
	}

	if (is_delete) {
		result->types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
		result->names = {"rows_deleted", "parts_rewritten", "txid"};
	} else {
		result->types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
		result->names = {"rows_inserted", "rows_updated", "parts_rewritten", "txid"};
	}
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

unique_ptr<FunctionData> AlignedUpsertBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	return MutateBind(context, input, return_types, names, false);
}

unique_ptr<FunctionData> AlignedDeleteBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	return MutateBind(context, input, return_types, names, true);
}

unique_ptr<GlobalTableFunctionState> MutateInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<MutateGlobalState>();
}

//===----------------------------------------------------------------------===//
// Execution helpers
//===----------------------------------------------------------------------===//

//! A source key row (date partition + symbol) in sorted order.
struct SortedRow {
	string partition_key; // full "name=value" segment ("" for unpartitioned)
	Value symbol;
	idx_t src_row; // row in the source collection
};

//! Sorts the key rows by (partition, symbol) ascending (Value order for the
//! symbol — the same order the resolver binary-searches) and keeps the LAST
//! duplicate per (partition, symbol) (dedupe: the last source row wins).
static void SortAndDedupe(vector<SortedRow> &rows) {
	std::sort(rows.begin(), rows.end(), [](const SortedRow &a, const SortedRow &b) {
		if (a.partition_key != b.partition_key) {
			return a.partition_key < b.partition_key;
		}
		if (a.symbol != b.symbol) {
			return a.symbol < b.symbol;
		}
		return a.src_row < b.src_row;
	});
	vector<SortedRow> dedup;
	for (idx_t i = 0; i < rows.size(); i++) {
		if (i + 1 < rows.size() && rows[i].partition_key == rows[i + 1].partition_key &&
		    rows[i].symbol == rows[i + 1].symbol) {
			continue;
		}
		dedup.push_back(std::move(rows[i]));
	}
	rows = std::move(dedup);
}

//! Reads the named columns of a parquet file into a new collection (the
//! collection's types are the reader's column types, matched by name).
static unique_ptr<ColumnDataCollection> ReadSourceColumns(ClientContext &context, const string &path,
                                                         const vector<string> &col_names) {
	auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(path), ParquetOptions(context));
	vector<LogicalType> types;
	for (auto &name : col_names) {
		idx_t pos = DConstants::INVALID_INDEX;
		for (idx_t c = 0; c < reader->columns.size(); c++) {
			if (StringUtil::CIEquals(reader->columns[c].name, name)) {
				pos = c;
				break;
			}
		}
		if (pos == DConstants::INVALID_INDEX) {
			throw IOException("Aligned table: column '%s' not found in '%s'", name, path);
		}
		types.push_back(reader->columns[pos].type);
		reader->column_ids.push_back(MultiFileLocalColumnId(pos));
	}
	auto out = make_uniq<ColumnDataCollection>(context, types);
	vector<PartitionStatistics> rg_stats;
	reader->GetPartitionStats(rg_stats);
	vector<idx_t> all_rgs;
	for (idx_t i = 0; i < rg_stats.size(); i++) {
		all_rgs.push_back(i);
	}
	ParquetReaderScanState scan_state;
	reader->InitializeScan(context, scan_state, all_rgs);
	DataChunk chunk;
	chunk.Initialize(context, types);
	ColumnDataAppendState append_state;
	out->InitializeAppend(append_state);
	while (true) {
		auto res = reader->Scan(context, scan_state, chunk);
		auto async_type = res.GetResultType();
		if (async_type == AsyncResultType::FINISHED || async_type == AsyncResultType::BLOCKED) {
			break;
		}
		if (chunk.size() == 0) {
			continue; // parquet emits empty setup chunks on row-group switches
		}
		out->Append(append_state, chunk);
	}
	return out;
}

//! The index group's partition with the given key (nullptr when absent).
static const GroupPartition *FindPartition(const GroupPlan &group, const string &key) {
	for (auto &p : group.partitions) {
		if (p.key == key) {
			return &p;
		}
	}
	return nullptr;
}

//! The row offset of the index part (partition_index) within its partition.
//! Returns false when the partition does not exist (a fresh partition: offset
//! 0 is returned, the caller treats the key as a new-partition insert).
static bool IndexPartOffset(const GroupPlan &index_group, const string &partition_key, idx_t part_index,
                            idx_t &offset) {
	auto p = FindPartition(index_group, partition_key);
	if (!p) {
		offset = 0;
		return false;
	}
	idx_t off = 0;
	for (idx_t k = 0; k < p->part_count; k++) {
		auto &part = index_group.parts[p->first_part + k];
		if (part.partition_index == part_index) {
			break;
		}
		off += part.row_count;
	}
	offset = off;
	return true;
}

//! The index group's part with the given partition key + partition index
//! (nullptr for a fresh partition).
static const PartInfo *FindIndexPart(const GroupPlan &group, const string &partition_key, idx_t part_index) {
	auto p = FindPartition(group, partition_key);
	if (!p) {
		return nullptr;
	}
	for (idx_t k = 0; k < p->part_count; k++) {
		auto &part = group.parts[p->first_part + k];
		if (part.partition_index == part_index) {
			return &part;
		}
	}
	return nullptr;
}

//! The part of a group's partition containing the partition-local position
//! `pos` (the part whose row range covers pos; the last part when pos == the
//! partition's row count). Returns nullptr when the group lacks the partition.
//! `local` = pos - (rows of the lower parts).
static const PartInfo *FindPartByPosition(const GroupPlan &group, const string &partition_key, idx_t pos,
                                          idx_t &local) {
	auto p = FindPartition(group, partition_key);
	if (!p) {
		return nullptr;
	}
	idx_t off = 0;
	const PartInfo *chosen = nullptr;
	for (idx_t k = 0; k < p->part_count; k++) {
		auto &part = group.parts[p->first_part + k];
		if (pos < off + part.row_count) {
			chosen = &part;
			break;
		}
		off += part.row_count;
	}
	if (!chosen) {
		// pos == partition row count: append at the last part's end
		chosen = &group.parts[p->first_part + p->part_count - 1];
		local = chosen->row_count;
		return chosen;
	}
	local = pos - off;
	return chosen;
}

//! Random access to a ColumnDataCollection's rows via chunk fetches (the
//! collection's chunk size is STANDARD_VECTOR_SIZE; a cached chunk is fetched
//! on demand).
struct SourceReader {
	ClientContext &context;
	ColumnDataCollection &src;
	DataChunk chunk;
	idx_t chunk_idx = DConstants::INVALID_INDEX;

	SourceReader(ClientContext &context_p, ColumnDataCollection &src_p) : context(context_p), src(src_p) {
		chunk.Initialize(context, src.Types());
	}

	Value GetValue(idx_t column, idx_t row) {
		idx_t ci = row / STANDARD_VECTOR_SIZE;
		if (ci != chunk_idx) {
			chunk_idx = ci;
			src.FetchChunk(chunk_idx, chunk);
		}
		return chunk.GetValue(column, row % STANDARD_VECTOR_SIZE);
	}
};

using TargetMap = std::map<std::pair<string, idx_t>, unique_ptr<MutateTarget>>;

//! Gets the target for (partition_key, part_index) in group gi, creating it on
//! first use. Existing parts use their own footer columns as the output schema
//! (schema evolution: inserts write NULL for missing columns); fresh parts use
//! the group's mapped columns.
static MutateTarget &GetCreateTarget(ClientContext &context, const MutateBindData &bind, idx_t gi,
                                     const string &partition_key, idx_t part_index, const PartInfo *part,
                                     vector<TargetMap> &targets) {
	auto &m = targets[gi];
	auto key = std::make_pair(partition_key, part_index);
	auto it = m.find(key);
	if (it != m.end()) {
		return *it->second;
	}
	auto t = make_uniq<MutateTarget>();
	t->partition_key = partition_key;
	t->part_index = part_index;
	t->part = part;
	auto &gm = bind.group_mapping[gi];
	t->mapped_names = gm.col_names;
	t->mapped_types = gm.col_types;
	if (part) {
		auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(part->path), ParquetOptions(context));
		for (auto &col : reader->columns) {
			t->out_names.push_back(col.name);
			t->out_types.push_back(col.type);
		}
	} else if (!bind.plan.groups[gi].parts.empty()) {
		// Fresh part in an EXISTING group: carry the group's full current
		// schema PLUS any newly mapped columns (schema evolution) — unmapped
		// existing columns write NULL. Using only the mapping would narrow
		// the group schema and break rewrites of older, wider parts; using
		// only the group schema would drop newly added columns.
		auto &group_schema = bind.plan.groups[gi];
		t->out_names = group_schema.column_order;
		t->out_types = group_schema.schema_types;
		for (idx_t mi = 0; mi < gm.col_names.size(); mi++) {
			bool exists = false;
			for (auto &on : t->out_names) {
				if (StringUtil::CIEquals(on, gm.col_names[mi])) {
					exists = true;
					break;
				}
			}
			if (!exists) {
				t->out_names.push_back(gm.col_names[mi]);
				t->out_types.push_back(gm.col_types[mi]);
			}
		}
	} else {
		t->out_names = gm.col_names;
		t->out_types = gm.col_types;
	}
	if (!bind.is_delete) {
		vector<LogicalType> types;
		types.push_back(LogicalType::BIGINT); // position / row column
		for (auto &ty : gm.col_types) {
			types.push_back(ty);
		}
		t->insert_buffer = make_uniq<ColumnDataCollection>(context, types);
		t->insert_buffer->InitializeAppend(t->insert_append);
		t->update_buffer = make_uniq<ColumnDataCollection>(context, types);
		t->update_buffer->InitializeAppend(t->update_append);
	}
	auto res = m.emplace(key, std::move(t));
	return *res.first->second;
}

//! Appends one [pos, ...mapped values] row to a buffer. `scratch` is a
//! caller-owned reusable chunk (initialized on first use) — allocating a
//! fresh DataChunk per row dominated batch-insert profiles.
static void AppendRowToBuffer(ClientContext &context, ColumnDataCollection &buffer,
                              ColumnDataAppendState &append_state, idx_t pos, const vector<idx_t> &src_pos,
                              SourceReader &src, idx_t src_row, DataChunk &scratch,
                              const ColumnDataCollection *&scratch_owner) {
	if (scratch_owner != &buffer) {
		// NOTE: DataChunk::Initialize appends vectors (D_ASSERT(data.empty())
		// is release-noop) - a reused chunk must be reconstructed wholesale.
		// DataChunk is non-copy-assignable (Vector is), so rebuild in place.
		scratch.~DataChunk();
		new (&scratch) DataChunk();
		scratch.Initialize(context, buffer.Types());
		scratch_owner = &buffer;
	}
	scratch.Reset();
	scratch.SetCardinality(1);
	scratch.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(pos)));
	for (idx_t c = 0; c < src_pos.size(); c++) {
		Value v = src.GetValue(src_pos[c], src_row);
		scratch.SetValue(1 + c, 0, v);
	}
	buffer.Append(append_state, scratch);
}

//! Stage + commit one mutation: rewrite every affected part into
//! _tmp/transaction-<txid>/, move the parts into place (atomic per move),
//! delete the superseded parts, remove delete-emptied single-part partitions
//! from every group, and rewrite _table.json (groups + partitioning only).
static void ExecuteAndCommit(ClientContext &context, const MutateBindData &bind, MutateGlobalState &gstate,
                             vector<TargetMap> &targets) {
	auto &fs = FileSystem::GetFileSystem(context);
	idx_t txid = NextTransactionId();
	gstate.txid = txid;
	string tmp_root = bind.plan.table_path + "/_tmp/transaction-" + to_string(txid);
	std::set<string> removed_partitions;

	try {
		// Pass A: stage every rewrite (visible to readers only after the move).
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			auto &group = bind.plan.groups[gi];
			for (auto &kv : targets[gi]) {
			auto &t = *kv.second;
			if (t.removed || t.remove_part) {
				if (t.removed) {
					removed_partitions.insert(t.partition_key);
				}
				continue;
			}
				bool has_ins = t.insert_buffer && t.insert_buffer->Count() > 0;
				bool has_upd = t.update_buffer && t.update_buffer->Count() > 0;
				if (!has_ins && !has_upd && t.delete_rows.empty()) {
					continue;
				}
				string staged_dir = tmp_root + "/" + group.manifest.group + "/" + t.partition_key;
				fs.CreateDirectoriesRecursive(staged_dir);
				t.staged_path = staged_dir + "/" + StringUtil::Format("%04llu-0000000000.parquet", t.part_index);
				PartMergeInput in;
				in.part = t.part;
				in.staged_path = t.staged_path;
				in.col_names = t.out_names;
				in.col_types = t.out_types;
				in.inserts = t.insert_buffer.get();
				in.insert_cols = t.mapped_names;
				in.updates = t.update_buffer.get();
				in.update_cols = t.mapped_names;
				in.deletes = &t.delete_rows;
				in.rgs = TableManifest::DEFAULT_RG_ROWS;
				t.new_row_count = RewritePart(context, in);
				if (t.new_row_count == 0) {
					// Only a pre-checked single-part partition can be emptied
					// (multi-part empties fail fast before staging); the part
					// is discarded and the partition removed below.
					removed_partitions.insert(t.partition_key);
					continue;
				}
				gstate.parts_rewritten++;
			}
		}
		// Pass B: move the staged parts into place, then delete the superseded
		// old parts (same-name updates are replaced by the move).
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			auto &group = bind.plan.groups[gi];
			for (auto &kv : targets[gi]) {
				auto &t = *kv.second;
				if (t.removed || t.staged_path.empty() || t.new_row_count == 0) {
					continue;
				}
				string final_name = StringUtil::Format("%04llu-%010llu.parquet", t.part_index, t.new_row_count);
				string final_path = group.group_path + "/" + t.partition_key + "/" + final_name;
				auto slash = final_path.find_last_of("/\\");
				fs.CreateDirectoriesRecursive(final_path.substr(0, slash));
				fs.MoveFile(t.staged_path, final_path);
				if (t.part && t.part->path != final_path) {
					fs.RemoveFile(t.part->path);
				}
			}
		}
		// Pass C: remove delete-emptied single-part partitions from every group.
		for (auto &key : removed_partitions) {
			for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
				auto &group = bind.plan.groups[gi];
				if (!FindPartition(group, key)) {
					continue;
				}
				string dir = group.group_path + "/" + key;
				if (fs.DirectoryExists(dir)) {
					fs.RemoveDirectory(dir);
				}
			}
			gstate.parts_removed++;
		}
		// Pass D: remove delete-emptied highest-index part files (per group;
		// the remaining indexes stay consecutive, no renumbering needed).
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			for (auto &kv : targets[gi]) {
				auto &t = *kv.second;
				if (!t.remove_part || !t.part) {
					continue;
				}
				fs.RemoveFile(t.part->path);
				gstate.parts_removed++;
			}
		}
		if (gstate.parts_rewritten > 0 || gstate.parts_removed > 0) {
			WriteManifest(fs, bind.plan);
		}
	} catch (...) {
		if (fs.DirectoryExists(tmp_root)) {
			fs.RemoveDirectory(tmp_root);
		}
		throw;
	}
	// Best-effort cleanup of the staging tree (the empty _tmp parent stays).
	if (fs.DirectoryExists(tmp_root)) {
		fs.RemoveDirectory(tmp_root);
	}
	string tmp_parent = bind.plan.table_path + "/_tmp";
	try {
		if (fs.DirectoryExists(tmp_parent)) {
			fs.RemoveDirectory(tmp_parent);
		}
	} catch (...) {
	}
}

//! The index group's partition template (the mutator only needs the index
//! group's single template — partition keys are index-defined).
static string IndexTemplate(const MutateBindData &bind) {
	auto &index_group = bind.plan.groups[0];
	if (index_group.manifest.partitioning.empty()) {
		return string();
	}
	return index_group.manifest.partitioning[0].template_str;
}

//===----------------------------------------------------------------------===//
// aligned_upsert
//===----------------------------------------------------------------------===//

void AlignedUpsertFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<MutateBindData>();
	auto &gstate = data.global_state->Cast<MutateGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &index_group = bind.plan.groups[0];
	string template_str = IndexTemplate(bind);

	// 1. Read the needed source columns (mapped columns; the index mapping
	//    includes the primary key columns).
	auto src = ReadSourceColumns(context, bind.source_path, bind.needed_names);
	SourceReader reader(context, *src);

	// 2. Locate the key columns among the needed columns.
	idx_t date_pos = DConstants::INVALID_INDEX;
	idx_t symbol_pos = DConstants::INVALID_INDEX;
	for (idx_t c = 0; c < bind.needed_names.size(); c++) {
		if (StringUtil::CIEquals(bind.needed_names[c], bind.date_col)) {
			date_pos = c;
		}
		if (StringUtil::CIEquals(bind.needed_names[c], bind.symbol_col)) {
			symbol_pos = c;
		}
	}
	if (date_pos == DConstants::INVALID_INDEX || symbol_pos == DConstants::INVALID_INDEX) {
		throw IOException("Aligned table: internal error — primary key columns not among the mapped columns");
	}

	// 3. Build + sort + dedupe the key list ((partition, symbol) order).
	vector<SortedRow> rows;
	rows.reserve(src->Count());
	for (idx_t r = 0; r < src->Count(); r++) {
		Value dv = reader.GetValue(date_pos, r);
		if (dv.IsNull()) {
			throw IOException("Aligned table: NULL in the partition source column '%s' at source row %llu",
			                  bind.date_col, r);
		}
		SortedRow row;
		date_t d = dv.GetValue<date_t>();
		if (!template_str.empty() && !EvaluatePartitionTemplate(template_str, d, row.partition_key)) {
			throw IOException("Aligned table: cannot evaluate partition template '%s'", template_str);
		}
		row.symbol = reader.GetValue(symbol_pos, r);
		row.src_row = r;
		rows.push_back(std::move(row));
	}
	SortAndDedupe(rows);

	// 4. Resolve every key against the index group (insert vs update).
	KeyResolver resolver(context, bind.plan);
	vector<KeyLocation> locs(rows.size());
	for (idx_t i = 0; i < rows.size(); i++) {
		Value dv = reader.GetValue(date_pos, rows[i].src_row);
		locs[i] = resolver.Resolve(dv.GetValue<date_t>(), rows[i].symbol);
		if (locs[i].found) {
			gstate.rows_updated++;
		} else {
			gstate.rows_inserted++;
		}
	}

	// 5. Dispatch to per-group targets (sorted order -> ascending positions).
	vector<TargetMap> targets(bind.plan.groups.size());
	DataChunk row_scratch; // reused by AppendRowToBuffer (re-init per buffer type)
	const ColumnDataCollection *row_scratch_owner = nullptr;
	for (idx_t i = 0; i < rows.size(); i++) {
		auto &loc = locs[i];
		idx_t src_row = rows[i].src_row;
		// The key's global partition position (its row, or its insertion point).
		idx_t p;
		IndexPartOffset(index_group, loc.partition_key, loc.part_index, p);
		p += loc.part_local_row;
		bool fresh = FindPartition(index_group, loc.partition_key) == nullptr;
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			auto &group = bind.plan.groups[gi];
			// UPDATE that touches none of this group's columns: the rewrite
			// would be byte-identical — skip the group entirely.
			if (gi > 0 && loc.found && bind.group_mapping[gi].col_names.empty()) {
				continue;
			}
			const PartInfo *part = nullptr;
			idx_t local = 0;
			idx_t target_idx = 0;
			bool do_synth = false;
			if (gi == 0) {
				part = FindIndexPart(group, loc.partition_key, loc.part_index);
				local = loc.part_local_row;
				target_idx = part ? part->partition_index : loc.part_index;
			} else if (loc.append_new_part) {
				// Append at the partition end: groups that have the partition
				// create a new part (index = loc.part_index) holding only the
				// new rows; subset groups keep NULL-filling their rows.
				if (!FindPartition(group, loc.partition_key)) {
					continue;
				}
				target_idx = loc.part_index;
				local = 0;
			} else if ((part = FindPartByPosition(group, loc.partition_key, p, local))) {
				target_idx = part->partition_index;
				// existing partition present in this group
			} else if (fresh && !bind.group_mapping[gi].col_names.empty()) {
				target_idx = 0;
				// new partition: mapped groups create a fresh part (unmapped
				// groups get nothing — the reader NULL-fills their rows)
			} else if (loc.found && !bind.group_mapping[gi].col_names.empty()) {
				// Key exists in index but this group has never seen this
				// partition (it predates the group's first mapping): synthesize
				// an aligned part mirroring the index partition.
				target_idx = 0;
				do_synth = true;
			} else {
				continue; // group lacks this partition (subset) or is unmapped
			}
			auto &target = GetCreateTarget(context, bind, gi, loc.partition_key, target_idx, part, targets);
			if (do_synth && !target.synth) {
				idx_t ri = 0;
				for (auto &ip : index_group.parts) {
					if (ip.partition_key == loc.partition_key) {
						ri += ip.row_count;
					}
				}
				target.synth = true;
				target.synth_rows = ri;
			}
			if (loc.found) {
				if (target.synth) {
					// Capture the keyed row; the full R_i-row fill happens in
					// sorted order after the key loop.
					vector<Value> vals;
					for (idx_t c = 0; c < bind.group_mapping[gi].src_pos.size(); c++) {
						vals.push_back(reader.GetValue(bind.group_mapping[gi].src_pos[c], src_row));
					}
					target.synth_values[p] = std::move(vals);
					continue;
				}
				AppendRowToBuffer(context, *target.update_buffer, target.update_append, local,
				                  bind.group_mapping[gi].src_pos, reader, src_row, row_scratch, row_scratch_owner);
			} else {
				idx_t pos = part ? local : target.insert_next++;
				AppendRowToBuffer(context, *target.insert_buffer, target.insert_append, pos,
				                  bind.group_mapping[gi].src_pos, reader, src_row, row_scratch, row_scratch_owner);
				target.inserts_count++;
			}
		}
	}

	// 5.5 Fill synthesized parts: append R_i rows in sorted position order —
	// keyed rows carry the captured mapped values, everything else NULL.
	for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
		for (auto &kv : targets[gi]) {
			auto &t = *kv.second;
			if (!t.synth) {
				continue;
			}
			const auto &mtypes = bind.group_mapping[gi].col_types;
			DataChunk row;
			row.Initialize(context, t.insert_buffer->Types());
			for (idx_t pos = 0; pos < t.synth_rows; pos++) {
				row.Reset();
				row.SetCardinality(1);
				row.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(pos)));
				auto it = t.synth_values.find(pos);
				for (idx_t c = 0; c < mtypes.size(); c++) {
					row.SetValue(1 + c, 0, it != t.synth_values.end() ? it->second[c] : Value(mtypes[c]));
				}
				t.insert_buffer->Append(t.insert_append, row);
			}
			t.inserts_count = t.synth_rows;
			t.synth_values.clear();
		}
	}

	// 6. Rewrite + commit.
	ExecuteAndCommit(context, bind, gstate, targets);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_inserted)));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_updated)));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(gstate.parts_rewritten)));
	output.SetValue(3, 0, Value::BIGINT(NumericCast<int64_t>(gstate.txid)));
}

//===----------------------------------------------------------------------===//
// aligned_delete
//===----------------------------------------------------------------------===//

void AlignedDeleteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<MutateBindData>();
	auto &gstate = data.global_state->Cast<MutateGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &index_group = bind.plan.groups[0];
	string template_str = IndexTemplate(bind);

	// 1. Read the keys source's two key columns.
	vector<string> key_names = {bind.date_col, bind.symbol_col};
	auto src = ReadSourceColumns(context, bind.source_path, key_names);
	SourceReader reader(context, *src);

	// 2. Build + sort + dedupe the key list.
	vector<SortedRow> rows;
	rows.reserve(src->Count());
	for (idx_t r = 0; r < src->Count(); r++) {
		Value dv = reader.GetValue(0, r);
		if (dv.IsNull()) {
			throw IOException("Aligned table: NULL in the partition source column '%s' at source row %llu",
			                  bind.date_col, r);
		}
		SortedRow row;
		date_t d = dv.GetValue<date_t>();
		if (!template_str.empty() && !EvaluatePartitionTemplate(template_str, d, row.partition_key)) {
			throw IOException("Aligned table: cannot evaluate partition template '%s'", template_str);
		}
		row.symbol = reader.GetValue(1, r);
		row.src_row = r;
		rows.push_back(std::move(row));
	}
	SortAndDedupe(rows);

	// 3. Resolve every key; non-existent keys are skipped (idempotent delete).
	KeyResolver resolver(context, bind.plan);
	vector<KeyLocation> locs(rows.size());
	for (idx_t i = 0; i < rows.size(); i++) {
		Value dv = reader.GetValue(0, rows[i].src_row);
		locs[i] = resolver.Resolve(dv.GetValue<date_t>(), rows[i].symbol);
	}

	// 4. Dispatch delete rows per group (every group with the partition loses
	//    the same rows).
	vector<TargetMap> targets(bind.plan.groups.size());
	for (idx_t i = 0; i < rows.size(); i++) {
		auto &loc = locs[i];
		if (!loc.found) {
			continue;
		}
		gstate.rows_deleted++;
		idx_t p;
		IndexPartOffset(index_group, loc.partition_key, loc.part_index, p);
		p += loc.part_local_row;
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			auto &group = bind.plan.groups[gi];
			const PartInfo *part = nullptr;
			idx_t local = 0;
			if (gi == 0) {
				part = FindIndexPart(group, loc.partition_key, loc.part_index);
				local = loc.part_local_row;
			} else if (!(part = FindPartByPosition(group, loc.partition_key, p, local))) {
				continue; // group lacks the partition
			}
			if (!part) {
				throw IOException("Aligned table: internal error — a found key has no index part");
			}
			auto &target = GetCreateTarget(context, bind, gi, loc.partition_key, part->partition_index, part, targets);
			target.delete_rows.push_back(local);
			target.deletes_count++;
		}
	}

	// 5. Pre-check: a part emptied by deletes must be removable without
	//    breaking the index-consecutiveness contract — either it is the only
	//    part of its partition (whole-partition removal) or it is the group's
	//    highest-index part in that partition (part-file removal; remaining
	//    indexes stay consecutive). Emptying an interior part of a multi-part
	//    partition is rejected before anything is staged.
	for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
		auto &group = bind.plan.groups[gi];
		for (auto &kv : targets[gi]) {
			auto &t = *kv.second;
			if (!t.part || t.delete_rows.empty()) {
				continue;
			}
			if (t.delete_rows.size() != t.part->row_count) {
				continue;
			}
			if (t.part->partition_parts == 1) {
				t.removed = true;
				continue;
			}
			// Highest existing index of this group within the partition?
			idx_t max_index = t.part->partition_index;
			for (auto &p : group.parts) {
				if (p.partition_key == t.partition_key && p.partition_index > max_index) {
					max_index = p.partition_index;
				}
			}
			if (t.part->partition_index == max_index) {
				t.remove_part = true;
				continue;
			}
			throw IOException("Aligned table '%s': cannot delete all rows of part '%s' (partition '%s' has "
			                  "%llu parts); run aligned_compact first",
			                  bind.table_name, t.part->part_name, t.partition_key, t.part->partition_parts);
		}
	}

	// 6. Rewrite + commit.
	ExecuteAndCommit(context, bind, gstate, targets);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_deleted)));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(gstate.parts_rewritten)));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(gstate.txid)));
}

} // namespace duckdb