#include "mutator/aligned_mutator.hpp"

#include "catalog/manifest.hpp"
#include "resolver/key_resolver.hpp"
#include "resolver/partition_resolver.hpp"
#include "rewriter/part_rewriter.hpp"
#include "io/parquet_io.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "parquet_reader.hpp"

#include <algorithm>
#include <map>
#include <set>
#include <atomic>
#include <mutex>
#include <thread>
#include <utility>

namespace duckdb {

//! Shared transaction counter (process-wide, non-persisted). Defined here
//! and declared in aligned_mutator.hpp so the compactor shares the same
//! counter — avoids txid collision between mutator and compactor.
idx_t NextTransactionId() {
	static std::atomic<idx_t> counter{0};
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

// NOTE: ResolveRoot + MutateBind (the SQL table-function bind path for the
// former aligned_upsert / aligned_delete) were removed when those table
// functions were deleted. The standard DML path (ATTACH + INSERT/UPDATE/DELETE)
// calls AlignedUpsertFromCollection / AlignedDeleteFromCollection, whose bind
// helpers BuildUpsertBindFromCollection / BuildDeleteBindFromCollection below
// replicate the same binding logic but source schema from an in-memory
// ColumnDataCollection instead of opening a parquet file.

// NOTE: AlignedUpsertBind, AlignedDeleteBind, and MutateInitGlobal were removed
// when the aligned_upsert / aligned_delete SQL table functions were deleted.
// The standard DML path (ATTACH + INSERT/UPDATE/DELETE) calls
// AlignedUpsertFromCollection / AlignedDeleteFromCollection directly, which in
// turn call the file-local AlignedUpsertFunction / AlignedDeleteFunction below.

// Forward declarations for the file-local function bodies (defined later).
static void AlignedUpsertFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);
static void AlignedDeleteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

//! Builds a MutateBindData for upsert, using an in-memory ColumnDataCollection
//! as the source instead of a parquet file. This replicates the logic of
//! MutateBind but sources schema info from the collection (types + names)
//! rather than opening a ParquetReader.
static unique_ptr<MutateBindData> BuildUpsertBindFromCollection(ClientContext &context,
                                                                const string &table_name,
                                                                const string &root,
                                                                const string &mapping_str,
                                                                const ColumnDataCollection &source_collection,
                                                                const vector<string> &source_col_names) {
	auto result = make_uniq<MutateBindData>();
	result->is_delete = false;
	result->table_name = table_name;
	result->source_path = "<in-memory>";
	result->source_collection = nullptr; // set by caller after bind
	const char *fn = "aligned_upsert";

	auto &fs = FileSystem::GetFileSystem(context);
	string table_dir = root + "/" + table_name;

	bool empty_table = !fs.DirectoryExists(table_dir) ||
	                   fs.GlobFiles(table_dir + "/**/*.parquet", FileGlobOptions::ALLOW_EMPTY).empty();
	if (empty_table) {
		throw BinderException("%s: internal API does not support first write of empty table (use SQL path)", fn);
	}
	BuildTablePlan(context, root, table_name, result->plan);
	result->empty_table = false;
	result->plan.table_path = table_dir;
	result->plan.table_name = table_name;

	// Build source schema map from the collection's types + names
	case_insensitive_map_t<LogicalType> source_schema;
	for (idx_t c = 0; c < source_col_names.size(); c++) {
		source_schema[source_col_names[c]] = source_collection.Types()[c];
	}
	result->source_col_names = source_col_names;

	// Parse mapping
	case_insensitive_map_t<vector<string>> mapping;
	if (!mapping_str.empty()) {
		ParseMapping(mapping_str, fn, mapping);
	}

	// For a non-empty table, the mapping may name groups that don't exist yet
	for (auto &kv : mapping) {
		bool found = false;
		for (auto &g : result->plan.groups) {
			if (StringUtil::CIEquals(g.manifest.group, kv.first)) {
				found = true;
				break;
			}
		}
		if (!found) {
			GroupPlan gp;
			gp.manifest.group = kv.first;
			gp.group_path = table_dir + "/" + kv.first;
			auto slash = kv.first.find('/');
			if (slash != string::npos && kv.first.find('/', slash + 1) == string::npos) {
				gp.lv1 = kv.first.substr(0, slash);
				gp.lv2 = kv.first.substr(slash + 1);
			}
			result->plan.groups.push_back(std::move(gp));
		}
	}

	result->group_mapping.resize(result->plan.groups.size());

	if (mapping_str.empty()) {
		// Auto-derive: each source column → the group whose schema owns it.
		for (auto &name : source_col_names) {
			for (idx_t gi = 0; gi < result->plan.groups.size(); gi++) {
				auto &group = result->plan.groups[gi];
				bool owned = std::any_of(group.column_order.begin(), group.column_order.end(),
				                         [&](const string &n) { return StringUtil::CIEquals(n, name); });
				if (owned) {
					result->group_mapping[gi].col_names.push_back(name);
					break;
				}
			}
		}
	} else {
		for (idx_t gi = 0; gi < result->plan.groups.size(); gi++) {
			auto &gm = result->group_mapping[gi];
			auto &group = result->plan.groups[gi];
			auto it = mapping.find(group.manifest.group);
			if (it == mapping.end()) {
				continue;
			}
			gm.col_names = it->second;
		}
	}

	// Every mapping entry must name a real group
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

	// Primary key columns (non-empty table)
	auto &index_group = result->plan.groups[0];
	result->date_col = index_group.partition_source;
	result->symbol_col = index_group.symbol_column;

	// index mapping must include the key columns
	{
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
			throw BinderException("%s: the index group's mapping must include the primary key columns ('%s', '%s')",
			                      fn, result->date_col, result->symbol_col);
		}
	}

	// Validate all mapped columns exist in the source + resolve types
	for (idx_t gi = 0; gi < result->plan.groups.size(); gi++) {
		auto &gm = result->group_mapping[gi];
		auto &group = result->plan.groups[gi];
		case_insensitive_map_t<LogicalType> group_types;
		for (idx_t ci = 0; ci < group.column_order.size(); ci++) {
			group_types[group.column_order[ci]] = group.schema_types[ci];
		}
		for (auto &col : gm.col_names) {
			auto it = source_schema.find(col);
			if (it == source_schema.end()) {
				throw BinderException("%s: column '%s' (group '%s') not found in source", fn, col,
				                      result->plan.groups[gi].manifest.group);
			}
			auto git = group_types.find(col);
			gm.col_types.push_back(git != group_types.end() ? git->second : it->second);
		}
		if (gi == 0) {
			auto it = source_schema.find(result->date_col);
			if (it == source_schema.end()) {
				throw BinderException("%s: partition column '%s' not found in source", fn, result->date_col);
			}
			if (it->second.id() != LogicalTypeId::DATE && it->second.id() != LogicalTypeId::TIMESTAMP) {
				throw BinderException("%s: partition column '%s' must be DATE or TIMESTAMP (got %s)", fn,
				                      result->date_col, it->second.ToString());
			}
		}
	}
	result->source_rows = source_collection.Count();

	// Needed source columns
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
	}

	result->types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	result->names = {"rows_inserted", "rows_updated", "parts_rewritten", "txid"};
	return result;
}

UpsertResult AlignedUpsertFromCollection(ClientContext &context, const string &table_name,
                                          const string &root, const string &mapping,
                                          ColumnDataCollection &source_collection,
                                          const vector<string> &source_col_names) {
	auto bind = BuildUpsertBindFromCollection(context, table_name, root, mapping, source_collection,
	                                           source_col_names);
	bind->source_collection = &source_collection;

	MutateGlobalState gstate;
	DataChunk output;
	output.Initialize(context, bind->types);

	TableFunctionInput input(bind.get(), nullptr, &gstate);

	AlignedUpsertFunction(context, input, output);

	UpsertResult result;
	if (output.size() > 0) {
		result.rows_inserted = output.GetValue(0, 0).GetValue<int64_t>();
		result.rows_updated = output.GetValue(1, 0).GetValue<int64_t>();
		result.parts_rewritten = output.GetValue(2, 0).GetValue<int64_t>();
	}
	return result;
}

//! Builds a MutateBindData for delete, using an in-memory ColumnDataCollection
//! as the source instead of a parquet file. The collection must have two
//! columns: (symbol VARCHAR, date DATE/TIMESTAMP).
static unique_ptr<MutateBindData> BuildDeleteBindFromCollection(ClientContext &context,
                                                                const string &table_name,
                                                                const string &root,
                                                                const ColumnDataCollection &keys_collection) {
	auto result = make_uniq<MutateBindData>();
	result->is_delete = true;
	result->table_name = table_name;
	result->source_path = "<in-memory>";
	result->source_collection = nullptr;
	const char *fn = "aligned_delete";

	auto &fs = FileSystem::GetFileSystem(context);
	string table_dir = root + "/" + table_name;

	bool empty_table = !fs.DirectoryExists(table_dir) ||
	                   fs.GlobFiles(table_dir + "/**/*.parquet", FileGlobOptions::ALLOW_EMPTY).empty();
	if (empty_table) {
		throw BinderException("%s: the table '%s' is empty (no parts) — nothing to delete", fn, table_name);
	}
	BuildTablePlan(context, root, table_name, result->plan);
	result->empty_table = false;
	result->plan.table_path = table_dir;
	result->plan.table_name = table_name;

	result->group_mapping.resize(result->plan.groups.size());

	// Primary key columns (non-empty table)
	auto &index_group = result->plan.groups[0];
	result->date_col = index_group.partition_source;
	result->symbol_col = index_group.symbol_column;

	// The keys collection must have 2 columns matching (symbol, date).
	result->source_col_names = {result->symbol_col, result->date_col};
	auto &ktypes = keys_collection.Types();
	if (ktypes.size() != 2) {
		throw BinderException("%s: keys collection must have 2 columns (symbol, date), got %llu", fn,
		                      (unsigned long long)ktypes.size());
	}
	// Column 0 = symbol (VARCHAR), column 1 = date (DATE or TIMESTAMP)
	if (ktypes[0].id() != LogicalTypeId::VARCHAR) {
		throw BinderException("%s: keys collection column 0 must be VARCHAR (symbol), got %s", fn,
		                      ktypes[0].ToString());
	}
	if (ktypes[1].id() != LogicalTypeId::DATE && ktypes[1].id() != LogicalTypeId::TIMESTAMP) {
		throw BinderException("%s: keys collection column 1 must be DATE or TIMESTAMP, got %s", fn,
		                      ktypes[1].ToString());
	}

	result->source_rows = keys_collection.Count();
	result->types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	result->names = {"rows_deleted", "parts_rewritten", "txid"};
	return result;
}

DeleteResult AlignedDeleteFromCollection(ClientContext &context, const string &table_name,
                                          const string &root, ColumnDataCollection &keys_collection) {
	auto bind = BuildDeleteBindFromCollection(context, table_name, root, keys_collection);
	bind->source_collection = &keys_collection;

	MutateGlobalState gstate;
	DataChunk output;
	output.Initialize(context, bind->types);

	TableFunctionInput input(bind.get(), nullptr, &gstate);

	AlignedDeleteFunction(context, input, output);

	DeleteResult result;
	if (output.size() > 0) {
		result.rows_deleted = output.GetValue(0, 0).GetValue<int64_t>();
		result.parts_rewritten = output.GetValue(1, 0).GetValue<int64_t>();
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Execution helpers
//===----------------------------------------------------------------------===//

//! A source key row (date partition, symbol, date) in sorted order.
struct SortedRow {
	string partition_key; // full "name=value" segment ("" for unpartitioned)
	Value symbol;
	int64_t date;         // v8: composite key (symbol, date) — date_t or timestamp_t
	idx_t src_row;         // row in the source collection
};

//! Sorts the key rows by (partition, symbol, date) ascending (Value order for
//! the symbol — the same order the resolver binary-searches; date_t numeric
//! order) and keeps the LAST duplicate per (partition, symbol, date) (dedupe:
//! the last source row wins).
static void SortAndDedupe(vector<SortedRow> &rows) {
	std::sort(rows.begin(), rows.end(), [](const SortedRow &a, const SortedRow &b) {
		if (a.partition_key != b.partition_key) {
			return a.partition_key < b.partition_key;
		}
		if (a.symbol != b.symbol) {
			return a.symbol < b.symbol;
		}
		return a.date < b.date;
	});
	vector<SortedRow> dedup;
	for (idx_t i = 0; i < rows.size(); i++) {
		if (i + 1 < rows.size() && rows[i].partition_key == rows[i + 1].partition_key &&
		    rows[i].symbol == rows[i + 1].symbol && rows[i].date == rows[i + 1].date) {
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
	vector<LogicalType> types;
	ParquetReaderScanState scan_state;
	auto reader = OpenPartReaderNamedColumns(context, path, col_names, types, scan_state);
	auto out = make_uniq<ColumnDataCollection>(context, types);
	ColumnDataAppendState append_state;
	out->InitializeAppend(append_state);
	DataChunk chunk;
	chunk.Initialize(context, types);
	while (true) {
		auto res = reader->Scan(context, scan_state, chunk);
		auto async_type = res.GetResultType();
		if (async_type == AsyncResultType::FINISHED) {
			break;
		}
		if (async_type == AsyncResultType::BLOCKED) {
			continue;
		}
		if (chunk.size() == 0) {
			continue;
		}
		out->Append(append_state, chunk);
	}
	return out;
}

//! Extract the needed columns from an in-memory ColumnDataCollection (used
//! by PhysicalAlignedInsert to avoid the temp-parquet double-write). The
//! collection's column order is matched by name (case-insensitive).
static unique_ptr<ColumnDataCollection> ReadSourceFromCollection(ClientContext &context,
                                                                 const ColumnDataCollection &src_collection,
                                                                 const vector<string> &col_names,
                                                                 const vector<string> &src_col_names) {
	// Map needed names → source collection column indices
	vector<idx_t> src_indices;
	vector<LogicalType> types;
	for (auto &name : col_names) {
		idx_t pos = DConstants::INVALID_INDEX;
		for (idx_t c = 0; c < src_col_names.size(); c++) {
			if (StringUtil::CIEquals(src_col_names[c], name)) {
				pos = c;
				break;
			}
		}
		if (pos == DConstants::INVALID_INDEX) {
			throw IOException("Aligned table: column '%s' not found in source collection", name);
		}
		src_indices.push_back(pos);
		types.push_back(src_collection.Types()[pos]);
	}
	auto out = make_uniq<ColumnDataCollection>(context, types);
	ColumnDataAppendState append_state;
	out->InitializeAppend(append_state);
	ColumnDataScanState scan_state;
	src_collection.InitializeScan(scan_state);
	DataChunk scan_chunk;
	src_collection.InitializeScanChunk(scan_chunk);
	DataChunk out_chunk;
	out_chunk.Initialize(context, types);
	while (src_collection.Scan(scan_state, scan_chunk)) {
		if (scan_chunk.size() == 0) {
			continue;
		}
		out_chunk.Reset();
		out_chunk.SetCardinality(scan_chunk.size());
		for (idx_t c = 0; c < src_indices.size(); c++) {
			VectorOperations::Copy(scan_chunk.data[src_indices[c]], out_chunk.data[c], scan_chunk.size(), 0, 0);
		}
		out->Append(append_state, out_chunk);
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
	// Cached unified formats per column (recomputed when chunk changes).
	vector<UnifiedVectorFormat> fmts;
	bool fmts_valid = false;

	SourceReader(ClientContext &context_p, ColumnDataCollection &src_p) : context(context_p), src(src_p) {
		chunk.Initialize(context, src.Types());
		fmts.resize(src.Types().size());
	}

	void EnsureChunk(idx_t row) {
		idx_t ci = row / STANDARD_VECTOR_SIZE;
		if (ci != chunk_idx) {
			chunk_idx = ci;
			src.FetchChunk(chunk_idx, chunk);
			fmts_valid = false;
		}
		if (!fmts_valid) {
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				chunk.data[c].ToUnifiedFormat(chunk.size(), fmts[c]);
			}
			fmts_valid = true;
		}
	}

	Value GetValue(idx_t column, idx_t row) {
		EnsureChunk(row);
		idx_t local = row % STANDARD_VECTOR_SIZE;
		auto &fmt = fmts[column];
		auto si = fmt.sel->get_index(local);
		if (!fmt.validity.RowIsValid(si)) {
			return Value(chunk.data[column].GetType());
		}
		auto &type = chunk.data[column].GetType();
		switch (type.id()) {
		case LogicalTypeId::DOUBLE: {
			auto data = UnifiedVectorFormat::GetData<double>(fmt);
			return Value(data[si]);
		}
		case LogicalTypeId::FLOAT: {
			auto data = UnifiedVectorFormat::GetData<float>(fmt);
			return Value(data[si]);
		}
		case LogicalTypeId::INTEGER: {
			auto data = UnifiedVectorFormat::GetData<int32_t>(fmt);
			return Value(data[si]);
		}
		case LogicalTypeId::BIGINT: {
			auto data = UnifiedVectorFormat::GetData<int64_t>(fmt);
			return Value(data[si]);
		}
		case LogicalTypeId::SMALLINT: {
			auto data = UnifiedVectorFormat::GetData<int16_t>(fmt);
			return Value::SMALLINT(data[si]);
		}
		case LogicalTypeId::TINYINT: {
			auto data = UnifiedVectorFormat::GetData<int8_t>(fmt);
			return Value::TINYINT(data[si]);
		}
		case LogicalTypeId::BOOLEAN: {
			auto data = UnifiedVectorFormat::GetData<bool>(fmt);
			return Value::BOOLEAN(data[si]);
		}
		case LogicalTypeId::VARCHAR: {
			auto data = UnifiedVectorFormat::GetData<string_t>(fmt);
			return Value(data[si].GetString());
		}
		default:
			return chunk.data[column].GetValue(local);
		}
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
//! caller-owned reusable chunk (initialized on first use).
static void AppendRowToBuffer(ClientContext &context, ColumnDataCollection &buffer,
                              ColumnDataAppendState &append_state, idx_t pos, const vector<idx_t> &src_pos,
                              SourceReader &src, idx_t src_row, DataChunk &scratch,
                              const ColumnDataCollection *&scratch_owner) {
	if (scratch_owner != &buffer) {
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

//! Batch appender: accumulates rows in a scratch chunk and flushes to the
//! ColumnDataCollection every STANDARD_VECTOR_SIZE rows. This reduces the
//! number of Append calls by ~2048x compared to per-row append.
//! Each target's insert/update buffer gets its own BatchAppender.
struct BatchAppender {
	ColumnDataCollection *buffer = nullptr;
	ColumnDataAppendState *append_state = nullptr;
	DataChunk scratch;
	bool initialized = false;

	void Init(ClientContext &context, ColumnDataCollection &buf, ColumnDataAppendState &state) {
		buffer = &buf;
		append_state = &state;
		scratch.Initialize(context, buf.Types());
		initialized = true;
	}

	void AppendRow(ClientContext &context, idx_t pos, const vector<idx_t> &src_pos,
	               SourceReader &src, idx_t src_row) {
		idx_t row_idx = scratch.size();
		scratch.SetCardinality(row_idx + 1);
		scratch.SetValue(0, row_idx, Value::BIGINT(NumericCast<int64_t>(pos)));
		for (idx_t c = 0; c < src_pos.size(); c++) {
			Value v = src.GetValue(src_pos[c], src_row);
			scratch.SetValue(1 + c, row_idx, v);
		}
		if (scratch.size() >= STANDARD_VECTOR_SIZE) {
			Flush();
		}
	}

	void Flush() {
		if (scratch.size() > 0) {
			buffer->Append(*append_state, scratch);
			scratch.Reset();
		}
	}
};

//! Batch-append multiple source rows to a target buffer. For each row, the
//! position column is set from `positions[i]`, and the value columns are
//! copied from the source collection via VectorOperations::Copy (vectorized
//! bulk copy, not per-row SetValue). The source rows must be in the same
//! chunk or adjacent chunks of the source ColumnDataCollection.
//!
//! `row_indices` maps buffer row → src_row index in the source collection.
//! `positions` maps buffer row → the part-local position (BIGINT col 0).
//! `src_pos` maps buffer column 1..N → source collection column index.
static void AppendRowsToBuffer(ClientContext &context, ColumnDataCollection &buffer,
                               ColumnDataAppendState &append_state,
                               const vector<idx_t> &row_indices, const vector<idx_t> &positions,
                               const vector<idx_t> &src_pos, SourceReader &src,
                               DataChunk &scratch, const ColumnDataCollection *&scratch_owner) {
	if (row_indices.empty()) {
		return;
	}
	if (scratch_owner != &buffer) {
		scratch.~DataChunk();
		new (&scratch) DataChunk();
		scratch.Initialize(context, buffer.Types());
		scratch_owner = &buffer;
	}
	idx_t n = row_indices.size();
	idx_t buf_pos = 0;
	while (buf_pos < n) {
		// Process up to STANDARD_VECTOR_SIZE rows at a time
		idx_t batch_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, n - buf_pos);
		scratch.Reset();
		scratch.SetCardinality(batch_size);

		// Position column (BIGINT)
		auto &pos_vec = scratch.data[0];
		auto pos_data = FlatVector::GetData<int64_t>(pos_vec);
		for (idx_t i = 0; i < batch_size; i++) {
			pos_data[i] = NumericCast<int64_t>(positions[buf_pos + i]);
		}

		// Value columns: fetch from source via SourceReader (still per-row,
		// but batched into a single chunk append — the append overhead
		// dominates, not the GetValue calls, since SourceReader caches chunks)
		for (idx_t c = 0; c < src_pos.size(); c++) {
			for (idx_t i = 0; i < batch_size; i++) {
				Value v = src.GetValue(src_pos[c], row_indices[buf_pos + i]);
				scratch.SetValue(1 + c, i, v);
			}
		}
		buffer.Append(append_state, scratch);
		buf_pos += batch_size;
	}
}

//! Stage + commit one mutation: rewrite every affected part into
//! _tmp/transaction-<txid>/, move the parts into place (atomic per move),
//! delete the superseded parts, and remove delete-emptied single-part partitions
//! from every group.
static void ExecuteAndCommit(ClientContext &context, const MutateBindData &bind, MutateGlobalState &gstate,
                             vector<TargetMap> &targets) {
	auto &fs = FileSystem::GetFileSystem(context);
	// Acquire the table-level write lock (file-based mutual exclusion across
	// concurrent aligned_upsert/aligned_delete/aligned_compact invocations).
	TableWriteLock write_lock(fs, bind.plan.table_path);
	StagedTransaction txn(fs, bind.plan.table_path);
	gstate.txid = txn.txid;
	const string &tmp_root = txn.tmp_root;
	std::set<string> removed_partitions;

	// Pass A: stage every rewrite (visible to readers only after the move).
	// Collect all targets that need rewriting (independent of each other).
		struct RewriteTask {
			MutateTarget *target;
			PartMergeInput input;
			const GroupPlan *group;
		};
		vector<RewriteTask> rewrite_tasks;
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
				t.staged_path = staged_dir + "/" + FormatPartName(t.part_index, 0);
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
				in.rgs = ALIGNED_DEFAULT_RG_ROWS;
				rewrite_tasks.push_back({&t, in, &group});
			}
		}

		// Execute rewrites in parallel (each RewritePart is independent: own
		// reader/writer, own buffers, no shared mutable state). The DuckDB
		// BufferManager and FileSystem are thread-safe; ColumnDataCollection
		// buffers are read-only during rewrite.
		auto num_threads = MinValue<idx_t>(rewrite_tasks.size(),
		                                   std::thread::hardware_concurrency());
		if (num_threads <= 1 || rewrite_tasks.size() <= 1) {
			// Serial path (single rewrite or single-core)
			for (auto &task : rewrite_tasks) {
				task.target->new_row_count = RewritePart(context, task.input);
				if (task.target->new_row_count == 0 && !task.target->empty_part) {
					removed_partitions.insert(task.target->partition_key);
					continue;
				}
				gstate.parts_rewritten++;
			}
		} else {
			// Parallel path
			std::atomic<idx_t> next_task {0};
			std::atomic<idx_t> parts_done {0};
			std::atomic<bool> failed {false};
			std::string error_msg;
			std::mutex error_mutex;
			std::mutex removed_mutex;
			vector<std::thread> threads;
			threads.reserve(num_threads);
			for (idx_t ti = 0; ti < num_threads; ti++) {
				threads.emplace_back([&]() {
					while (true) {
						if (failed.load()) {
							return;
						}
						idx_t task_idx = next_task.fetch_add(1);
						if (task_idx >= rewrite_tasks.size()) {
							return;
						}
						auto &task = rewrite_tasks[task_idx];
						try {
							task.target->new_row_count = RewritePart(context, task.input);
							if (task.target->new_row_count == 0 && !task.target->empty_part) {
								std::lock_guard<std::mutex> lock(removed_mutex);
								removed_partitions.insert(task.target->partition_key);
							} else {
								parts_done.fetch_add(1);
							}
						} catch (std::exception &e) {
							bool expected = false;
							if (failed.compare_exchange_strong(expected, true)) {
								std::lock_guard<std::mutex> lock(error_mutex);
								error_msg = e.what();
							}
							return;
						} catch (...) {
							bool expected = false;
							if (failed.compare_exchange_strong(expected, true)) {
								std::lock_guard<std::mutex> lock(error_mutex);
								error_msg = "unknown error during parallel rewrite";
							}
							return;
						}
					}
				});
			}
			for (auto &t : threads) {
				t.join();
			}
			if (failed.load()) {
				throw IOException("Aligned table: parallel rewrite failed: %s", error_msg);
			}
			gstate.parts_rewritten += parts_done.load();
		}
		// Pass B: move the staged parts into place, then delete the superseded
		// old parts (same-name updates are replaced by the move).
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			auto &group = bind.plan.groups[gi];
			for (auto &kv : targets[gi]) {
				auto &t = *kv.second;
				if (t.removed || t.staged_path.empty() || (t.new_row_count == 0 && !t.empty_part)) {
					continue;
				}
				string final_name = FormatPartName(t.part_index, t.new_row_count);
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
	// RAII StagedTransaction destructor cleans up _tmp/transaction-<id>/ and
	// the _tmp/ parent directory.
}

//! Vectorized extraction of SortedRow entries from a source ColumnDataCollection.
//! Scans the collection chunk-by-chunk, extracts the date (partition source)
//! and symbol columns at the given positions, evaluates the partition
//! template, and appends SortedRow entries. Throws on NULL date. Shared by
//! AlignedUpsertFunction and AlignedDeleteFunction.
static vector<SortedRow> ExtractSortedRows(ClientContext &context, const ColumnDataCollection &src,
                                           idx_t date_pos, idx_t symbol_pos,
                                           const string &date_col_name,
                                           const string &template_str) {
	vector<SortedRow> rows;
	rows.reserve(src.Count());
	ColumnDataScanState scan_state;
	src.InitializeScan(scan_state);
	DataChunk src_chunk;
	src_chunk.Initialize(context, src.Types());
	idx_t row_offset = 0;
	while (true) {
		src_chunk.Reset();
		src.Scan(scan_state, src_chunk);
		if (src_chunk.size() == 0) {
			break;
		}
		idx_t n = src_chunk.size();
		auto &date_vec = src_chunk.data[date_pos];
		bool date_is_null = date_vec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		                    ConstantVector::IsNull(date_vec);
		UnifiedVectorFormat date_fmt;
		date_vec.ToUnifiedFormat(n, date_fmt);
		auto date_sel = date_fmt.sel;
		bool is_timestamp = date_vec.GetType().id() == LogicalTypeId::TIMESTAMP;
		auto &sym_vec = src_chunk.data[symbol_pos];
		UnifiedVectorFormat sym_fmt;
		sym_vec.ToUnifiedFormat(n, sym_fmt);
		auto sym_sel = sym_fmt.sel;
		// For VARCHAR symbols, extract string_t directly (avoids Value
		// construction + string copy overhead on every row).
		auto sym_type = sym_vec.GetType().id();
		string_t const *sym_str_data = nullptr;
		if (sym_type == LogicalTypeId::VARCHAR) {
			sym_str_data = UnifiedVectorFormat::GetData<string_t>(sym_fmt);
		}
		for (idx_t i = 0; i < n; i++) {
			idx_t r = row_offset + i;
			auto di = date_sel->get_index(i);
			if (date_is_null || !date_fmt.validity.AllValid() ||
			    !date_fmt.validity.RowIsValid(di)) {
				throw IOException("Aligned table: NULL in the partition source column '%s' at source row %llu",
				                  date_col_name, r);
			}
			SortedRow row;
			int64_t d;
			if (is_timestamp) {
				auto tptr = UnifiedVectorFormat::GetData<int64_t>(date_fmt);
				d = tptr[di]; // full timestamp value (not truncated to date)
			} else {
				auto dptr = UnifiedVectorFormat::GetData<int32_t>(date_fmt);
				d = static_cast<int64_t>(dptr[di]);
			}
			if (!template_str.empty() && !EvaluatePartitionTemplate(template_str, d, row.partition_key)) {
				throw IOException("Aligned table: cannot evaluate partition template '%s'", template_str);
			}
			auto si = sym_sel->get_index(i);
			if (!sym_fmt.validity.RowIsValid(si)) {
				throw IOException("Aligned table: NULL in the symbol column at source row %llu", r);
			}
			if (sym_str_data) {
				// Fast path: construct Value from string_t (zero-copy for
				// inline strings, one alloc for long strings).
				row.symbol = Value(sym_str_data[si].GetString());
			} else {
				row.symbol = sym_vec.GetValue(si);
			}
			row.date = d;
			row.src_row = r;
			rows.push_back(std::move(row));
		}
		row_offset += n;
	}
	return rows;
}

//! The index group's partition template (the mutator only needs the index
//! group's single template — partition keys are index-defined). For an empty
//! table (first write), defaults to "month=%Y-%m".
static string IndexTemplate(const MutateBindData &bind) {
	auto &index_group = bind.plan.groups[0];
	if (index_group.manifest.partitioning.empty()) {
		return "month=%Y-%m"; // default for first write of an empty table
	}
	return index_group.manifest.partitioning[0].template_str;
}

//===----------------------------------------------------------------------===//
// AlignedUpsertFunction (file-local — called by AlignedUpsertFromCollection)
//===----------------------------------------------------------------------===//

static void AlignedUpsertFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<MutateBindData>();
	auto &gstate = data.global_state->Cast<MutateGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &index_group = bind.plan.groups[0];
	string template_str = IndexTemplate(bind);

	// 1. Read the needed source columns.
	unique_ptr<ColumnDataCollection> src;
	if (bind.source_collection) {
		src = ReadSourceFromCollection(context, *bind.source_collection, bind.needed_names, bind.source_col_names);
	} else {
		src = ReadSourceColumns(context, bind.source_path, bind.needed_names);
	}
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

	// 3. Build + sort + dedupe the key list.
	auto rows = ExtractSortedRows(context, *src, date_pos, symbol_pos, bind.date_col, template_str);
	SortAndDedupe(rows);

	// Early exit for empty input: no rows to upsert → return zero counts
	// without acquiring the write lock or creating a staged transaction.
	if (rows.empty()) {
		output.SetCardinality(1);
		output.SetValue(0, 0, Value::BIGINT(0));
		output.SetValue(1, 0, Value::BIGINT(0));
		output.SetValue(2, 0, Value::BIGINT(0));
		output.SetValue(3, 0, Value::BIGINT(0));
		return;
	}

	// 4. Pre-load partition boundaries for all existing partitions that
	//    appear in the source data. KeyResolver::Resolve triggers
	// LoadPartitionBoundaries (RG stats only, no data read) on first
	// access per partition; if the key's symbol is outside the partition's
	// range, it fast-rejects without loading any data. Only keys within
	// the symbol range trigger LoadPartition (reads symbol+date columns).
	// The partition cache means each partition's data is loaded at most
	// once regardless of how many keys fall in it.
	KeyResolver resolver(context, bind.plan);

	// Batch resolve all keys.
	vector<KeyLocation> locs(rows.size());
	for (idx_t i = 0; i < rows.size(); i++) {
		locs[i] = resolver.Resolve(rows[i].date, rows[i].symbol);
		if (locs[i].found) {
			gstate.rows_updated++;
		} else {
			gstate.rows_inserted++;
		}
	}

	// 6. Dispatch to per-group targets.
	vector<TargetMap> targets(bind.plan.groups.size());

	// 6.5 Append-to-last cross-group validation (same as before — the resolver
	// optimistically set append_to_last; we must validate across all groups).
	std::set<string> fallback_partitions;
	for (idx_t i = 0; i < locs.size(); i++) {
		auto &loc = locs[i];
		if (!loc.append_to_last) {
			continue;
		}
		if (fallback_partitions.count(loc.partition_key)) {
			continue;
		}
		idx_t last_idx = loc.part_index;
		idx_t last_rows = loc.part_local_row;
		bool ok = true;
		for (idx_t gi = 0; gi < bind.plan.groups.size() && ok; gi++) {
			auto &group = bind.plan.groups[gi];
			auto *gp = FindPartition(group, loc.partition_key);
			if (!gp) {
				if (!bind.group_mapping[gi].col_names.empty()) {
					ok = false;
				}
				continue;
			}
			auto &last_part = group.parts[gp->first_part + gp->part_count - 1];
			if (last_part.partition_index != last_idx || last_part.row_count != last_rows ||
			    last_part.row_count >= ALIGNED_DEFAULT_PART_ROWS) {
				ok = false;
				break;
			}
			if (!bind.group_mapping[gi].col_names.empty()) {
				auto reader =
				    make_uniq<ParquetReader>(context, OpenFileInfo(last_part.path), ParquetOptions(context));
				for (auto &mc : bind.group_mapping[gi].col_names) {
					bool found = false;
					for (auto &rc : reader->columns) {
						if (StringUtil::CIEquals(rc.name, mc)) {
							found = true;
							break;
						}
					}
					if (!found) {
						ok = false;
						break;
					}
				}
			}
		}
		if (!ok) {
			fallback_partitions.insert(loc.partition_key);
		}
	}
	for (auto &key : fallback_partitions) {
		idx_t next_idx = NextPartIndexForPartition(bind.plan, key);
		for (idx_t i = 0; i < locs.size(); i++) {
			if (locs[i].partition_key == key && locs[i].append_to_last) {
				locs[i].append_to_last = false;
				locs[i].append_new_part = true;
				locs[i].part_index = next_idx;
				locs[i].part_local_row = 0;
			}
		}
	}

	// 7. Batch dispatch: for each key, route to the appropriate target buffer.
	// Uses BatchAppender to accumulate up to STANDARD_VECTOR_SIZE rows per
	// target before flushing to the ColumnDataCollection — reducing Append
	// calls by ~2048x vs per-row append.
	// First pass: classify each (row, group) → (target, mode, position).
	// Second pass: append via BatchAppender.
	struct DispatchEntry {
		idx_t row_idx;      // index into rows[]
		idx_t gi;           // group index
		MutateTarget *target;
		bool is_update;     // true=update_buffer, false=insert_buffer
		idx_t pos;          // part-local position
		bool is_synth;      // synth path
		idx_t global_pos;   // for synth_values key
	};
	vector<DispatchEntry> dispatches;
	dispatches.reserve(rows.size() * bind.plan.groups.size());

	for (idx_t i = 0; i < rows.size(); i++) {
		auto &loc = locs[i];
		idx_t src_row = rows[i].src_row;
		idx_t p;
		IndexPartOffset(index_group, loc.partition_key, loc.part_index, p);
		p += loc.part_local_row;
		bool fresh = FindPartition(index_group, loc.partition_key) == nullptr;
		for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
			auto &group = bind.plan.groups[gi];
			if (loc.found) {
				bool only_keys = !bind.group_mapping[gi].col_names.empty();
				for (auto &col : bind.group_mapping[gi].col_names) {
					if (!StringUtil::CIEquals(col, bind.date_col) &&
					    !StringUtil::CIEquals(col, bind.symbol_col)) {
						only_keys = false;
						break;
					}
				}
				if (only_keys) {
					continue;
				}
			}
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
			} else if (loc.append_to_last) {
				part = FindIndexPart(group, loc.partition_key, loc.part_index);
				if (!part) {
					continue;
				}
				local = loc.part_local_row;
				target_idx = part->partition_index;
			} else if (loc.append_new_part) {
				if (!FindPartition(group, loc.partition_key)) {
					continue;
				}
				target_idx = loc.part_index;
				local = 0;
			} else if ((part = FindPartByPosition(group, loc.partition_key, p, local))) {
				target_idx = part->partition_index;
			} else if (fresh && !bind.group_mapping[gi].col_names.empty()) {
				target_idx = 0;
			} else if (loc.found && !bind.group_mapping[gi].col_names.empty()) {
				target_idx = 0;
				do_synth = true;
			} else {
				continue;
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
			if (loc.found && target.synth) {
				// Synth path: capture values immediately (needs reader).
				vector<Value> vals;
				for (idx_t c = 0; c < bind.group_mapping[gi].src_pos.size(); c++) {
					vals.push_back(reader.GetValue(bind.group_mapping[gi].src_pos[c], src_row));
				}
				target.synth_values[p] = std::move(vals);
			} else {
				dispatches.push_back({i, gi, &target, loc.found, loc.found ? local : (part ? local : 0),
				                      false, p});
				if (!loc.found) {
					dispatches.back().pos = part ? local : target.insert_next++;
				}
			}
		}
	}

	// Second pass: batch-append each dispatch entry via BatchAppender.
	// Group dispatches by (target, is_update) to reuse the BatchAppender.
	// Since rows are sorted by (partition, symbol, date), dispatches to the
	// same target tend to be contiguous — maximizing batch efficiency.
	struct AppenderKey {
		MutateTarget *target;
		bool is_update;
		bool operator<(const AppenderKey &o) const {
			if (target != o.target) return target < o.target;
			return is_update < o.is_update;
		}
	};
	std::map<AppenderKey, BatchAppender> appenders;
	for (auto &d : dispatches) {
		AppenderKey key{d.target, d.is_update};
		auto it = appenders.find(key);
		if (it == appenders.end()) {
			auto &ap = appenders[key];
			auto &buf = d.is_update ? *d.target->update_buffer : *d.target->insert_buffer;
			auto &state = d.is_update ? d.target->update_append : d.target->insert_append;
			ap.Init(context, buf, state);
			it = appenders.find(key);
		}
		it->second.AppendRow(context, d.pos, bind.group_mapping[d.gi].src_pos,
		                     reader, rows[d.row_idx].src_row);
		if (!d.is_update) {
			d.target->inserts_count++;
		}
	}
	// Flush all appenders.
	for (auto &kv : appenders) {
		kv.second.Flush();
	}

	// 8. Fill synthesized parts (vectorized — unchanged).
	for (idx_t gi = 0; gi < bind.plan.groups.size(); gi++) {
		for (auto &kv : targets[gi]) {
			auto &t = *kv.second;
			if (!t.synth) {
				continue;
			}
			const auto &mtypes = bind.group_mapping[gi].col_types;
			const auto &buf_types = t.insert_buffer->Types();
			DataChunk chunk;
			chunk.Initialize(context, buf_types);
			for (idx_t pos = 0; pos < t.synth_rows; pos += STANDARD_VECTOR_SIZE) {
				idx_t n = MinValue<idx_t>(STANDARD_VECTOR_SIZE, t.synth_rows - pos);
				chunk.Reset();
				chunk.SetCardinality(n);
				auto &pos_vec = chunk.data[0];
				auto pos_data = FlatVector::GetData<int64_t>(pos_vec);
				for (idx_t i = 0; i < n; i++) {
					pos_data[i] = NumericCast<int64_t>(pos + i);
				}
				for (idx_t c = 0; c < mtypes.size(); c++) {
					auto &vec = chunk.data[1 + c];
					auto &validity = FlatVector::Validity(vec);
					validity.SetAllInvalid(n);
					for (idx_t i = 0; i < n; i++) {
						idx_t key_pos = pos + i;
						auto it = t.synth_values.find(key_pos);
						if (it != t.synth_values.end()) {
							chunk.SetValue(1 + c, i, it->second[c]);
						}
					}
				}
				t.insert_buffer->Append(t.insert_append, chunk);
			}
			t.inserts_count = t.synth_rows;
			t.synth_values.clear();
		}
	}

	// 9. Rewrite + commit.
	ExecuteAndCommit(context, bind, gstate, targets);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_inserted)));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_updated)));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(gstate.parts_rewritten)));
	output.SetValue(3, 0, Value::BIGINT(NumericCast<int64_t>(gstate.txid)));
}

//===----------------------------------------------------------------------===//
// AlignedDeleteFunction (file-local — called by AlignedDeleteFromCollection)
//===----------------------------------------------------------------------===//

static void AlignedDeleteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<MutateBindData>();
	auto &gstate = data.global_state->Cast<MutateGlobalState>();
	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}
	gstate.done = true;

	auto &index_group = bind.plan.groups[0];
	string template_str = IndexTemplate(bind);

	// 1. Read the keys source's two key columns. When source_collection is
	//    set (internal API from PhysicalAlignedDelete), read from the in-memory
	//    collection instead of opening a parquet file.
	vector<string> key_names = {bind.date_col, bind.symbol_col};
	unique_ptr<ColumnDataCollection> src;
	if (bind.source_collection) {
		src = ReadSourceFromCollection(context, *bind.source_collection, key_names,
		                               bind.source_col_names.empty() ? vector<string> {"symbol", "date"} : bind.source_col_names);
	} else {
		src = ReadSourceColumns(context, bind.source_path, key_names);
	}
	SourceReader reader(context, *src);

	// 2. Build + sort + dedupe the key list (vectorized chunk scan).
	// date is column 0, symbol is column 1 (key_names = {date_col, symbol_col})
	auto rows = ExtractSortedRows(context, *src, 0, 1, bind.date_col, template_str);
	SortAndDedupe(rows);

	// Early exit for empty input: no rows to delete → return zero counts
	// without acquiring the write lock or creating a staged transaction.
	if (rows.empty()) {
		output.SetCardinality(1);
		output.SetValue(0, 0, Value::BIGINT(0));
		output.SetValue(1, 0, Value::BIGINT(0));
		output.SetValue(2, 0, Value::BIGINT(0));
		output.SetValue(3, 0, Value::BIGINT(0));
		return;
	}

	// 3. Resolve every key; non-existent keys are skipped (idempotent delete).
	KeyResolver resolver(context, bind.plan);
	vector<KeyLocation> locs(rows.size());
	for (idx_t i = 0; i < rows.size(); i++) {
		locs[i] = resolver.Resolve(rows[i].date, rows[i].symbol);
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

	// 5. Pre-check: a part emptied by deletes must be handled without
	//    breaking the index-consecutiveness contract — either it is the only
	//    part of its partition (whole-partition removal) or it is the group's
	//    highest-index part in that partition (part-file removal; remaining
	//    indexes stay consecutive). An interior part emptied by deletes is
	//    rewritten to a 0-row file in-place (empty_part), preserving its
	//    index so the remaining indexes stay consecutive.
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
			// Interior part emptied by delete: rewrite to a 0-row file in-place
			// (preserves the part index, keeping the remaining indexes consecutive).
			t.empty_part = true;
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