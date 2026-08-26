#include "io/parquet_io.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "parquet_field_id.hpp"
#include "parquet_shredding.hpp"
#include "zstd_file_system.hpp"

namespace duckdb {

unique_ptr<ParquetWriter> CreateParquetWriter(ClientContext &context, FileSystem &fs,
                                               const string &path,
                                               const vector<string> &col_names,
                                               const vector<LogicalType> &col_types) {
	return make_uniq<ParquetWriter>(
	    context, fs, path, col_types, col_names,
	    duckdb_parquet::CompressionCodec::ZSTD, ChildFieldIDs(), ShreddingType(),
	    vector<pair<string, string>>(), nullptr, optional_idx(0),
	    0ULL /* string_dictionary_page_size_limit */, 1 /* enable_bloom_filters */,
	    0.01, 1 /* ZSTD compression level (1 = fast) */, ParquetVersion::V1, GeoParquetVersion::V1);
}

string FormatPartName(idx_t index, idx_t rows) {
	return StringUtil::Format("%04llu-%010llu.parquet", (unsigned long long)index,
	                          (unsigned long long)rows);
}

bool ParsePartName(const string &name, idx_t &index, idx_t &rows) {
	if (!StringUtil::EndsWith(name, ".parquet")) {
		return false;
	}
	string base = name.substr(0, name.size() - 8); // strip ".parquet"
	if (base.size() != 15 || base[4] != '-') {
		return false; // 4 digits + '-' + 10 digits
	}
	for (idx_t i = 0; i < 15; i++) {
		if (i == 4) {
			continue;
		}
		if (base[i] < '0' || base[i] > '9') {
			return false;
		}
	}
	index = (idx_t)std::stoull(base.substr(0, 4));
	rows = (idx_t)std::stoull(base.substr(5));
	return true;
}

void WriteEmptyParquet(ClientContext &context, FileSystem &fs, const string &path,
                       const vector<string> &col_names, const vector<LogicalType> &col_types) {
	auto writer = CreateParquetWriter(context, fs, path, col_names, col_types);
	auto buffer = make_uniq<ColumnDataCollection>(context, col_types);
	unique_ptr<ParquetWriteTransformData> transform;
	writer->Flush(*buffer, transform);
	writer->Finalize();
}

void WriteNullParquet(ClientContext &context, FileSystem &fs, const string &path,
                      const vector<string> &col_names, const vector<LogicalType> &col_types,
                      idx_t row_count) {
	auto writer = CreateParquetWriter(context, fs, path, col_names, col_types);

	auto buffer = make_uniq<ColumnDataCollection>(context, col_types);
	ColumnDataAppendState append_state;
	buffer->InitializeAppend(append_state);

	DataChunk chunk;
	chunk.Initialize(context, col_types);
	idx_t remaining = row_count;
	while (remaining > 0) {
		idx_t batch = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
		chunk.Reset();
		chunk.SetCardinality(batch);
		for (idx_t c = 0; c < col_types.size(); c++) {
			FlatVector::Validity(chunk.data[c]).SetAllInvalid(batch);
		}
		buffer->Append(append_state, chunk);
		remaining -= batch;
	}

	unique_ptr<ParquetWriteTransformData> transform;
	writer->Flush(*buffer, transform);
	writer->Finalize();
}

unique_ptr<ParquetReader> OpenPartReaderAllColumns(ClientContext &context, const string &path,
                                                    ParquetReaderScanState &scan_state) {
	auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(path), ParquetOptions(context));
	for (idx_t i = 0; i < reader->columns.size(); i++) {
		reader->column_ids.push_back(MultiFileLocalColumnId(i));
	}
	vector<PartitionStatistics> rg_stats;
	reader->GetPartitionStats(rg_stats);
	vector<idx_t> all_rgs;
	for (idx_t i = 0; i < rg_stats.size(); i++) {
		all_rgs.push_back(i);
	}
	reader->InitializeScan(context, scan_state, all_rgs);
	return reader;
}

unique_ptr<ParquetReader> OpenPartReaderNamedColumns(ClientContext &context, const string &path,
                                                      const vector<string> &col_names,
                                                      vector<LogicalType> &out_types,
                                                      ParquetReaderScanState &scan_state) {
	auto reader = make_uniq<ParquetReader>(context, OpenFileInfo(path), ParquetOptions(context));
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
		out_types.push_back(reader->columns[pos].type);
		reader->column_ids.push_back(MultiFileLocalColumnId(pos));
	}
	vector<PartitionStatistics> rg_stats;
	reader->GetPartitionStats(rg_stats);
	vector<idx_t> all_rgs;
	for (idx_t i = 0; i < rg_stats.size(); i++) {
		all_rgs.push_back(i);
	}
	reader->InitializeScan(context, scan_state, all_rgs);
	return reader;
}

void CountRecursive(FileSystem &fs, const string &path, idx_t &dirs_count, idx_t &files_count) {
	fs.ListFiles(path, [&](OpenFileInfo &info) {
		if (StringUtil::CIEquals(info.path, ".aligned_write.lock")) {
			return;
		}
		string child = path + "/" + info.path;
		if (fs.DirectoryExists(child)) {
			dirs_count++;
			CountRecursive(fs, child, dirs_count, files_count);
		} else {
			files_count++;
		}
	});
}

} // namespace duckdb
