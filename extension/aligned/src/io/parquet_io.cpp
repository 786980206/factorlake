#include "io/parquet_io.hpp"

#include "duckdb/common/string_util.hpp"
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
	    vector<pair<string, string>>(), nullptr, optional_idx(),
	    1073741824ULL /* MAX_UNCOMPRESSED_DICT_PAGE_SIZE */, 1, 0.01,
	    ZStdFileSystem::DefaultCompressionLevel(), ParquetVersion::V1, GeoParquetVersion::V1);
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

} // namespace duckdb
