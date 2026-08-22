#include "io/parquet_io.hpp"

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

} // namespace duckdb
