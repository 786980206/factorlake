#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class ParquetWriter;

//! Creates a ParquetWriter with the standard aligned-extension options
//! (ZSTD compression, V1 parquet/geoparquet, ZStdFileSystem default level).
//! Shared by aligned_create (WriteEmptyParquet/WriteNullParquet), the
//! part_rewriter, and the aligned_compactor so the writer parameters stay
//! identical across all three write paths.
unique_ptr<ParquetWriter> CreateParquetWriter(ClientContext &context, FileSystem &fs,
                                              const string &path,
                                              const vector<string> &col_names,
                                              const vector<LogicalType> &col_types);

} // namespace duckdb
