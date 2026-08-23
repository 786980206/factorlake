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

//! Formats a part file name as "{idx:04d}-{rows:10d}.parquet".
//! Shared by aligned_create, aligned_mutator, and aligned_compactor.
string FormatPartName(idx_t index, idx_t rows);

//! Parses a part file base name (without directory) into its index and row
//! count. Returns false if the name is malformed. The name must be exactly
//! "{idx:04d}-{rows:10d}.parquet" (15 chars + ".parquet" = 24 chars total).
bool ParsePartName(const string &file_name, idx_t &index, idx_t &rows);

} // namespace duckdb
