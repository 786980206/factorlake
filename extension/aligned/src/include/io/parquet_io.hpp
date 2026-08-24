#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class ParquetWriter;
class ParquetReader;
class ParquetReaderScanState;
struct PartitionStatistics;
class ColumnDataCollection;

//! Creates a ParquetWriter with the standard aligned-extension options
//! (ZSTD compression, V1 parquet/geoparquet, ZStdFileSystem default level).
unique_ptr<ParquetWriter> CreateParquetWriter(ClientContext &context, FileSystem &fs,
                                               const string &path,
                                               const vector<string> &col_names,
                                               const vector<LogicalType> &col_types);

//! Formats a part file name as "{idx:04d}-{rows:10d}.parquet".
string FormatPartName(idx_t index, idx_t rows);

//! Parses a part file base name (without directory) into its index and row
//! count. Returns false if the name is malformed.
bool ParsePartName(const string &file_name, idx_t &index, idx_t &rows);

//! Writes a 0-row parquet file (footer carries the schema).
void WriteEmptyParquet(ClientContext &context, FileSystem &fs, const string &path,
                       const vector<string> &col_names, const vector<LogicalType> &col_types);

//! Writes a parquet file with `row_count` rows of all-NULL values.
void WriteNullParquet(ClientContext &context, FileSystem &fs, const string &path,
                      const vector<string> &col_names, const vector<LogicalType> &col_types,
                      idx_t row_count);

//! Opens a ParquetReader for `path`, pushes ALL column ids, collects RG
//! stats, builds the all-RGs vector, and calls InitializeScan. Returns the
//! reader and the initialized scan state. Shared by part_rewriter,
//! aligned_compactor, and ReadPartToCollection.
unique_ptr<ParquetReader> OpenPartReaderAllColumns(ClientContext &context, const string &path,
                                                    ParquetReaderScanState &scan_state);

//! Opens a ParquetReader for `path`, selects only the named columns (by
//! case-insensitive name match), and calls InitializeScan. Returns the reader
//! (with column_ids set) and the scan state. Throws IOException if a name is
//! not found. Shared by the mutator's ReadSourceColumns and key_resolver.
unique_ptr<ParquetReader> OpenPartReaderNamedColumns(ClientContext &context, const string &path,
                                                      const vector<string> &col_names,
                                                      vector<LogicalType> &out_types,
                                                      ParquetReaderScanState &scan_state);

//! Reads ALL columns of a parquet file into a ColumnDataCollection.
//! Shared by aligned_compactor and part_rewriter (the read-all-columns path).
unique_ptr<ColumnDataCollection> ReadPartToCollection(ClientContext &context, const string &path,
                                                       const vector<LogicalType> &col_types);

//! Recursively count files and subdirectories under a path.
//! Skips the `.aligned_write.lock` file (transient, created by TableWriteLock).
void CountRecursive(FileSystem &fs, const string &path, idx_t &dirs_count, idx_t &files_count);

} // namespace duckdb
