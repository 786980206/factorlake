cmake_minimum_required(VERSION 3.5)
project(AlignedExtension)

# The extension's own headers (src/include) + the DuckDB Parquet extension
# headers (ParquetReader reuse). Note: DUCKDB_EXTENSION_*_INCLUDE_PATH is only
# registered metadata — the extension CMakeLists must add include dirs itself.
include_directories(
  "${CMAKE_CURRENT_LIST_DIR}/src/include"
  "${CMAKE_SOURCE_DIR}/extension/parquet/include"
  "${CMAKE_SOURCE_DIR}/third_party/parquet"
  "${CMAKE_SOURCE_DIR}/third_party/thrift"
)

set(ALIGNED_EXTENSION_FILES
    src/extension.cpp
    src/catalog/manifest.cpp
    src/resolver/row_space.cpp
    src/resolver/partition_resolver.cpp
    src/scan/aligned_scan.cpp
    src/writer/aligned_writer.cpp
)

# Phase 1: statically linked into the DuckDB binary (dev loop).
# (build_loadable_extension will be added later for distribution.)
build_static_extension(aligned ${ALIGNED_EXTENSION_FILES})
