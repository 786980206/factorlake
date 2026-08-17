cmake_minimum_required(VERSION 3.5)
project(AlignedExtension)

# Reuse the DuckDB Parquet extension headers (ParquetReader) directly.
include_directories(
  "${CMAKE_SOURCE_DIR}/extension/parquet/include"
)

set(ALIGNED_EXTENSION_FILES
    src/extension.cpp
    src/catalog/manifest.cpp
    src/resolver/row_space.cpp
    src/scan/aligned_scan.cpp
)

# Phase 1: statically linked into the DuckDB binary (dev loop).
# (build_loadable_extension will be added later for distribution.)
build_static_extension(aligned ${ALIGNED_EXTENSION_FILES})
