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
    src/catalog/aligned_attach.cpp
    src/resolver/row_space.cpp
    src/resolver/partition_resolver.cpp
    src/resolver/key_resolver.cpp
    src/rewriter/part_rewriter.cpp
    src/mutator/aligned_mutator.cpp
    src/scan/aligned_scan.cpp
    src/compaction/aligned_compactor.cpp
)

# Statically linked into the DuckDB binary (dev loop).
build_static_extension(aligned ${ALIGNED_EXTENSION_FILES})

# Loadable binary for distribution (INSTALL/LOAD as a custom extension).
# Links the parquet extension (ParquetReader reuse) + its third-party deps so
# the .duckdb_extension is self-contained. Build with -DEXTENSION_STATIC_BUILD=1
# (official release mode: DuckDB core statically linked into the binary).
build_loadable_extension(aligned "" ${ALIGNED_EXTENSION_FILES})
target_link_libraries(aligned_loadable_extension parquet_extension duckdb_mbedtls duckdb_zstd)
