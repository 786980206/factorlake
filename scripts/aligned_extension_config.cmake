# AlignedTable: register the `aligned` extension for DuckDB's build.
# Used via: -DDUCKDB_EXTENSION_CONFIGS=<this-file> (see AGENTS.md 搂16).
#
# The extension lives OUTSIDE the duckdb tree (this repo's extension/aligned),
# so we register it with an explicit SOURCE_DIR pointing at it.
duckdb_extension_load(aligned SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/../extension/aligned")
