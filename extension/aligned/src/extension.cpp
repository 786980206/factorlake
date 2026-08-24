#include "aligned_extension.hpp"

#include "catalog/aligned_catalog.hpp"
#include "catalog/aligned_create_fn.hpp"
#include "catalog/aligned_groups.hpp"
#include "compaction/aligned_compactor.hpp"
#include "compaction/aligned_drop.hpp"
#include "copy/aligned_copy.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "scan/aligned_scan.hpp"

namespace duckdb {

void AlignedExtension::Load(ExtensionLoader &loader) {
	// aligned_scan(table_name, root=...)
	// Scan a logical AlignedTable: reads parquet column groups and assembles
	// them into a single DataChunk (no JOIN, no materialization).
	TableFunction aligned_scan_fn("aligned_scan", {LogicalType::VARCHAR}, AlignedScanFunction,
	                              AlignedBind, AlignedInitGlobal, AlignedInitLocal);
	aligned_scan_fn.named_parameters["root"] = LogicalType::VARCHAR;
	aligned_scan_fn.cardinality = AlignedCardinality;
	aligned_scan_fn.projection_pushdown = true;
	// Filter pushdown lets partition/row-group pruning reach the aligned scan.
	aligned_scan_fn.filter_pushdown = true;
	// filter_prune=true: prune filter-only columns from the scan output.
	aligned_scan_fn.filter_prune = true;
	loader.RegisterFunction(aligned_scan_fn);

	// aligned_groups(table_name, root=...)
	// List all column groups in an AlignedTable.
	TableFunction aligned_groups_fn("aligned_groups", {LogicalType::VARCHAR},
	                                 AlignedGroupsFunction, AlignedGroupsBind, AlignedGroupsInitGlobal, nullptr);
	aligned_groups_fn.named_parameters["root"] = LogicalType::VARCHAR;
	loader.RegisterFunction(aligned_groups_fn);

	// Setting that supplies the default data root for aligned_scan(name)
	auto &db = loader.GetDatabaseInstance();
	db.config.AddExtensionOption("aligned_data_root", "Root directory for AlignedTable logical tables",
	                             LogicalType::VARCHAR);

	// aligned_compact(table_name, group_name, root=...)
	// Phase 7: merge a group's parts per partition directory (atomic switch)
	TableFunction aligned_compact_fn("aligned_compact", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 AlignedCompactFunction, AlignedCompactBind, AlignedCompactInitGlobal, nullptr);
	aligned_compact_fn.named_parameters["root"] = LogicalType::VARCHAR;
	loader.RegisterFunction(aligned_compact_fn);

	// aligned_drop(table_name, group_name, root=...)
	// Drop a column group (all partitions) or the entire table (index).
	// group_name = "index" deletes the whole table directory; other names
	// delete only that group's directory tree. Returns (dirs_removed,
	// files_removed, txid).
	TableFunction aligned_drop_fn("aligned_drop", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                              AlignedDropFunction, AlignedDropBind, AlignedDropInitGlobal, nullptr);
	aligned_drop_fn.named_parameters["root"] = LogicalType::VARCHAR;
	loader.RegisterFunction(aligned_drop_fn);

	// aligned_create(table_name, group_name [, columns], root=..., partition_template=...)
	// Create a new AlignedTable (group_name='index') or extend an existing
	// table with a new column group (group_name='factor/alpha'). `columns` is
	// an optional column-definition string (omit to create an empty group
	// whose schema is inferred on first COPY TO). Returns (dirs_created,
	// files_created, txid).
	TableFunction aligned_create_fn_2("aligned_create",
	                                  {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                  AlignedCreateFunction, AlignedCreateBind, AlignedCreateInitGlobal, nullptr);
	aligned_create_fn_2.named_parameters["root"] = LogicalType::VARCHAR;
	aligned_create_fn_2.named_parameters["partition_template"] = LogicalType::VARCHAR;
	TableFunction aligned_create_fn_3("aligned_create",
	                                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                  AlignedCreateFunction, AlignedCreateBind, AlignedCreateInitGlobal, nullptr);
	aligned_create_fn_3.named_parameters["root"] = LogicalType::VARCHAR;
	aligned_create_fn_3.named_parameters["partition_template"] = LogicalType::VARCHAR;
	TableFunctionSet aligned_create_set("aligned_create");
	aligned_create_set.functions.push_back(std::move(aligned_create_fn_2));
	aligned_create_set.functions.push_back(std::move(aligned_create_fn_3));
	loader.RegisterFunction(std::move(aligned_create_set));

	// Phase 8: DuckLake-style storage extension. ATTACH '<root>' AS name
	// (TYPE ALIGNED) creates a logical catalog over the parquet column groups:
	// SELECT reads the parquet files directly (no materialization).
	RegisterAlignedStorageExtension(db);

	// COPY TO (FORMAT aligned, GROUP '...') — bulk write to a column group.
	// Routes through DuckDB's COPY TO framework with aligned-specific:
	//   - Hive partitioning by the index group's date/timestamp column
	//   - Self-describing part file names {idx:04d}-{rows:10d}.parquet
	//   - ZSTD compression, RG flush at 131072, part limit at 1048576 rows
	//   - OVERWRITE of the target group's partitions
	loader.RegisterFunction(GetAlignedCopyFunction());

	// Phase 4: Parquet metadata cache (footer / schema / row-group stats, LRU
	// via DuckDB's ObjectCache, 8 GiB, validity-checked on access). The parquet
	// extension registers the option; make it default ON so repeated aligned
	// scans of the same parts skip footer parsing. Reuse DuckDB's cache rather
	// than building our own (contract §8).
	auto &config = DBConfig::GetConfig(db);
	if (!config.HasExtensionOption("parquet_metadata_cache")) {
		config.AddExtensionOption("parquet_metadata_cache",
		                          "Cache Parquet metadata - useful when reading the same files multiple times",
		                          LogicalType::BOOLEAN, Value(true));
	} else {
		config.SetOptionByName("parquet_metadata_cache", Value::BOOLEAN(true));
	}
}

std::string AlignedExtension::Name() {
	return "aligned";
}

std::string AlignedExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb

// Loadable extension entrypoint (used when the extension is built with
// build_loadable_extension and loaded via INSTALL/LOAD, e.g. from a GitHub
// Release. The statically-linked build path uses the generated extension
// loader instead and never calls this function.)
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(aligned, loader) {
	duckdb::AlignedExtension ext;
	ext.Load(loader);
}
}
