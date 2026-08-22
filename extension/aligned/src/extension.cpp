#include "aligned_extension.hpp"

#include "catalog/aligned_catalog.hpp"
#include "compaction/aligned_compactor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "mutator/aligned_mutator.hpp"
#include "scan/aligned_scan.hpp"

namespace duckdb {

void AlignedExtension::Load(ExtensionLoader &loader) {
	// aligned_table(table_name, root=...)
	TableFunction aligned_table_fn("aligned_table", {LogicalType::VARCHAR}, AlignedScanFunction, AlignedBind,
	                               AlignedInitGlobal, AlignedInitLocal);
	aligned_table_fn.named_parameters["root"] = LogicalType::VARCHAR;
	aligned_table_fn.cardinality = AlignedCardinality;
	aligned_table_fn.projection_pushdown = true; // Phase 2: only open requested groups/columns
	// Filter pushdown is what lets partition/row-group pruning actually reach
	// the aligned scan: with filter_pushdown the executor passes WHERE predicates
	// into TableFunctionInitInput::filters; the optimizer always keeps filtered
	// columns in column_ids (remove_unused_columns keeps filter columns alive
	// regardless of filter_prune), so the scan can prune partitions / row groups
	// and apply row-level filters on filter-only columns even when they are not
	// projected (verified: SELECT alpha001 WHERE date=... prunes without date
	// being projected).
	aligned_table_fn.filter_pushdown = true;
	// filter_prune=true: additionally prune the filter-only columns from the
	// scan OUTPUT (projection_ids). The scan still reads them (hidden read
	// columns for pruning + row filters) but the output chunk only carries the
	// projected columns, saving the upstream PROJECTION operator. The
	// scratch-chunk + ReferenceColumns path in the scan already implements this.
	aligned_table_fn.filter_prune = true;
	loader.RegisterFunction(aligned_table_fn);

	// aligned_scan(root, table_name)
	TableFunction aligned_scan_fn("aligned_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AlignedScanFunction,
	                              AlignedBind, AlignedInitGlobal, AlignedInitLocal);
	aligned_scan_fn.cardinality = AlignedCardinality;
	aligned_scan_fn.projection_pushdown = true; // Phase 2
	aligned_scan_fn.filter_pushdown = true;
	aligned_scan_fn.filter_prune = true;
	loader.RegisterFunction(aligned_scan_fn);

	// Setting that supplies the default data root for aligned_table(name)
	auto &db = loader.GetDatabaseInstance();
	db.config.AddExtensionOption("aligned_data_root", "Root directory for AlignedTable logical tables",
	                             LogicalType::VARCHAR);

	// aligned_upsert(table_name, source_path, mapping, root=...)
	// v8: upsert rows by primary key (symbol, date) from a source parquet file.
	// An existing key is UPDATED (only the mapped columns are overwritten), a
	// new key is INSERTED at its sorted position. Returns
	// (rows_inserted, rows_updated, parts_rewritten, txid).
	TableFunction aligned_upsert_fn("aligned_upsert", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AlignedUpsertFunction,
	                                AlignedUpsertBind, MutateInitGlobal, nullptr);
	aligned_upsert_fn.varargs = LogicalType::VARCHAR; // mapping (3rd arg) is optional (auto-derived)
	aligned_upsert_fn.named_parameters["root"] = LogicalType::VARCHAR;
	loader.RegisterFunction(aligned_upsert_fn);

	// aligned_delete(table_name, keys_source, root=...)
	// v8: delete rows by primary key (symbol, date). Non-existent keys are
	// skipped (idempotent). Returns (rows_deleted, parts_rewritten, txid).
	TableFunction aligned_delete_fn("aligned_delete", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                AlignedDeleteFunction, AlignedDeleteBind, MutateInitGlobal, nullptr);
	aligned_delete_fn.named_parameters["root"] = LogicalType::VARCHAR;
	loader.RegisterFunction(aligned_delete_fn);

	// aligned_compact(table_name, group_name, root=...)
	// Phase 7: merge a group's parts per partition directory (atomic switch)
	TableFunction aligned_compact_fn("aligned_compact", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 AlignedCompactFunction, AlignedCompactBind, AlignedCompactInitGlobal, nullptr);
	aligned_compact_fn.named_parameters["root"] = LogicalType::VARCHAR;
	loader.RegisterFunction(aligned_compact_fn);

	// Phase 8: DuckLake-style storage extension. ATTACH '<root>' AS name
	// (TYPE ALIGNED) creates a logical catalog over the parquet column groups:
	// SELECT reads the parquet files directly (no materialization).
	RegisterAlignedStorageExtension(db);

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
