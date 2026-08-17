#include "aligned_extension.hpp"

#include "compaction/aligned_compactor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "scan/aligned_scan.hpp"
#include "writer/aligned_writer.hpp"

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
	// into TableFunctionInitInput::filters; filter_prune additionally pushes
	// filters on columns that are filtered but not projected (e.g. a partition
	// source column like `date` when only factor columns are selected), so the
	// leaf can prune its kept parts and the aligned=true intersection shrinks the
	// global scan range.
	aligned_table_fn.filter_pushdown = true;
	// NOTE: filter_prune = false on purpose. With filter_prune=true DuckDB
	// pushes filters on columns that are filtered but NOT projected; our scan
	// currently reads only projected columns, so it cannot apply a row filter on
	// a pruned (never-read) column. Leave filter_prune off so filter-only columns
	// are still projected through the function (DuckDB then filters after the
	// scan). Pruning still works when the partition source column is projected
	// (the common `SELECT ... date ... WHERE date=...` shape).
	aligned_table_fn.filter_prune = false;
	loader.RegisterFunction(aligned_table_fn);

	// aligned_scan(root, table_name)
	TableFunction aligned_scan_fn("aligned_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AlignedScanFunction,
	                              AlignedBind, AlignedInitGlobal, AlignedInitLocal);
	aligned_scan_fn.cardinality = AlignedCardinality;
	aligned_scan_fn.projection_pushdown = true; // Phase 2
	aligned_scan_fn.filter_pushdown = true;
	aligned_scan_fn.filter_prune = false;
	loader.RegisterFunction(aligned_scan_fn);

	// Setting that supplies the default data root for aligned_table(name)
	auto &db = loader.GetDatabaseInstance();
	db.config.AddExtensionOption("aligned_data_root", "Root directory for AlignedTable logical tables",
	                             LogicalType::VARCHAR);

	// aligned_write(table_name, source_path, mapping, root=..., start_row=...)
	// Phase 5: append rows to an AlignedTable from a source parquet file
	TableFunction aligned_write_fn("aligned_write", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               AlignedWriteFunction, AlignedWriteBind, AlignedWriteInitGlobal, nullptr);
	aligned_write_fn.named_parameters["root"] = LogicalType::VARCHAR;
	aligned_write_fn.named_parameters["start_row"] = LogicalType::UBIGINT;
	loader.RegisterFunction(aligned_write_fn);

	// aligned_compact(table_name, group_name, root=...)
	// Phase 7: merge a group's parts per partition directory (atomic switch)
	TableFunction aligned_compact_fn("aligned_compact", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 AlignedCompactFunction, AlignedCompactBind, AlignedCompactInitGlobal, nullptr);
	aligned_compact_fn.named_parameters["root"] = LogicalType::VARCHAR;
	loader.RegisterFunction(aligned_compact_fn);

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
