#include "aligned_extension.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "scan/aligned_scan.hpp"

namespace duckdb {

void AlignedExtension::Load(ExtensionLoader &loader) {
	// aligned_table(table_name, root=...)
	TableFunction aligned_table_fn("aligned_table", {LogicalType::VARCHAR}, AlignedScanFunction, AlignedBind,
	                               AlignedInitGlobal, AlignedInitLocal);
	aligned_table_fn.named_parameters["root"] = LogicalType::VARCHAR;
	aligned_table_fn.cardinality = AlignedCardinality;
	loader.RegisterFunction(aligned_table_fn);

	// aligned_scan(root, table_name)
	TableFunction aligned_scan_fn("aligned_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AlignedScanFunction,
	                              AlignedBind, AlignedInitGlobal, AlignedInitLocal);
	aligned_scan_fn.cardinality = AlignedCardinality;
	loader.RegisterFunction(aligned_scan_fn);

	// Setting that supplies the default data root for aligned_table(name)
	auto &db = loader.GetDatabaseInstance();
	db.config.AddExtensionOption("aligned_data_root", "Root directory for AlignedTable logical tables",
	                             LogicalType::VARCHAR);
}

std::string AlignedExtension::Name() {
	return "aligned";
}

std::string AlignedExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb
