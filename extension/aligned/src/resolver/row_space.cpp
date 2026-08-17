#include "resolver/row_space.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void ValidateRowSpace(const string &table_name, const string &group_name, idx_t row_count,
                      const vector<PartInfo> &parts) {
	if (row_count == 0) {
		if (!parts.empty()) {
			throw IOException("Aligned table '%s' group '%s': table has 0 rows but %llu part(s) exist",
			                  table_name, group_name, parts.size());
		}
		return;
	}
	if (parts.empty()) {
		throw IOException("Aligned table '%s' group '%s': group has no parts but table declares %llu rows",
		                  table_name, group_name, row_count);
	}
	if (parts[0].start_row != 0) {
		throw IOException("Aligned table '%s' group '%s': first part starts at row %llu instead of 0 (alignment "
		                  "violation)",
		                  table_name, group_name, parts[0].start_row);
	}
	idx_t expected = 0;
	for (auto &part : parts) {
		if (part.row_count == 0) {
			continue;
		}
		if (part.start_row != expected) {
			throw IOException("Aligned table '%s' group '%s': part '%s' starts at row %llu but expected %llu "
			                  "(gap or overlap, alignment violation)",
			                  table_name, group_name, part.part_name, part.start_row, expected);
		}
		if (part.start_row > row_count || part.row_count > row_count - part.start_row) {
			throw IOException("Aligned table '%s' group '%s': part '%s' extends past the table end (%llu + %llu > %llu, "
			                  "alignment violation)",
			                  table_name, group_name, part.part_name, part.start_row, part.row_count, row_count);
		}
		expected += part.row_count;
	}
	if (expected != row_count) {
		throw IOException("Aligned table '%s' group '%s': parts cover [0, %llu) but table declares %llu rows "
		                  "(alignment violation)",
		                  table_name, group_name, expected, row_count);
	}
}

} // namespace duckdb
