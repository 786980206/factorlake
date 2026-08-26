#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! Validates that a group name is either "index" or a two-level path
//! "lv1/lv2" (exactly one slash, neither segment empty). Throws
//! BinderException on invalid names.
inline void ValidateGroupName(const string &group_name) {
	if (StringUtil::CIEquals(group_name, "index")) {
		return;
	}
	auto slash = group_name.find('/');
	if (slash == string::npos || group_name.find('/', slash + 1) != string::npos ||
	    slash == 0 || slash + 1 >= group_name.size()) {
		throw BinderException("aligned: group name '%s' must be 'index' or a "
		                       "two-level path 'lv1/lv2' (e.g. 'factor/alpha101')", group_name);
	}
}

//! Creates an AlignedTable on disk: creates the table directory, the column
//! group subdirectories, and writes one empty (0-row) placeholder parquet file
//! per group so that the reader can discover the schema from the footer.
//! For table extension (existing table), writes N-row all-NULL placeholders
//! (N = index partition row count) to satisfy partition alignment.
//!
//! Syntax:
//!   CREATE TABLE al.<table> (col0 <type>, col1 <type>, ...) WITH (
//!     groups 'index:col2,col3;factor/alpha:col4',
//!     partition_template 'date=%Y-%m-%d'
//!   );
//!
//! Rules:
//!   - The first two columns must be (symbol VARCHAR, date DATE/TIMESTAMP) —
//!     the primary key contract (col0=symbol, col1=date).
//!   - `groups` maps columns to column groups (format "group:col1,col2;...").
//!     Columns not listed in any group default to the index group.
//!   - `partition_template` defaults to "month=%Y-%m".
//!   - A placeholder parquet (0 rows) is written per group under
//!     <table>/<group>/<partition_key>/0000-0000000000.parquet.
void AlignedCreateTable(ClientContext &context, const string &root, const string &table_name,
                        const vector<ColumnDefinition> &columns,
                        const string &groups_option, const string &partition_template_option);

//! Creates a new empty partition directory on an existing AlignedTable. For
//! each discovered column group, writes a 0-row placeholder parquet under
//! <table>/<group>/<partition_key>/0000-0000000000.parquet.
//!
//! Syntax:
//!   CREATE TABLE al.<table> (PARTITION 'date=2026-09-01');
//!
//! Rules:
//!   - The table must already exist (discovered via glob).
//!   - The partition key format must match the existing partition template
//!     (e.g. "date=2026-09-01" for date=%Y-%m-%d, "month=2026-09" for month=%Y-%m).
void AlignedCreatePartition(ClientContext &context, const string &root, const string &table_name,
                             const string &partition_key);

} // namespace duckdb
