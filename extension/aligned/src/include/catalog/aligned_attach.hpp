#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {

//! aligned_attach(root=...): register every logical table under the data root
//! as a real DuckDB catalog table so standard SQL (SELECT / INSERT / UPDATE /
//! DELETE) works against the bare table name.
TableFunctionSet CreateAlignedAttachFunctions();

//! aligned_detach(root=...): drop the catalog tables created by aligned_attach.
TableFunctionSet CreateAlignedDetachFunctions();

//! aligned_detach(root=...): drop the catalog tables created by aligned_attach.
TableFunctionSet CreateAlignedDetachFunctions();

} // namespace duckdb
