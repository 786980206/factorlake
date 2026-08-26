#include "catalog/write_lock.hpp"

namespace duckdb {

static std::atomic<idx_t> g_transaction_id {1};

idx_t NextTransactionId() {
	return g_transaction_id.fetch_add(1);
}

} // namespace duckdb
