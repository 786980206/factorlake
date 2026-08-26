#pragma once

#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

//! Minimal transaction for the aligned attached database. The aligned storage
//! has no MVCC of its own — COPY/compaction commit atomically via _tmp + rename,
//! so transactions are effectively no-ops that only carry the bookkeeping.
class AlignedTransaction : public Transaction {
public:
	AlignedTransaction(TransactionManager &manager, ClientContext &context);
};

class AlignedTransactionManager : public TransactionManager {
public:
	explicit AlignedTransactionManager(AttachedDatabase &db);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	vector<unique_ptr<Transaction>> transactions;
};

} // namespace duckdb
