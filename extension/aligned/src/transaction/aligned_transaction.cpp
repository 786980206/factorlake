#include "transaction/aligned_transaction.hpp"

namespace duckdb {

AlignedTransaction::AlignedTransaction(TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

AlignedTransactionManager::AlignedTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
}

Transaction &AlignedTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<AlignedTransaction>(*this, context);
	auto &result = *transaction;
	transactions.push_back(std::move(transaction));
	return result;
}

ErrorData AlignedTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	for (idx_t i = 0; i < transactions.size(); i++) {
		if (transactions[i].get() == &transaction) {
			transactions.erase(transactions.begin() + i);
			break;
		}
	}
	return ErrorData();
}

void AlignedTransactionManager::RollbackTransaction(Transaction &transaction) {
	for (idx_t i = 0; i < transactions.size(); i++) {
		if (transactions[i].get() == &transaction) {
			transactions.erase(transactions.begin() + i);
			break;
		}
	}
}

void AlignedTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// no-op: aligned storage commits atomically via _tmp + rename (no MVCC).
	(void)context;
	(void)force;
}

} // namespace duckdb
