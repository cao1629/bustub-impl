//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void SeqScanExecutor::Init() {
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  auto *lock_mgr = exec_ctx_->GetLockManager();
  auto *txn = exec_ctx_->GetTransaction();


  // with READ COMMITED, we don't need S, IS.
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException &ex) {
      throw ExecutionException("SeqScan Executor Get Table Lock Failed");
    }
  }
}

// After the first time we get false from Next(), we will never call Next() again.
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto *lock_mgr = exec_ctx_->GetLockManager();
  auto *txn = exec_ctx_->GetTransaction();

  do {
    if (table_iter_ == table_info_->table_->End()) {
      // READ COMMITED: we need to release locks when we're done with this SeqScanExecutor.
      try {
        if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
          const auto locked_row_set = txn->GetSharedRowLockSet()->at(table_info_->oid_);
          for (const auto locked_rid : locked_row_set) {
            lock_mgr->UnlockRow(txn, table_info_->oid_, locked_rid);
          }

          lock_mgr->UnlockTable(txn, table_info_->oid_);
        }
      } catch (TransactionAbortException &ex) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
      return false;
    }
    *tuple = *table_iter_;
    ++table_iter_;
    *rid = tuple->GetRid();

  } while (plan_->filter_predicate_ != nullptr &&
           !plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_).GetAs<bool>());

  // Now we get the tuple successfully.
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = lock_mgr->LockRow(txn, LockManager::LockMode::SHARED, plan_->table_oid_, *rid);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor Get Row Lock Failed");
      }

    } catch (TransactionAbortException &ex) {
      throw ExecutionException("SeqScan Executor Get Row Lock Failed");
    }
  }

  return true;
}

}  // namespace bustub
