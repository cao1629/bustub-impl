//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->table_oid_);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  child_executor_->Init();

  auto *lock_mgr = exec_ctx_->GetLockManager();
  auto *txn = exec_ctx_->GetTransaction();

  try {
    bool is_locked = lock_mgr->LockTable(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Insert Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException &ex) {
    throw ExecutionException("Insert Executor Get Table Lock Failed");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // auto *lock_mgr = exec_ctx_->GetLockManager();
  // auto *txn = exec_ctx_->GetTransaction();

  if (done_) {
    return false;
  }

  int insert_count = 0;

  Tuple to_insert_tuple{};
  RID emit_rid{};

  // Both "to_insert_tuple" and "insert_rid" are out parameters.
  // If child_executor_ is a ValuesExecutor. Next() produces a tuple without rid.
  while (child_executor_->Next(&to_insert_tuple, &emit_rid)) {

    // Insert a tuple from child executor's Next() method
    // "rid" will be updated by InsertTuple() method.

    // I think we should lock the inserted row before InsertTuple()
    // Assume the current transaction is interrupted here, and another transaction
    // tries to read the just inserted tuple. In this case, the other transaction
    // read an uncommited write.
    table_info_->table_->InsertTuple(to_insert_tuple, rid, exec_ctx_->GetTransaction());

    // Insert the tuple into all indexes associated with the table.
    // Page latches are used to protecting indexes.
    for (const auto index_info : table_indexes_) {
      // key: index key
      // value: rid, already updated by InsertTuple() method
      auto index_key = to_insert_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
        index_info->index_->GetKeyAttrs());

      index_info->index_->InsertEntry(index_key, *rid, exec_ctx_->GetTransaction());
    }
    insert_count++;
  }

  std::vector<Value> values{};
  values.emplace_back(INTEGER, insert_count);
  *tuple = {values, &GetOutputSchema()};
  done_ = true;
  return true;
}

}  // namespace bustub
