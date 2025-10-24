//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

#include "type/integer_type.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() {
  child_executor_->Init();

  auto *lock_mgr = exec_ctx_->GetLockManager();
  auto *txn = exec_ctx_->GetTransaction();
  try {
    bool is_locked = lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!is_locked) {
      throw ExecutionException("Delete Executor Get Table Lock Failed");
    }
  } catch (TransactionAbortException &ex) {
    throw ExecutionException("Delete Executor Get Table Lock Failed");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }

  Tuple to_delete_tuple{};
  RID emit_rid{};
  int delete_count = 0;
  auto *lock_mgr = exec_ctx_->GetLockManager();
  auto *txn = exec_ctx_->GetTransaction();

  while (child_executor_->Next(&to_delete_tuple, &emit_rid)) {
    try {
      bool is_locked = lock_mgr->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, emit_rid);
      if (!is_locked) {
        throw ExecutionException("Delete Executor Get Row Lock Failed");
      }
    } catch (TransactionAbortException &ex) {
      throw ExecutionException("Delete Executor Get Row Lock Failed");
    }

    bool deleted = table_info_->table_->MarkDelete(emit_rid, exec_ctx_->GetTransaction());

    if (deleted) {
      for (const auto index_info : table_indexes_) {
        auto index_key = to_delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                      index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(index_key, emit_rid, exec_ctx_->GetTransaction());
      }
    }

    delete_count++;
  }

  std::vector<Value> values{};
  values.emplace_back(INTEGER, delete_count);
  *tuple = {values, &GetOutputSchema()};
  done_ = true;

  return true;
}

}  // namespace bustub
