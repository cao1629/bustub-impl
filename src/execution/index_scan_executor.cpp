//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

#include <execution/expressions/constant_value_expression.h>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  index_info_ = this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  table_info_ = this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  if (plan_->filter_predicate_ == nullptr) {
    iter_ = index_->GetBeginIterator();
  }
}

void IndexScanExecutor::Init() {
  // where colA = 10; We need to set the iterator to 10 from the beginning
  if (plan_->filter_predicate_ != nullptr) {
    auto right_expr = dynamic_cast<ConstantValueExpression*>(plan_->filter_predicate_->children_[1].get());
    Value v = right_expr->val_;
    index_->ScanKey(Tuple{{v}, &index_info_->key_schema_}, &results_, exec_ctx_->GetTransaction());
    results_iter_ = results_.begin();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // where colA = 10
  if (plan_->filter_predicate_ != nullptr) {
    if (results_iter_ == results_.end()) {
      return false;
    }
    *rid = *results_iter_;
    // We have a RID, but we might not able to get the tuple.
    bool found_tuple = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    ++results_iter_;
    return found_tuple;
  }

  // order by
  if (iter_ == index_->GetEndIterator()) {
    return false;
  }

  *rid = (*iter_).second;
  bool found_tuple = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iter_;
  return found_tuple;
}
}  // namespace bustub