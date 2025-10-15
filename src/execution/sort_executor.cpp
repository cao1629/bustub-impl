#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}


void SortExecutor::Init() {
  // Load all tuples from child executor and sort them out.
  child_executor_->Init();

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    inmem_tuples_.push_back(tuple);
  }

  // Define comparison function for sorting
  auto cmp = [this](const Tuple &a, const Tuple &b) -> bool {
    for (const auto &[order_type, expr] : plan_->GetOrderBy()) {
      // expr is ColumnValueExpression
      Value val_a = expr->Evaluate(&a, plan_->OutputSchema());
      Value val_b = expr->Evaluate(&b, plan_->OutputSchema());

      if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
        continue;  // Values are equal, check next order by column
      }

      // Determine sort order (ASC or DESC)
      bool ascending = (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT);

      if (ascending) {
        return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
      } else {
        return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
      }
    }
    return false;  // All values are equal
  };

  // Sort tuples using the comparison function
  std::sort(inmem_tuples_.begin(), inmem_tuples_.end(), cmp);

  cursor_ = inmem_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == inmem_tuples_.end()) {
    return false;
  }

  *tuple = *cursor_;
  ++cursor_;
  return true;
}

}  // namespace bustub
