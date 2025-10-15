//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  // Comparator for the priority queue
  class TupleComparator {
   public:
    explicit TupleComparator(const TopNPlanNode *plan) : plan_(plan) {}

    auto operator()(const Tuple &a, const Tuple &b) const -> bool {
      for (const auto &[order_type, expr] : plan_->GetOrderBy()) {
        Value val_a = expr->Evaluate(&a, plan_->OutputSchema());
        Value val_b = expr->Evaluate(&b, plan_->OutputSchema());

        if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
          continue;
        }

        // For TopN, we use a max-heap for ASC (to keep smallest N elements)
        // and a min-heap for DESC (to keep largest N elements)
        bool ascending = (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT);

        if (ascending) {
          // Max-heap
          return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
        } else {
          // Min-heap
          return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
        }
      }
      return false;
    }

   private:
    const TopNPlanNode *plan_;
  };

  const TopNPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> child_executor_;

  std::priority_queue<Tuple, std::vector<Tuple>, TupleComparator> pq_;

};
}  // namespace bustub
