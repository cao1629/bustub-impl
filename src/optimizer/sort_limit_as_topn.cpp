#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

// Limit plan has only one child. If that child is a Sort plan, we do the optimization.
// Similar to clone a tree, post-order traversal
auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule

  if (plan->GetType() == PlanType::Limit) {
    auto limit_plan = dynamic_cast<const LimitPlanNode *>(plan.get());
    if (limit_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
      auto sort_plan = dynamic_cast<const SortPlanNode *>(limit_plan->GetChildAt(0).get());
      return std::make_shared<TopNPlanNode>(limit_plan->output_schema_, sort_plan->GetChildAt(0),
                                            sort_plan->GetOrderBy(), limit_plan->GetLimit());
    }
  }

  std::vector<AbstractPlanNodeRef> children;

  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(children);

  return optimized_plan;
}

}  // namespace bustub
