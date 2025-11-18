#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), pq_(TupleComparator(plan)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  auto limit = plan_->GetN();
  Tuple tuple;
  RID rid;

  for (size_t i = 0; i < limit; i++) {
    if (!child_executor_->Next(&tuple, &rid)) {
      break;
    }
    pq_.push(tuple);
  }

  while (child_executor_->Next(&tuple, &rid)) {
    pq_.push(tuple);
    pq_.pop();
  }

}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (pq_.empty()) {
    return false;
  }

  *tuple = pq_.top();
  pq_.pop();
  return true;
}

}  // namespace bustub
