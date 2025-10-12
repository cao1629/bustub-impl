//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iter_(aht_.Begin()) {}


void AggregationExecutor::Init() {
  child_->Init();

  // Build up the hash table
  Tuple tuple;
  RID rid;

  while (child_->Next(&tuple, &rid)) {
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
}

// What do we expect for Next()?
// a list of aggregate keys, a list of aggregate values
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iter_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values;

  const auto aggregate_keys = aht_iter_.Key();
  const auto aggregate_values  = aht_iter_.Val();

  values.insert(values.end(), aggregate_keys.group_bys_.begin(), aggregate_keys.group_bys_.end());
  values.insert(values.end(), aggregate_values.aggregates_.begin(), aggregate_values.aggregates_.end());

  // Create a rvalue first, and then assign it to *tuple.
  *tuple = Tuple{values, &GetOutputSchema()};

  ++aht_iter_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
