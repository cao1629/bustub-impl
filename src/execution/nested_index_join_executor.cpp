//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

#include "type/value_factory.h"

namespace bustub {

NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetInnerTableOid())),

      // Why we can use reinterpret_cast here?
      // Because what we get from index_info_->index_.get() is a pointer to
      // BPlusTreeIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>;
      index_(reinterpret_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestedIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  std::vector<Value> vals;

  while (child_executor_->Next(&left_tuple, &left_rid)) {
    // tuple -> index key
    Value val = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());

    std::vector<RID> rids;
    index_->ScanKey(Tuple{{val}, index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());

    if (!rids.empty()) {
      Tuple right_tuple;
      table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());

      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        vals.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }

      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        vals.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), i));
      }

      *tuple = {vals, &GetOutputSchema()};
      return true;
    }
    // try left outer join
    if (plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        vals.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }

      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        vals.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
      }

      *tuple = {vals, &GetOutputSchema()};
      return true;
    }
  }

  return false;
}

}  // namespace bustub
