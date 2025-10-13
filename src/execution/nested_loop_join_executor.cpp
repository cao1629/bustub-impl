//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    // vector.push_back will invoke the copy constructor of Tuple.
    right_tuples_.push_back(tuple);
  }

  // -1 means we haven't started scanning the right tuples for the current left tuple.
  cursor_ = -1;

  matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;

  // If cursor_ >= 0, we are in the middle of scanning right tuples.
  // If cursor_ is -1, we are done scanning right tuples for the left tuple. We need to get a new left tuple.
  while (cursor_ >= 0 || !left_executor_->Next(&left_tuple, &left_rid)) {
    if (cursor_ < 0) {
      cursor_ = 0;
      matched_ = false;
    }
    std::vector<Value> vals;
    for (uint32_t idx = cursor_; idx < right_tuples_.size(); ++idx) {
      const auto &right_tuple = right_tuples_[idx];
      if (Matched(left_tuple, right_tuple)) {
        // Join two tuples
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          vals.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }

        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          vals.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }

        *tuple = {vals, &GetOutputSchema()};

        cursor_++;
        matched_ = true;
        return true;
      }

      // try to match the next right tuple
      cursor_++;
    }

    cursor_ = -1;

    // We have scanned all right tuples for the current left tuple.
    // Check if it is a left outer join.
    if (!matched_ && plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        vals.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }

      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        vals.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = {vals, &GetOutputSchema()};
      return true;
    }
  }

  return false;
}

auto NestedLoopJoinExecutor::Matched(const Tuple &left_tuple, const Tuple &right_tuple) const -> bool {
  auto val = plan_->Predicate().EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                             right_executor_->GetOutputSchema());
  return val.GetAs<bool>();
}

}  // namespace bustub
