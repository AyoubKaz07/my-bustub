//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(std::move(left_child)), right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() { 
  right_child_->Init();
  left_child_->Init();
  Tuple left_tuple;
  RID left_rid;
  // Build hash table
  while (left_child_->Next(&left_tuple, &left_rid)) {
    // Get the value of the left key
    auto join_key = plan_->LeftJoinKeyExpression().Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
    // Insert into hash table
    hash_table_[HashUtil::HashValue(&join_key)].push_back(left_tuple);
  }
  Tuple right_tuple;
  RID right_rid;
  while (right_child_->Next(&right_tuple, &right_rid)) {
    auto right_key = plan_->RightJoinKeyExpression().Evaluate(&right_tuple, plan_->GetRightPlan()->OutputSchema());
    if (hash_table_.count(HashUtil::HashValue(&right_key)) > 0) {
      auto left_tuples = hash_table_[HashUtil::HashValue(&right_key)];
      for (const auto &tuple : left_tuples) {
        auto left_join_key = plan_->LeftJoinKeyExpression().Evaluate(&tuple, plan_->GetLeftPlan()->OutputSchema());
        if (left_join_key.CompareEquals(right_key) == CmpBool::CmpTrue) {
          std::vector<Value> values{};
          for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
            values.push_back(tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
          }
          for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
            values.push_back(right_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
          }
          output_tuples_.emplace_back(values, &GetOutputSchema());
        }
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
          values.push_back(left_tuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
        }
        for (uint32_t i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
        }
        output_tuples_.emplace_back(values, &GetOutputSchema());
    }
  }
  output_iterator_ = output_tuples_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_iterator_ == output_tuples_.end()) {
    return false;
  }
  *tuple = *output_iterator_;
  output_iterator_++;
  return true;
}

}  // namespace bustub
