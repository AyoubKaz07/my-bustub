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
      right_executor_(std::move(right_executor)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()) {
  if (plan->GetJoinType() == JoinType::INNER) {
    inner_join_ = true;
  }
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    throw NotImplementedException("Only LEFT and INNER joins are supported");
  }
}

void NestedLoopJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  right_executor_->Init();

  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Value cmp(BOOLEAN, static_cast<int8_t>(1));
  // Prepare Schema for output
  std::vector<Column> values(left_schema_.GetColumns());
  values.insert(values.end(), right_schema_.GetColumns().begin(), right_schema_.GetColumns().end());
  Schema output_schema(values);

  if (inner_join_) {
    if (index_ > right_tuples_.size()) {
      return false;
    }
    if (index_ != 0) {
      for (auto j = index_; j < right_tuples_.size(); j++) {
        index_ = (index_ + 1) % right_tuples_.size();
        // Tuples match
        if (plan_->Predicate()
                .EvaluateJoin(&left_tuple, left_schema_, &right_tuples_[j], right_schema_)
                .CompareEquals(cmp) == CmpBool::CmpTrue) {
          std::vector<Value> tuple_values;
          for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
            tuple_values.push_back(left_tuple.GetValue(&left_schema_, i));
          }
          for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
            tuple_values.push_back(right_tuples_[j].GetValue(&right_schema_, i));
          }

          *tuple = Tuple(tuple_values, &output_schema);
          return true;
        }
      }
    }
    if (index_ == 0) {
      while (left_executor_->Next(&left_tuple, &left_rid)) {
        for (const auto &right_tuple : right_tuples_) {
          index_ = (index_ + 1) % right_tuples_.size();
          // Tuples match
          if (plan_->Predicate()
                  .EvaluateJoin(&left_tuple, left_schema_, &right_tuple, right_schema_)
                  .CompareEquals(cmp) == CmpBool::CmpTrue) {
            std::vector<Value> tuple_values;
            for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
              tuple_values.push_back(left_tuple.GetValue(&left_schema_, i));
            }
            for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
              tuple_values.push_back(right_tuple.GetValue(&right_schema_, i));
            }

            *tuple = Tuple(tuple_values, &output_schema);
            return true;
          }
        }
      }
      index_ = right_tuples_.size() + 1;
    }
    return false;
  }

  if (index_ > right_tuples_.size()) {
    return false;
  }

  // CASE OF LEFT JOIN
  if (index_ != 0) {
    for (auto j = index_; j < right_tuples_.size(); j++) {
      index_ = (index_ + 1) % right_tuples_.size();
      // Tuples match
      if (plan_->Predicate()
              .EvaluateJoin(&left_tuple, left_schema_, &right_tuples_[j], right_schema_)
              .CompareEquals(cmp) == CmpBool::CmpTrue) {
        std::vector<Value> tuple_values;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          tuple_values.push_back(left_tuple.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          tuple_values.push_back(right_tuples_[j].GetValue(&right_schema_, i));
        }
        found_match = true;
        *tuple = Tuple(tuple_values, &output_schema);
        return true;
      }
    }
  }
  if (index_ == 0) {
    if (!found_match) {
      // No match, output left tuple with nulls
      std::vector<Value> tuple_values;
      for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
        tuple_values.push_back(left_tuple.GetValue(&left_schema_, i));
      }
      for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
        tuple_values.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
      }
      found_match = true;
      *tuple = Tuple(tuple_values, &output_schema);
      return true;
    }
    while (left_executor_->Next(&left_tuple, &left_rid)) {
      found_match = false;
      for (const auto &right_tuple : right_tuples_) {
        index_ = (index_ + 1) % right_tuples_.size();
        // Tuples match
        if (plan_->Predicate()
                .EvaluateJoin(&left_tuple, left_schema_, &right_tuple, right_schema_)
                .CompareEquals(cmp) == CmpBool::CmpTrue) {
          std::vector<Value> tuple_values;
          for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
            tuple_values.push_back(left_tuple.GetValue(&left_schema_, i));
          }
          for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
            tuple_values.push_back(right_tuple.GetValue(&right_schema_, i));
          }
          found_match = true;
          *tuple = Tuple(tuple_values, &output_schema);
          return true;
        }
      }
      if (!found_match) {
        // No match, output left tuple with nulls
        std::vector<Value> tuple_values;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          tuple_values.push_back(left_tuple.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          tuple_values.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
        }
        found_match = true;
        *tuple = Tuple(tuple_values, &output_schema);
        return true;
      }
    }
    index_ = right_tuples_.size() + 1;
  }
  return false;
}

}  // namespace bustub
