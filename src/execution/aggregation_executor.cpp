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
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // BUILD PHASE
  child_->Init();
  Tuple tuple_;
  RID rid_;

  while (child_->Next(&tuple_, &rid_)) {
    // make the group bys from the group by expressions (the key)
    AggregateKey key = MakeAggregateKey(&tuple_);
    // get the aggregate values from the aggregate expressions (the value) out of the tuple
    AggregateValue value = MakeAggregateValue(&tuple_);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // ITERATE PHASE
  Schema schema(*plan_->output_schema_);
  if (aht_iterator_ != aht_.End()) {
    // get the group bys from the key
    std::vector<Value> values(aht_iterator_.Key().group_bys_);
    // for each aggregate, push the aggregate value into the values vector to make the tuple
    for (auto &agg : aht_iterator_.Val().aggregates_) {
      values.push_back(agg);
    }
    *tuple = Tuple(values, &schema);
    ++aht_iterator_;
    is_successful = true;
    return true;
  }
  // EMPTY TABLE
  if (!is_successful) {
    is_successful = true;
    if (plan_->GetGroupBys().empty()) {
      std::vector<Value> values(aht_.GenerateInitialAggregateValue().aggregates_);
      *tuple = Tuple(values, &schema);
      return true;
    }
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
