#include "execution/executors/topn_executor.h"
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // Comparator for the top-N heap
  auto comparator = [this](const Tuple &t1, const Tuple &t2) -> bool {
    for (const auto &[order_by_type, expr] : plan_->GetOrderBy()) {
      if (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT) {
        if (expr->Evaluate(&t1, child_executor_->GetOutputSchema())
                .CompareLessThan(expr->Evaluate(&t2, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        } else if (expr->Evaluate(&t1, child_executor_->GetOutputSchema())
                       .CompareGreaterThan(expr->Evaluate(&t2, child_executor_->GetOutputSchema())) ==
                   CmpBool::CmpTrue) {
          return false;
        }
      }
      if (order_by_type == OrderByType::DESC) {
        if (expr->Evaluate(&t1, child_executor_->GetOutputSchema())
                .CompareLessThan(expr->Evaluate(&t2, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        } else if (expr->Evaluate(&t1, child_executor_->GetOutputSchema())
                       .CompareGreaterThan(expr->Evaluate(&t2, child_executor_->GetOutputSchema())) ==
                   CmpBool::CmpTrue) {
          return true;
        }
      }
    }
    return false;
  };
  
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comparator)> topNHeap_(comparator);

  Tuple tuple;
  RID rid;
  child_executor_->Init();
  // Process each tuple from the child executor
  while (child_executor_->Next(&tuple, &rid)) {
    // Add the tuple to the top-N heap
    topNHeap_.push(tuple);

    // Ensure the heap size doesn't exceed N
    while (topNHeap_.size() > plan_->GetN()) {
      topNHeap_.pop();
    }
  }
  while (!(topNHeap_.empty())) {
    sorted_tuples_.push_back(topNHeap_.top());
    topNHeap_.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_tuples_.empty()) {
    return false;
  }
  *tuple = sorted_tuples_.back();
  sorted_tuples_.pop_back();
  return true;
}

}  // namespace bustub
