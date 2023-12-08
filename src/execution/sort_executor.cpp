#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;

  // Get all the tuples from the child executor
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }
  auto comparator = [this](const Tuple &t1, const Tuple &t2) {
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
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), comparator);
  iter_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == sorted_tuples_.end()) {
    return false;
  }
  *tuple = *iter_;
  iter_++;
  return true;
}

}  // namespace bustub
