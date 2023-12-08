#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      comparator_(this),      // Pass this TopNExecutor instance to the TupleComparator constructor
      topNHeap_(comparator_)  // Pass the TupleComparator instance to the priority_queue constructor
{}

void TopNExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  // Process each tuple from the child executor
  while (child_executor_->Next(&tuple, &rid)) {
    // Add the tuple to the top-N heap
    this->topNHeap_.push(tuple);

    // Ensure the heap size doesn't exceed N
    while (topNHeap_.size() > plan_->GetN()) {
      this->topNHeap_.pop();
    }
  }
  while (!(this->topNHeap_.empty())) {
    sorted_tuples_.push_back(topNHeap_.top());
    this->topNHeap_.pop();
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
