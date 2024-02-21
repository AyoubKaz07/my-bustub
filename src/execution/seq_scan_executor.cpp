//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // .get() returns a raw pointer to the managed object (the table_)
  table_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = std::make_unique<TableIterator>(table_->Begin(exec_ctx_->GetTransaction()));
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED 
  && !exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid())) {
    throw ExecutionException("LOCK TABLE SHARED FAILED");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!exec_ctx_->GetTransaction()->GetSharedRowLockSet()->empty()
  && exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED 
  && !exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), *rid)) {
    throw ExecutionException("UNLOCK ROW FAILED");
  }
  if (*iter_ == table_->End()) {
    return false;
  }
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED 
  && !exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->GetTableOid(), (*(*iter_)).GetRid())) {
    throw ExecutionException("LOCK ROW SHARED FAILED");
  }
  *tuple = *(*iter_);
  *rid = (*iter_)++->GetRid();
  return true;
}

}  // namespace bustub
