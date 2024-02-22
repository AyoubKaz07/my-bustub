//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // Get the table to delete from
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_ = table_info_->table_.get();
  table_name_ = table_info_->name_;

  child_executor_->Init();
  // Initialize the child executor
  if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_)) {
    throw ExecutionException("LOCK TABLE EXCLUSIVE FAILED");
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // We return only once
  if (is_successful_) {
    return false;
  }
  RID rid_;
  Tuple tup;

  int num_deleted = 0;

  while (child_executor_->Next(&tup, &rid_)) {
    if (!table_->MarkDelete(rid_, this->GetExecutorContext()->GetTransaction())) {
      return false;
    }
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, rid_)) {
      throw ExecutionException("LOCK ROW EXCLUSIVE FAILED");
    }
    auto indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_name_);
    for (auto i : indexes) {
      auto key = tup.KeyFromTuple(table_info_->schema_, i->key_schema_, i->index_->GetKeyAttrs());
      i->index_->DeleteEntry(key, rid_, this->GetExecutorContext()->GetTransaction());
    }
    num_deleted++;
  }

  // prepare schema of one column of type integer
  // Schema: "":(INTEGER)
  std::vector<Column> cols;
  Column col("", INTEGER);
  cols.push_back(col);
  Schema schema(cols);

  // value of only one tuple (num of tuples deleted)
  Value val(INTEGER, num_deleted);
  std::vector<Value> values;
  values.push_back(val);
  *tuple = Tuple(values, &schema);

  /* RESULT TUPLE: Schema:        "":INTEGER
   *               TUPLE/ROW 0    num_deleted
   */

  is_successful_ = true;
  return true;
}

}  // namespace bustub
