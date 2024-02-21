//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // Get the table to insert into
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_ = table_info_->table_.get();
  table_name_ = table_info_->name_;

  if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_)) {
    throw ExecutionException("LOCK TABLE EXCLUSIVE FAILED");
  }
  // Initialize the child executor
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // We return only once
  if (is_successful_) {
    return false;
  }
  RID rid_;
  Tuple tup;

  int num_inserted = 0;

  while (child_executor_->Next(&tup, &rid_)) {
    if (!table_->InsertTuple(tup, &rid_, this->GetExecutorContext()->GetTransaction())) {
      return false;
    }
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->table_oid_, rid_)) {
      throw ExecutionException("LOCK ROW EXCLUSIVE FAILED");
    }
    auto indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_name_);
    for (auto i : indexes) {
      auto key = tup.KeyFromTuple(table_info_->schema_, i->key_schema_, i->index_->GetKeyAttrs());
      i->index_->InsertEntry(key, rid_, this->GetExecutorContext()->GetTransaction());
    }

    num_inserted++;
  }

  // prepare schema of one column of type integer
  // Schema: "":(INTEGER)
  std::vector<Column> cols;
  Column col("", INTEGER);
  cols.push_back(col);
  Schema schema(cols);

  // value of only one tuple (num of tuples inserted)
  // Value: num_inserted
  std::vector<Value> values;
  Value val(INTEGER, num_inserted);
  values.push_back(val);

  Tuple t(values, &schema);
  *tuple = t;
  /*  RESULT TUPLE: Schema:         "":INTEGER
   *                TUPLE/ROW 0     num_inserted */

  is_successful_ = true;
  return true;
}

}  // namespace bustub
