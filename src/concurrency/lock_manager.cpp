//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include <set>

namespace bustub {

auto LockManager::DeleteTableLockFromTxn(Transaction* txn, LockMode lock_mode, table_oid_t oid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  }
  else if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  }
  else if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
  else if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  }
  else if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  }
  return;
}

auto LockManager::AddTableLockToTxn(Transaction* txn, LockMode lock_mode, table_oid_t oid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  }
  else if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  }
  else if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
  else if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  }
  else if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  }
  return;
}

auto LockManager::DeleteRowLockFromTxn(Transaction* txn, LockMode lock_mode, table_oid_t oid, RID rid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
    if ((*txn->GetSharedRowLockSet())[oid].empty()) {
      (*txn->GetSharedRowLockSet()).erase(oid);
    }
    // WARNING, TABLE OID ENTRY SOMEHOW STILL EXISTS IN SHARED ROW LOCK SET
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
    if ((*txn->GetExclusiveRowLockSet())[oid].empty()) {
      (*txn->GetExclusiveRowLockSet()).erase(oid);
    }
    return;
  }
}

auto LockManager::AddRowLockToTxn(Transaction* txn, LockMode lock_mode, table_oid_t oid, RID rid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
    return;
  }
}

auto LockManager::Compatible(std::set<LockMode> granted_locks_set, LockMode lock_mode) -> bool {
  if (lock_mode == LockManager::LockMode::SHARED) {
    return granted_locks_set.find(LockManager::LockMode::EXCLUSIVE) == granted_locks_set.end()
      && granted_locks_set.find(LockManager::LockMode::INTENTION_EXCLUSIVE) == granted_locks_set.end()
      && granted_locks_set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == granted_locks_set.end();
  }
  if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return granted_locks_set.find(LockManager::LockMode::EXCLUSIVE) == granted_locks_set.end()
      && granted_locks_set.find(LockManager::LockMode::INTENTION_EXCLUSIVE) == granted_locks_set.end()
      && granted_locks_set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == granted_locks_set.end()
      && granted_locks_set.find(LockManager::LockMode::SHARED) == granted_locks_set.end();
  }
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    return granted_locks_set.find(LockManager::LockMode::EXCLUSIVE) == granted_locks_set.end()
      && granted_locks_set.find(LockManager::LockMode::SHARED) == granted_locks_set.end()
      && granted_locks_set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == granted_locks_set.end();
  }
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    return granted_locks_set.find(LockManager::LockMode::EXCLUSIVE) == granted_locks_set.end();
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    return granted_locks_set.empty();
  }

  return false;
}

auto LockManager::LockRequestQueue::GrantTableLockRequest(Transaction* txn, LockRequest* lock_request) -> bool {
  // std::set <LockMode> waiting_locks_set;
  std::set<LockMode> granted_locks_set;
  for (auto iter : request_queue_) {
    if (iter->granted_) {
      granted_locks_set.insert(iter->lock_mode_);
    }
  }
  if (Compatible(granted_locks_set, lock_request->lock_mode_)) {
    LOG_DEBUG("GRANTING TABLE LOCK");
    if (upgrading_ != INVALID_TXN_ID && upgrading_ == txn->GetTransactionId()) {
      upgrading_ = INVALID_TXN_ID;
    }
    lock_request->granted_ = true;
    AddTableLockToTxn(txn, lock_request->lock_mode_, lock_request->oid_);
    return true;
  }
  return false;
}

auto LockManager::LockRequestQueue::GrantRowLockRequest(Transaction* txn, LockRequest* lock_request) -> bool {
  // std::set <LockMode> waiting_locks_set;
  std::set<LockMode> granted_locks_set;
  for (auto iter : request_queue_) {
    if (iter->granted_) {
      granted_locks_set.insert(iter->lock_mode_);
    }
  }
  if (Compatible(granted_locks_set, lock_request->lock_mode_)) {
    LOG_DEBUG("GRANTING ROW LOCK");
    if (upgrading_ != INVALID_TXN_ID && upgrading_ == txn->GetTransactionId()) {
      upgrading_ = INVALID_TXN_ID;
    }
    lock_request->granted_ = true;
    AddRowLockToTxn(txn, lock_request->lock_mode_, lock_request->oid_, lock_request->rid_);
    return true;
  }
  return false;
}

auto LockManager::LockRequestQueue::CheckTableLock(Transaction* txn, LockManager::LockMode lock_mode) -> bool {
  for (auto iter : request_queue_) {
    if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
      if (lock_mode == LockManager::LockMode::SHARED) {
        if (iter->lock_mode_ != LockManager::LockMode::INTENTION_SHARED && iter->lock_mode_ != LockManager::LockMode::INTENTION_EXCLUSIVE 
        && iter->lock_mode_ != LockManager::LockMode::SHARED && iter->lock_mode_ != LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
          return false;
        }
      }
      else if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
        if (iter->lock_mode_ != LockManager::LockMode::INTENTION_EXCLUSIVE && iter->lock_mode_ != LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE
        && iter->lock_mode_ != LockManager::LockMode::EXCLUSIVE) {
          return false;
        }
      }
      return true;
    }
  }
  return false;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::cout << "Transaction ID: " << txn->GetTransactionId() << std::endl;
  std::cout << "Transaction State: " << int(txn->GetState()) << std::endl;
  std::cout << "Isolation Level: " << int(txn->GetIsolationLevel()) << std::endl;
  std::cout << "Locking table: " << oid << std::endl;
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockManager::LockMode::INTENTION_SHARED || lock_mode != LockManager::LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE || lock_mode == LockManager::LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  else {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode != LockManager::LockMode::INTENTION_EXCLUSIVE && lock_mode != LockManager::LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock_queue_latch(lock_queue->latch_);

  // Upgrading lock
  for (auto iter : lock_queue->request_queue_) {
    if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
      if (iter->lock_mode_ == lock_mode) {
        return true;
      }
      if (lock_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (iter->lock_mode_ == LockManager::LockMode::SHARED && (lock_mode != LockManager::LockMode::EXCLUSIVE && lock_mode != LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      if (iter->lock_mode_ == LockManager::LockMode::INTENTION_EXCLUSIVE && (lock_mode != LockManager::LockMode::EXCLUSIVE && lock_mode != LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      if (iter->lock_mode_ == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockManager::LockMode::EXCLUSIVE ) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // NO UPGRADE throw exception
      if (iter->lock_mode_ == LockManager::LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // Upgrade (not granted yet)
      lock_queue->request_queue_.remove(iter);
      DeleteTableLockFromTxn(txn, iter->lock_mode_, oid);
      delete iter;
      lock_queue->upgrading_ = txn->GetTransactionId();
      break;
    }
  }
  LockRequest* lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  lock_queue->request_queue_.push_back(lock_request);

  // Grant lock
  while (!lock_queue->GrantTableLockRequest(txn, lock_request)) {
    lock_queue->cv_.wait(lock_queue_latch);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_queue->request_queue_.remove(lock_request);
      if (lock_queue->upgrading_ == txn->GetTransactionId()) {
        LOG_DEBUG("UPGRADED TABLE LOCK");
        lock_queue->upgrading_ = INVALID_TXN_ID;
      }
      lock_queue->cv_.notify_all();
      delete lock_request;
      return false;
    }
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::cout << "Transaction ID: " << txn->GetTransactionId() << std::endl;
  std::cout << "Transaction State: " << int(txn->GetState()) << std::endl;
  std::cout << "Isolation Level: " << int(txn->GetIsolationLevel()) << std::endl;
  std::cout << "Unlocking table: " << oid << std::endl;

  // Ensure that txn holds no row locks on oid
  // WARNING: table oid entry might still exists, but row set has to be empty
  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock_queue_latch(lock_queue->latch_);

  bool lock_held = false;
  for (auto iter : lock_queue->request_queue_) {
    if (txn->GetTransactionId() == iter->txn_id_ && iter->granted_) {
      lock_held = true;
      if ((txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && (iter->lock_mode_ == LockMode::EXCLUSIVE || iter->lock_mode_ == LockMode::SHARED)) 
        || (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE)) 
        || (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE))) {
        txn->SetState(TransactionState::SHRINKING);
      }
      DeleteTableLockFromTxn(txn, iter->lock_mode_, oid);
      lock_queue->request_queue_.remove(iter);
      delete iter;
      break;
    }
  }
  if (lock_held) {
    lock_queue->cv_.notify_all();
    lock_queue_latch.unlock();

  } else {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return false;
  }
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockManager::LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }
  else {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode != LockManager::LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_queue_table = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  
  lock_queue_table->latch_.lock();
  // Require table lock 
  if (!lock_queue_table->CheckTableLock(txn, lock_mode)) {
    lock_queue_table->latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  lock_queue_table->latch_.unlock();

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>(); 
  }
  auto lock_queue_row = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock_queue_latch(lock_queue_row->latch_);

  // Upgrading row lock
  for (auto iter : lock_queue_row->request_queue_) {
    if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
      if (iter->lock_mode_ == lock_mode) {
        return true;
      }
      if (lock_queue_row->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (iter->lock_mode_ == LockManager::LockMode::SHARED && (lock_mode != LockManager::LockMode::EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // NO UPGRADE throw exception
      if (iter->lock_mode_ == LockManager::LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // Upgrade (not granted yet)
      lock_queue_row->request_queue_.remove(iter);
      DeleteRowLockFromTxn(txn, iter->lock_mode_, oid, rid);
      delete iter;
      lock_queue_row->upgrading_ = txn->GetTransactionId();
      break;
    }
  }
  LockRequest* lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_queue_row->request_queue_.push_back(lock_request);

  // Grant lock
  if (!lock_queue_row->GrantRowLockRequest(txn, lock_request)) {
    lock_queue_row->cv_.wait(lock_queue_latch);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_queue_row->request_queue_.remove(lock_request);
      if (lock_queue_row->upgrading_ == txn->GetTransactionId()) {
        LOG_DEBUG("UPGRADED ROW LOCK");
        lock_queue_row->upgrading_ = INVALID_TXN_ID;
      }
      lock_queue_row->cv_.notify_all();
      delete lock_request;
      return false;
    }
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { 
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto row_lock_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> row_lock_queue_latch(row_lock_queue->latch_);

  bool lock_held = false;
  for (auto iter : row_lock_queue->request_queue_) {
    if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
      lock_held = true;
      if ((txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && (iter->lock_mode_ == LockMode::EXCLUSIVE || iter->lock_mode_ == LockMode::SHARED)) 
      || (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&(iter->lock_mode_ == LockMode::EXCLUSIVE)) 
      || (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE))) {
        txn->SetState(TransactionState::SHRINKING);
      }
      DeleteRowLockFromTxn(txn, iter->lock_mode_, oid, rid);
      row_lock_queue->request_queue_.remove(iter);
      delete iter;
      break;
    }
  }

  if (lock_held) {
    row_lock_queue->cv_.notify_all();
    row_lock_queue_latch.unlock();
  }
  else {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  LOG_DEBUG("UNLOCKED ROW");
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
