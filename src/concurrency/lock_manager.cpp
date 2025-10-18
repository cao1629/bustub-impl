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

namespace bustub {


auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {

  // Check if transaction is already aborted
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // Check if the lock request is valid for this ongoing transaction
  auto abort_reason = CheckLockRequestValid(txn, lock_mode);
  if (abort_reason.has_value()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), abort_reason.value());
  }

  // Get the lock request for this table. If it does not exist, create one.
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);

  // Check for existing lock request from this transaction
  LockRequest *existing_request = nullptr;
  for (auto *request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      existing_request = request;
      break;
    }
  }

  // We saw an existing lock request from this transaction.
  if (existing_request != nullptr) {
    // Case 1: Same lock mode. We still use the existing lock and ignore the new one.
    if (existing_request->lock_mode_ == lock_mode) {
      return true;
    }

    // Case 2: Different lock mode. Need to upgrade.
    // If this transaction is now trying to upgrade a lock, we are unable to start another upgrade.
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(),
                                     AbortReason::UPGRADE_CONFLICT);
    }

    // Validate upgrade compatibility
    auto can_upgrade = CheckUpgradeCompatible(existing_request->lock_mode_, lock_mode);
    if (!can_upgrade) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    // Mark as upgrading
    lock_request_queue->upgrading_ = txn->GetTransactionId();

    // How do we upgrade a lock?
    // [1] Remove old lock request
    RemoveTableLockFromTxn(txn, existing_request->lock_mode_, oid);
    lock_request_queue->request_queue_.remove(existing_request);
    delete existing_request;

    // [2] Create new lock request and insert it after granted locks and before waiting locks
    auto upgrade_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);

    // Find the first waiting lock request and then insert right before it.
    auto it = lock_request_queue->request_queue_.begin();
    while (it != lock_request_queue->request_queue_.end() && (*it)->granted_) {
      ++it;
    }
    lock_request_queue->request_queue_.insert(it, upgrade_request);

    // [3] Wait for upgrade to be granted
    while (!CanGrantLock(upgrade_request, lock_request_queue.get())) {
      lock_request_queue->cv_.wait(lock);

      if (txn->GetState() == TransactionState::ABORTED) {
        lock_request_queue->request_queue_.remove(upgrade_request);
        delete upgrade_request;
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        // This transaction is aborted. Notify others.
        lock_request_queue->cv_.notify_all();
        return false;
      }
    }

    upgrade_request->granted_ = true;
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
    AddTableLockToTxn(txn, lock_mode, oid);

    return true;
  }

  //  No existing lock - create new request
  auto new_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(new_request);

  // Wait for lock to be granted
  while (!CanGrantLock(new_request, lock_request_queue.get())) {
    lock_request_queue->cv_.wait(lock);

    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(new_request);
      delete new_request;
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  new_request->granted_ = true;
  AddTableLockToTxn(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {

}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {

}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {

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

auto LockManager::CanGrantLock(LockRequest *request, LockRequestQueue *queue) -> bool {

  // Check compatibility with all granted locks
  for (auto *req : queue->request_queue_) {
    if (req->txn_id_ == request->txn_id_) {
      continue;
    }

    if (req->granted_) {
      // Check compatibility with granted lock
      if (!AreLocksCompatible(req->lock_mode_, request->lock_mode_)) {
        return false;
      }
    } else {
      // waiting requests
      // If we're at the front of waiting queue, grant the lock.
      if (req == request) {
        return true;
      }
    }
  }

  return false;
}

void LockManager::AddTableLockToTxn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  }
}

void LockManager::RemoveTableLockFromTxn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
}

}  // namespace bustub
