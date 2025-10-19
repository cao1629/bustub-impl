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

auto LockManager::CheckUpgradeCompatible(LockMode old_lock, LockMode new_lock) -> bool {
  switch (old_lock) {
    case LockMode::INTENTION_SHARED:
      // IS -> [S, X, IX, SIX]
      if (new_lock == LockMode::SHARED || new_lock == LockMode::EXCLUSIVE ||
          new_lock == LockMode::INTENTION_EXCLUSIVE || new_lock == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      return false;

    case LockMode::SHARED:
      // S -> [X, SIX]
      if (new_lock == LockMode::EXCLUSIVE || new_lock == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      return false;

    case LockMode::INTENTION_EXCLUSIVE:
      // IX -> [X, SIX]
      if (new_lock == LockMode::EXCLUSIVE || new_lock == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      return false;

    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      // SIX -> [X]
      if (new_lock == LockMode::EXCLUSIVE) {
        return true;
      }
      return false;

    case LockMode::EXCLUSIVE:
      return false;

    default:
      return false;
  }
}

auto LockManager::CheckLockRequestValid(Transaction *txn, LockMode lock_mode) -> std::optional<AbortReason> {
  auto isolation_level = txn->GetIsolationLevel();
  auto txn_state = txn->GetState();

  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    // Read Uncommited: only X and IX are allowed
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      return AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
    }
  }

  if (txn_state == TransactionState::SHRINKING) {
    if (isolation_level == IsolationLevel::REPEATABLE_READ) {
      // Repeatable Reads: no locks allowed in shrinking phase
      return AbortReason::LOCK_ON_SHRINKING;
    }

    if (isolation_level == IsolationLevel::READ_COMMITTED) {
      // Read Commited: only IS and S allowed in shrinking phase
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        return AbortReason::LOCK_ON_SHRINKING;
      }
    }
  }

  // Lock request is valid
  return std::nullopt;
}

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
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    // Check upgrade compatibility
    auto can_upgrade = CheckUpgradeCompatible(existing_request->lock_mode_, lock_mode);
    if (!can_upgrade) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    // Mark as upgrading
    lock_request_queue->upgrading_ = txn->GetTransactionId();

    // How do we upgrade a lock?
    // [1] Remove old lock request
    txn->RemoveTableLock(existing_request->lock_mode_, oid);
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
    txn->AddTableLock(lock_mode, oid);

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
  txn->LockTxn();
  txn->AddTableLock(lock_mode, oid);
  txn->UnlockTxn();

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // Step 1: Find which lock mode the transaction holds on this table
  auto lock_mode = txn->FindTableLock(oid);

  // If no lock held, abort
  if (!lock_mode.has_value()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // Step 2: Check if transaction holds any row locks on this table
  // Must unlock all row locks before unlocking table
  txn->LockTxn();
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();

  bool has_row_locks = false;
  if (s_row_lock_set->find(oid) != s_row_lock_set->end() && !(*s_row_lock_set)[oid].empty()) {
    has_row_locks = true;
  }
  if (x_row_lock_set->find(oid) != x_row_lock_set->end() && !(*x_row_lock_set)[oid].empty()) {
    has_row_locks = true;
  }
  txn->UnlockTxn();

  if (has_row_locks) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // Step 3: Get the lock request queue
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  // Step 4: Lock the queue and find the request
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);

  LockRequest *lock_request = nullptr;
  for (auto *request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      lock_request = request;
      break;
    }
  }

  if (lock_request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // Step 5: Remove from queue
  lock_request_queue->request_queue_.remove(lock_request);
  delete lock_request;

  // Step 6: Remove from transaction's lock sets
  txn->RemoveTableLock(lock_mode.value(), oid);

  // Step 7: Update transaction state based on lock mode and isolation level
  if (txn->GetState() != TransactionState::ABORTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        if (txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        // READ_COMMITTED: Unlocking X -> SHRINKING, S does not affect state
        if (lock_mode == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        // READ_UNCOMMITTED: Unlocking X -> SHRINKING
        if (lock_mode == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
    }
  }

  // Step 8: Notify waiting transactions
  lock_request_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // Check lock compatability
  auto abort_reason = CheckLockRequestValid(txn, lock_mode);
  if (abort_reason.has_value()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), abort_reason.value());
  }

  // Check if transaction holds necessary table locks
  auto table_lock = txn->FindTableLock(oid);
  if (!table_lock.has_value()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  row_lock_map_latch_.lock();

  // Get the lock request queue for this row. If it does not exist, create one.
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }

  auto lock_request_queue = row_lock_map_[rid];

  // Acquire a finer lock and then release a coarser lock to make sure
  // the execution is not interrupted here.
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  LockRequest *lock_request = nullptr;
  for (auto *request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      lock_request = request;
      break;
    }
  }

  // no existing lock request on this row from this transaction.
  if (lock_request == nullptr) {
    LockRequest *new_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
    lock_request_queue->request_queue_.push_back(new_request);

    while (!CanGrantLock(new_request, lock_request_queue.get())) {
      lock_request_queue->cv_.wait(lock);
    }
    // Granted the lock
    txn->AddRowLock(lock_mode, oid, rid, lock_request);
    return true;
  }

  // an existing lock request on this row from this transaction.
  // Since each transaction runs in a separate thread, this lock request must be granted alreadly.
  // Same lock node - we just ignore the new lock request.
  // Different lock mode - we try to upgrade.
  if (lock_request->lock_mode_ == lock_mode) {
    return true;
  }

  // Check upgrade compatibility
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  auto compatible = CheckUpgradeCompatible(lock_request->lock_mode_, lock_mode);
  if (!compatible) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // Upgrade
  // Step 1: Remove old lock request
  lock_request_queue->request_queue_.remove(lock_request);
  txn->RemoveRowLock(lock_request->lock_mode_, oid, rid);
  delete lock_request;

  // Step 2: Create new lock request and insert it after granted locks and before waiting locks
  auto upgrade_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  auto it = lock_request_queue->request_queue_.begin();
  while (it != lock_request_queue->request_queue_.end() && (*it)->granted_) {
    it++;
  }
  lock_request_queue->request_queue_.insert(it, upgrade_request);

  // Step 3: Mark as upgrading
  lock_request_queue->upgrading_ = txn->GetTransactionId();

  // Step 4: Wait for upgrade to be granted
  while (!CanGrantLock(upgrade_request, lock_request_queue.get())) {
    lock_request_queue->cv_.wait(lock);
  }

  // Step 5: Granted
  upgrade_request->granted_ = true;
  lock_request_queue->upgrading_ = INVALID_TXN_ID;
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // Step 1: Do we try to unlock a row lock that we do not hold?
  auto lock_mode = txn->FindRowLock(oid, rid);
  if (!lock_mode.has_value()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // Step 2: Remove the lock request from the lock request queue
  row_lock_map_latch_.lock();
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  // Find the lock request we need to remove
  LockRequest *lock_request = nullptr;

  for (auto *request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      lock_request = request;
      break;
    }
  }

  lock_request_queue->request_queue_.remove(lock_request);
  free(lock_request);

  // Step 3: Remove the lock from the transaction's lock set
  txn->RemoveRowLock(lock_mode.value(), oid, rid);

  // Step 4: notify waiting transactions
  lock_request_queue->cv_.notify_all();
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

}  // namespace bustub
