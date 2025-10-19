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
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  // Is there an existing lock request from this transaction?
  LockRequest *existing_request = nullptr;
  for (auto *request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      existing_request = request;
      break;
    }
  }

  // No existing lock request from this transaction.
  // We create a new lock request and append it to the queue.
  if (existing_request == nullptr) {
    LockRequest *new_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    lock_request_queue->request_queue_.push_back(new_request);

    // Wait for lock to be granted
    while (!CanGrantLock(new_request, lock_request_queue.get())) {
      lock_request_queue->cv_.wait(lock);
    }

    // Lock granted
    new_request->granted_ = true;
    txn->AddTableLock(lock_mode, oid);
    return true;
  }

  // An existing lock request from this transaction.
  // See if we need to upgrade it.
  if (existing_request->lock_mode_ == lock_mode) {
    return true;
  }

  // Check upgrade compatability
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  auto can_upgrade = CheckUpgradeCompatible(existing_request->lock_mode_, lock_mode);
  if (!can_upgrade) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // Now we know we need to upgrade the existing lock request.
  // Step 1: Mark as upgrading
  lock_request_queue->upgrading_ = txn->GetTransactionId();

  // Step 2: Remove the existing lock request from the queue.
  txn->RemoveTableLock(existing_request->lock_mode_, oid);
  lock_request_queue->request_queue_.remove(existing_request);
  delete existing_request;

  // Step 3: Remove the lock from transaction's lock set.
  // The lock must be in the transaction's lock set, because when a transaction tries to upgrade
  // an existing lock, it must have held the existing lock.
  txn->RemoveTableLock(existing_request->lock_mode_, oid);

  // Step 4: Create a new lock request and insert it before the first waiting request.
  auto upgrade_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  auto it = lock_request_queue->request_queue_.begin();
  while (it != lock_request_queue->request_queue_.end() && (*it)->granted_) {
    ++it;
  }
  lock_request_queue->request_queue_.insert(it, upgrade_request);

  // Step 5: Wait for be granted
  while (!CanGrantLock(upgrade_request, lock_request_queue.get())) {
    lock_request_queue->cv_.wait(lock);

  }

  // Step 6: Upgrade is complete. Finish up.
  upgrade_request->granted_ = true;
  lock_request_queue->upgrading_ = INVALID_TXN_ID;
  txn->AddTableLock(lock_mode, oid);

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // Find which lock mode the transaction holds on this table
  auto lock_mode = txn->FindTableLock(oid);

  // If no lock held, abort. We cannot unlock a lock that is not held.
  if (!lock_mode.has_value()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // Check if transaction holds any row locks on this table.
  // We cannot unlock table locks if there are row locks held.
  // Must unlock all row locks before unlocking table locks
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();

  bool has_row_locks = false;
  if (s_row_lock_set->find(oid) != s_row_lock_set->end() && !(*s_row_lock_set)[oid].empty()) {
    has_row_locks = true;
  }
  if (x_row_lock_set->find(oid) != x_row_lock_set->end() && !(*x_row_lock_set)[oid].empty()) {
    has_row_locks = true;
  }

  if (has_row_locks) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  table_lock_map_latch_.lock();
  // We try to find the lock request queue for this table.
  // If this does not exist, which seems impossible, we abort.
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  LockRequest *existing_request = nullptr;
  for (auto request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      existing_request = request;
      break;
    }
  }

  // This transaction has never issued a lock request on this table.
  // Or this lock has already been released.
  if (existing_request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // Now we unlock this table.
  // Step 1: Remove from lock request queue.
  lock_request_queue->request_queue_.remove(existing_request);
  delete existing_request;

  // Step 2: Remove from transaction's lock sets
  txn->RemoveTableLock(lock_mode.value(), oid);

  // Step 3: Update transaction state based on lock mode and isolation level
  if (txn->GetState() != TransactionState::ABORTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        if (txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        if (lock_mode == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if (lock_mode == LockMode::EXCLUSIVE && txn->GetState() == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
    }
  }

  // Step 4: Notify other waiting transactions.
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
    txn->AddRowLock(lock_mode, oid, rid);
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

// Multiple transactions are competing for the same resource.
// "queue" contains their lock requests.
// At this moment, the current transaction only has one lock request on this resource.
auto LockManager::CanGrantLock(LockRequest *request, LockRequestQueue *queue) -> bool {
  // Go through all lock requests in the queue.
  for (auto request_in_queue : queue->request_queue_) {
    // We first look at all granted locks. We check lock compatibility of our lock request with each granted lock.
    if (request_in_queue->granted_) {
      if (!CheckLocksCompatible(request_in_queue->lock_mode_, request->lock_mode_)) {
        return false;
      }
    }

    // Now we start look at all waiting locks.
    // If I am the first one in the waiting queue, I can be granted the lock.
    // Otherwise, I have to wait.
    else if (request_in_queue == request) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
