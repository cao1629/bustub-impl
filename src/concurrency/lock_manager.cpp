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

auto LockManager::CheckUpgradeValid(LockMode old_lock, LockMode new_lock) -> bool {
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
  // std::cout << txn->GetTransactionId() << "LockTable" << std::endl;
  // Check if transaction is already aborted.
  // How come this transaction is aborted? Aborted by cycle detection.
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
  auto table_lock_request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(table_lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  // check if there is already a lock request for the same table.
  LockRequest *table_lock_request = nullptr;
  for (auto request : table_lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      table_lock_request = request;
      break;
    }
  }

  // no existing lock request from this transaction on this table.
  // We create a new one.
  if (table_lock_request == nullptr) {
    table_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    table_lock_request_queue->request_queue_.push_back(table_lock_request);
    // wait to be granted
    while (!GrantLock(table_lock_request, table_lock_request_queue.get())) {
      table_lock_request_queue->cv_.wait(lock);

      if (txn->GetState() == TransactionState::ABORTED) {
        // transaction is aborted in the meantime
        table_lock_request_queue->request_queue_.remove(table_lock_request);
        table_lock_request_queue->cv_.notify_all();
        return false;
      }
    }
    // lock is granted
    table_lock_request->granted_ = true;
    txn->AddTableLockToSet(lock_mode, oid);
    return true;
  }

  // The existing lock request for the same table has the same lock mode, we just
  // ignore the newer request.
  if (table_lock_request->lock_mode_ == lock_mode) {
    return true;
  }

  // If the existing lock request for the same table has a different lock mode,
  // we try to upgrade it.

  // (1) We cannot upgrade a lock request if another transaction is upgrading its lock request
  // on this table.
  if (table_lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // (2) check upgrade compatibility
  auto compatible = CheckUpgradeValid(table_lock_request->lock_mode_, lock_mode);
  if (!compatible) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // (3) upgrade it
  // (3.a) mark as upgrading
  table_lock_request_queue->upgrading_ = txn->GetTransactionId();
  // (3.b) Remove the exiting lock request from the queue and transaction's lock set.
  table_lock_request_queue->request_queue_.remove(table_lock_request);
  txn->RemoveTableLockFromSet(table_lock_request->lock_mode_, oid);
  // (3.c) Create a new lock request and insert it before the first waiting request.
  auto upgrade_table_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  auto it = table_lock_request_queue->request_queue_.begin();
  while (it != table_lock_request_queue->request_queue_.end() && (*it)->granted_) {
    ++it;
  }
  table_lock_request_queue->request_queue_.insert(it, upgrade_table_lock_request);
  // (3.d) Wait to be granted
  while (!GrantLock(upgrade_table_lock_request, table_lock_request_queue.get())) {
    table_lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      // transaction is aborted in the meantime
      table_lock_request_queue->upgrading_ = INVALID_TXN_ID;
      table_lock_request_queue->request_queue_.remove(upgrade_table_lock_request);
      table_lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // (3.e) The lock is granted.
  upgrade_table_lock_request->granted_ = true;
  table_lock_request_queue->upgrading_ = INVALID_TXN_ID;
  txn->AddTableLockToSet(lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // std::cout << txn->GetTransactionId() << " UnlockTable" << std::endl;
  // Step 1: Check if the transaction is trying to unlock a lock that is not in any lock request.
  table_lock_map_latch_.lock();

  if (table_lock_map_.find(oid) == table_lock_map_.cend()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto table_lock_request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(table_lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  LockRequest *table_lock_request = nullptr;
  for (auto request : table_lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      table_lock_request = request;
      break;
    }
  }

  if (table_lock_request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // Step 2: Check if the transactions holds any row locks on this table.
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

  // Step 3: Remove from lock request queue and transaction's lock set.
  table_lock_request_queue->request_queue_.remove(table_lock_request);
  auto lock_mode = table_lock_request->lock_mode_;
  txn->RemoveTableLockFromSet(lock_mode, oid);

  // Step 4: Update transaction state based on lock mode and isolation level
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
  table_lock_request_queue->cv_.notify_all();
  table_lock_map_latch_.unlock();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::cout << txn->GetTransactionId() << " LockRow " << rid.GetSlotNum() << std::endl;
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
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }

  auto row_lock_request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(row_lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  LockRequest *row_lock_request = nullptr;
  for (auto *request : row_lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      row_lock_request = request;
      break;
    }
  }

  // no existing lock request on this row from this transaction.
  if (row_lock_request == nullptr) {
    row_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
    row_lock_request_queue->request_queue_.push_back(row_lock_request);

    while (!GrantLock(row_lock_request, row_lock_request_queue.get())) {
      row_lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          // transaction is aborted in the meantime
          row_lock_request_queue->request_queue_.remove(row_lock_request);
          row_lock_request_queue->cv_.notify_all();
          return false;
        }
    }
    // Granted the lock
    txn->AddRowLockToSet(lock_mode, oid, rid);
    row_lock_request->granted_ = true;
    return true;
  }

  // an existing lock request on this row from this transaction.
  // Since each transaction runs in a separate thread, this lock request must be granted alreadly.
  // Same lock node - we just ignore the new lock request.
  // Different lock mode - we try to upgrade.
  if (row_lock_request->lock_mode_ == lock_mode) {
    return true;
  }

  // Check upgrade compatibility
  if (row_lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  auto compatible = CheckUpgradeValid(row_lock_request->lock_mode_, lock_mode);
  if (!compatible) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  // Upgrade
  // Step 1: Remove old lock request
  row_lock_request_queue->request_queue_.remove(row_lock_request);
  txn->RemoveRowLockFromSet(row_lock_request->lock_mode_, oid, rid);
  delete row_lock_request;

  // Step 2: Create new lock request and insert it after granted locks and before waiting locks
  auto upgrade_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  auto it = row_lock_request_queue->request_queue_.begin();
  while (it != row_lock_request_queue->request_queue_.end() && (*it)->granted_) {
    it++;
  }
  row_lock_request_queue->request_queue_.insert(it, upgrade_request);

  // Step 3: Mark as upgrading
  row_lock_request_queue->upgrading_ = txn->GetTransactionId();

  // Step 4: Wait for upgrade to be granted
  while (!GrantLock(upgrade_request, row_lock_request_queue.get())) {
    row_lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      // transaction is aborted in the meantime
      row_lock_request_queue->upgrading_ = INVALID_TXN_ID;
      row_lock_request_queue->request_queue_.remove(upgrade_request);
      row_lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  // Step 5: Granted
  upgrade_request->granted_ = true;
  row_lock_request_queue->upgrading_ = INVALID_TXN_ID;
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  std::cout << txn->GetTransactionId() << " UnLockRow " << rid.GetSlotNum() << std::endl;
  // Step 1: try to unlock a lock that is not in any lock request queue.
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto row_lock_request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(row_lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  LockRequest *row_lock_request = nullptr;
  for (auto request : row_lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      row_lock_request = request;
      break;
    }
  }

  if (row_lock_request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // Step 2: Remove the lock request from the lock request queue
  row_lock_request_queue->request_queue_.remove(row_lock_request);

  // Step 3: Remove the lock from the transaction's lock set
  txn->LockTxn();
  auto lock_mode = row_lock_request->lock_mode_;
  txn->RemoveRowLockFromSet(lock_mode, oid, rid);
  txn->UnlockTxn();

  // Step 4: GROWING -> SHRINKING
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

  // Step 5: notify waiting transactions
  row_lock_request_queue->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].push_back(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &neighbors = waits_for_[t1];
  neighbors.erase(std::find(neighbors.begin(), neighbors.end(), t2));
}

// Find a cycle in waits_for_, and return the largest number of transaction id.
// dfs, find a cycle, return the max txn id in the grey set.
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  visited_.clear();
  gray_set_.clear();
  found_cycle_ = false;

  for (auto source_txn_id : txn_set_) {
    dfs(source_txn_id);
    if (found_cycle_) {
      break;
    }
  }

  if (!found_cycle_) {
    return false;
  }

  txn_id_t newest_txn_id = -1;
  for (auto source_txn_id : path_) {
    newest_txn_id = std::max(newest_txn_id, source_txn_id);
  }
  *txn_id = newest_txn_id;
  return true;
}

void LockManager::dfs(txn_id_t vertex) {
  if (found_cycle_) {
    return;
  }
  visited_.insert(vertex);

  gray_set_.insert(vertex);
  for (auto neighbor : waits_for_[vertex]) {
    if (gray_set_.find(neighbor) != gray_set_.end()) {
      found_cycle_ = true;
      path_ = gray_set_;
      return;
    }

    if (visited_.find(neighbor) == visited_.end()) {
      dfs(neighbor);
    }
  }
  gray_set_.erase(vertex);
}

auto LockManager::FindTxnRowLockRequest(Transaction *txn, const RID &rid) -> LockRequest * {
  LockRequest *result = nullptr;

  // Look for the lock request for this row.
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.cend()) {
    row_lock_map_latch_.unlock();
    return nullptr;
  }

  auto lock_request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  for (auto *request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      result = request;
      break;
    }
  }

  return result;
}

auto LockManager::FindTxnTableLockRequest(Transaction *txn, table_oid_t oid) -> LockRequest * {
  LockRequest *result = nullptr;

  // Look for the lock request for this table.
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.cend()) {
    table_lock_map_latch_.unlock();
    return nullptr;
  }

  auto lock_request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  for (auto *request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn->GetTransactionId()) {
      result = request;
      break;
    }
  }

  return result;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  waits_for_latch_.lock();
  for (auto &[txn_id, neighbors] : waits_for_) {
    for (auto neighbor : neighbors) {
      edges.emplace_back(txn_id, neighbor);
    }
  }
  waits_for_latch_.unlock();
  return edges;
}

void LockManager::BuildWaitsForGraph() {
  // see table_lock_map
  table_lock_map_latch_.lock();
  row_lock_map_latch_.lock();
  for (auto &pair : table_lock_map_) {
    auto lock_request_queue = pair.second;
    std::unordered_set<txn_id_t> granted_set;
    lock_request_queue->latch_.lock();
    for (const auto &lock_request : lock_request_queue->request_queue_) {
      if (lock_request->granted_) {
        granted_set.insert(lock_request->txn_id_);
      } else {
        for (auto txn_id : granted_set) {
          txn_set_.insert(lock_request->txn_id_);
          txn_set_.insert(txn_id);
          AddEdge(lock_request->txn_id_, txn_id);
        }
      }
    }
    lock_request_queue->latch_.unlock();
  }
  // table_lock_map_latch_.unlock();

  // see row_lock_map
  // row_lock_map_latch_.lock();
  for (auto &pair : row_lock_map_) {
    auto lock_request_queue = pair.second;
    std::unordered_set<txn_id_t> granted_set;
    lock_request_queue->latch_.lock();
    for (const auto &lock_request : lock_request_queue->request_queue_) {
      if (lock_request->granted_) {
        granted_set.insert(lock_request->txn_id_);
      } else {
        for (auto txn_id : granted_set) {
          txn_set_.insert(lock_request->txn_id_);
          txn_set_.insert(txn_id);
          AddEdge(lock_request->txn_id_, txn_id);
        }
      }
    }
    lock_request_queue->latch_.unlock();
  }
  row_lock_map_latch_.unlock();
  table_lock_map_latch_.unlock();
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      BuildWaitsForGraph();
      txn_id_t abort_txn_id;

      while (HasCycle(&abort_txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(abort_txn_id);
        txn->SetState(TransactionState::ABORTED);

        // Remove abort_txn_id from waits_for
        waits_for_latch_.lock();
        waits_for_.erase(abort_txn_id);
        for (auto &[txn_id, neighbors] : waits_for_) {
          neighbors.erase(std::find(neighbors.begin(), neighbors.end(), abort_txn_id));
        }

        // Now we are aborting a transaction. This transaction might be either
        // waiting for a table lock or a row lock.
        // We need to notify other transactions waiting for the same resource.
        table_lock_map_latch_.lock();
        for (auto &[oid, lock_request_queue] : table_lock_map_) {
          lock_request_queue->latch_.lock();

          // Now we have the lock request queue for this table.
          // Find if abort_txn_id is in this queue.
          for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end();
               ++it) {
            if ((*it)->txn_id_ == abort_txn_id) {
              lock_request_queue->cv_.notify_all();
              break;
            }
          }
          lock_request_queue->latch_.unlock();
        }
        table_lock_map_latch_.unlock();

        row_lock_map_latch_.lock();
        for (auto &[rid, lock_request_queue] : row_lock_map_) {
          lock_request_queue->latch_.lock();

          // Now we have the lock request queue for this row.
          // Find if abort_txn_id is in this queue.
          for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end();
               ++it) {
            if ((*it)->txn_id_ == abort_txn_id) {
              lock_request_queue->cv_.notify_all();
              break;
            }
          }
          lock_request_queue->latch_.unlock();
        }
        row_lock_map_latch_.unlock();
      }

      txn_set_.clear();
      waits_for_.clear();
      visited_.clear();
      gray_set_.clear();
    }
  }
}

// Multiple transactions are competing for the same resource.
// "queue" contains their lock requests.
// At this moment, the current transaction only has one lock request on this resource.
auto LockManager::GrantLock(LockRequest *request, LockRequestQueue *queue) -> bool {
  // Go through all lock requests in the queue.
  for (auto request_in_queue : queue->request_queue_) {
    // We first look at all granted locks. We check lock compatibility of our lock request with each granted lock.
    if (request_in_queue->granted_) {
      if (!CheckTwoLocksCompatible(request_in_queue->lock_mode_, request->lock_mode_)) {
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
