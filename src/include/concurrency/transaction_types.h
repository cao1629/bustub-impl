//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_types.h
//
// Identification: src/include/concurrency/transaction_types.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace bustub {

/**
 * Reason to a transaction abortion
 */
enum class AbortReason {
  LOCK_ON_SHRINKING,
  UPGRADE_CONFLICT,
  LOCK_SHARED_ON_READ_UNCOMMITTED,
  TABLE_LOCK_NOT_PRESENT,
  ATTEMPTED_INTENTION_LOCK_ON_ROW,
  TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS,
  INCOMPATIBLE_UPGRADE,
  ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
};

}  // namespace bustub