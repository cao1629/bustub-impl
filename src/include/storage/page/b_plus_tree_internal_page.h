//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(ItemType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:

  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;

  void SetKeyAt(int index, const KeyType &key);

  auto ValueAt(int index) const -> ValueType;

  void RemoveAt(int index);

  // Now I am in an internal page. Given a key, find the page on the next level to go down.
  auto FindNextLevelPage(const KeyType &key, const KeyComparator &comparator) const -> ValueType;

  auto ValueIndex(const ValueType &value) const -> int;

  void InsertAfterValue(const ValueType &prev, const KeyType &key, const ValueType &value);

  // We want to move the first item to the recipient, but the first item has no key. We need to provide
  // a key.
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager,
    const KeyType &separator_key);

  // After we move the last item to the recipient, the first item of the recipient becomes
  // the second item, but it has no key. We need to provide a key.
  void MoveLastToHeadOf(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager,
    const KeyType &separator_key);

  // the recipient is an empty page.
  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);

  // for coalescing
  void MoveAllTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager, KeyType &separator_key);

 private:
  // Flexible array member for page data.
  ItemType array_[1];

  void CopyToEnd(const ItemType &item, BufferPoolManager *buffer_pool_manager);

  void CopyToHead(const ItemType &item, BufferPoolManager *buffer_pool_manager);

  void CopyNToEnd(ItemType *items, int size, BufferPoolManager *buffer_pool_manager);
};
}  // namespace bustub
