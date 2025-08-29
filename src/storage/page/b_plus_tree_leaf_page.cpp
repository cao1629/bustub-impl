//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }


// Find the first index i so that array_[i].first >= key.
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) const -> int {
  auto it = std::lower_bound(array_.begin(), array_.end(), key,
    [&keyComparator](const auto &pair, const KeyType &key) { return keyComparator(pair.first, key) < 0; });
  return std::distance(array_, it);
}

// Find the first index i so that array_[i].first >= key.
// (In fact, array_[i].first == key never happens in this B+ tree)
// Then insert the key-value pair at index i, and move the rest part to the right by one position.
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                                                  const KeyComparator &keyComparator) {
  auto index = KeyIndex(key, keyComparator);

  // Insert at the end
  if (index == GetSize()) {
    *(array_ + index) = {key, value};
    IncreaseSize(1);
  }

  std::move(array_+index, array_+GetSize(), array_+index+1);
  *(array_ + index) = {key, value};
  IncreaseSize(1);
}

// Given a key, find its value. Return true if exists, false otherwise.
// Store the value in the output parameter "value"/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType &key, ValueType *value, const KeyComparator &keyComparator) const -> bool {
  auto it = std::find_if(array_, array_+GetSize(),
        [&key, &keyComparator](const auto &pair) {
          return keyComparator(pair.first, key) == 0;
        });
  if (it != array_+GetSize()) {
    *value = it->second;
    return true;
  }
  return false;
}

// Given a key, remove its k/v pair from the leaf page.
// Return true if exists, false otherwise.
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &keyComparator) -> bool {
  auto index = KeyIndex(key, keyComparator);
  if (index == GetSize()) {
    return false;
  }

  if (keyComparator(array_[index].first, key) != 0) {
    return false;
  }

  std::move(array_ + index, array_ + GetSize(), array_ + index - 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  auto first_item = array_[0];
  std::move(array_ + 1, array_ + GetSize(), array_);
  recipient->CopyToEnd(first_item);
  IncreaseSize(-1);
}

// Move the last key/value pair from this page to the front of "recipient".
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToHeadOf(BPlusTreeLeafPage *recipient) {
  auto last_item = array_[GetSize()-1];
  recipient->CopyToHead(last_item);
  IncreaseSize(-1);
}

// Move
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  // Keep the minimum number of items in this page
  int start_index = GetMinSize();
  recipient->CopyNToEnd(array_+start_index, GetSize() - start_index);
  SetSize(start_index);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyToEnd(array_, GetSize());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyToHead(const ItemType &item) {
  std::move(array_, array_+GetSize(), array_+1);

  // copy
  array_[0] = item;

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyToEnd(const ItemType &item) {
  array_[GetSize()] = item;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNToEnd(ItemType *items, int size) {
  std::move(items, items+size, array_+GetSize());
  IncreaseSize(size);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
