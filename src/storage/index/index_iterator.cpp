/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int pos) : buffer_pool_manager_(bpm), page_(page), pos_(pos) {
  leaf_node_ = reinterpret_cast<LeafPage*>(page_->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

// In algorithm questions, we reach the end of a list when the next pointer of the current one is null.
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_node_->GetNextPageId() == INVALID_PAGE_ID && pos_ == leaf_node_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  return leaf_node_->GetItem(pos_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {

  // We haven't reached the end yet, so we can advance.
  assert(!IsEnd());

  // Skip to the next leaf node
  if (pos_ == leaf_node_->GetSize()-1 && leaf_node_->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next = leaf_node_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    page_ = buffer_pool_manager_->FetchPage(next);
    leaf_node_ = reinterpret_cast<LeafPage*>(page_->GetData());
    return *this;
  }

  pos_++;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
