#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

#include "concurrency/transaction.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
// Insert the result into "result" vector. If the key does not exist,return false.
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  if (IsEmpty()) {
    return false;
  }

  auto leaf_page = FindLeaf(key, Operation::READ);  // unpin the leaf page later on
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // After we return from FindLeaf(), we still hold the read latch on the leaf node.

  // Search for the key in the leaf node
  ValueType value;
  auto found = leaf_node->Find(key, &value, comparator_);
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);

  // Release the latch on this leaf node
  leaf_page->RUnlatch();

  if (found) {
    result->push_back(value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
//
// Unpin all pages that have been fetched.
// If "key" already exists in some leaf page, do nothing and return false.
// Otherwise, do the insertion and return true.
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // check if tree is empty
  root_latch_.WLock();

  if (IsEmpty()) {
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);

    // If the tree only has a root node, then the root node is a leaf node.
    auto root_node = reinterpret_cast<LeafPage *>(root_page->GetData());

    root_page->WLatch();

    root_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    root_node->SetPageType(IndexPageType::LEAF_PAGE);
    root_node->Insert(key, value, comparator_);
    root_node->SetNextPageId(INVALID_PAGE_ID);
    root_page->WUnlatch();

    root_latch_.WUnlock();
    return true;
  }

  // Find the leaf node to insert the key/value pair.
  auto leaf_page = FindLeaf(key, Operation::INSERT, txn);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  if (!leaf_node->Insert(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    ReleaseAllLatchesFromQueue(txn);
    return false;
  }

  auto new_size = leaf_node->GetSize();

  // no need to split
  if (new_size < leaf_max_size_) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    ReleaseAllLatchesFromQueue(txn);
    return true;
  }

  // need to split
  // now we have latches all the way from root_latch_ to the leaf node here.
  auto new_right_sibling = Split(leaf_node);
  new_right_sibling->SetNextPageId(leaf_node->GetNextPageId());
  leaf_node->SetNextPageId(new_right_sibling->GetPageId());

  // Insert one item into parent.
  InsertIntoParent(leaf_node, new_right_sibling, new_right_sibling->KeyAt(0), txn);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
// What if we try to remove a key that does not exist? We do nothing and just return.
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  if (IsEmpty()) {
    return;
  }

  root_latch_.WLock();
  auto leaf_page = FindLeaf(key, Operation::DELETE, txn);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page);

  // "key" exists in this leaf page. So we do nothing and return.
  if (!leaf_node->Remove(key, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    ReleaseAllLatchesFromQueue(txn);
    return;
  }

  // no need to redistribute or coalesce
  if (leaf_node->IsRootPage() || leaf_node->GetSize() >= leaf_node->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    ReleaseAllLatchesFromQueue(txn);
    return;
  }

  // need to redistribute or coalesce
  RedistributeOrCoalesce(leaf_node, txn);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftOrRightMostLeaf(bool leftmost) -> Page * {
  auto p = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *node = reinterpret_cast<BPlusTreePage *>(p->GetData());

  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    int child_page_id;
    if (leftmost) {
      child_page_id = internal_node->ValueAt(0);
    } else {
      child_page_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    }
    buffer_pool_manager_->UnpinPage(child_page_id, false);
    p = buffer_pool_manager_->FetchPage(child_page_id);
    node = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }

  return p;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto leftmost_leaf_page = FindLeftOrRightMostLeaf(true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leftmost_leaf_page);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto start_leaf_page = FindLeaf(key, Operation::READ);
  auto start_leaf_node = reinterpret_cast<LeafPage *>(start_leaf_page->GetData());
  auto pos = start_leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, start_leaf_page, pos);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto rightmost_leaf_page = FindLeftOrRightMostLeaf(false);
  auto rightmost_leaf_node = reinterpret_cast<LeafPage *>(rightmost_leaf_page->GetData());
  auto pos = rightmost_leaf_node->GetSize();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, rightmost_leaf_page, pos);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllLatchesFromQueue(Transaction *txn) {
  while (!txn->GetPageSet()->empty()) {
    auto *page = txn->GetPageSet()->front();
    txn->GetPageSet()->pop_front();
    page->WUnlatch();
  }
  root_latch_.WUnlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseOneLatchFromQueue(Transaction *txn) {
  while (!txn->GetPageSet()->empty()) {
    auto *page = txn->GetPageSet()->back();
    txn->GetPageSet()->pop_back();
    page->WUnlatch();
  }
}


// If "key" is smaller than any key in the tree, return the leftmost leaf page.
// If "key" is larger than any key in the tree, return the rightmost leaf.
// When we call FindLeaf(), we are sure that the tree is not empty.
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Operation operation, Transaction *txn) -> Page * {
  // At this moment, root_latch_ is already latched.
  // Latch mode of root_latch_ depends on the caller of FindLeaf()
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  // root node could be either internal node or leaf node
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  Page *next_page;
  BPlusTreePage *next_tree_page;

  if (operation == Operation::READ) {
    page->RLatch();
  } else {
    txn->AddIntoPageSet(page);
    page->WLatch();
  }

  // FindLeaf() is read operation.
  while (!tree_page->IsLeafPage()) {

    auto *internal_node = reinterpret_cast<InternalPage *>(tree_page);
    auto child_page_id = internal_node->FindChild(key, comparator_);
    buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), false);

    next_page = buffer_pool_manager_->FetchPage(child_page_id);
    next_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

    if (operation == Operation::READ) {
      // latch the next page and unlatch the current page
      next_page->RLatch();
      page->RUnlatch();

    } else if (operation == Operation::INSERT) {
      next_page->WLatch();

      // child node is safe, release all locks on ancestors
      if (next_tree_page->GetSize() < next_tree_page->GetMaxSize()-1) {
        ReleaseAllLatchesFromQueue(txn);
      }

      txn->AddIntoPageSet(next_page);

    } else if (operation == Operation::DELETE) {
      next_page->WLatch();

      // child node is safe, release all locks on ancestors
      if (next_tree_page->GetSize() > next_tree_page->GetMinSize()) {
        ReleaseAllLatchesFromQueue(txn);
      }

      txn->AddIntoPageSet(next_page);
    }

    page = next_page;
    tree_page = next_tree_page;
  }

  return page;
}

// After splitting the parent page, we need to insert a new k/v pair into its parent.
// k: the first key in the new node
// after inserting k/v pair into the parent, we do not need the first key any more if "new_node" is an internal page
// v: the page id of the new node
//
// {1} If the old node is the root, we need to create a new root page.
// {2} If the parent node is not full (< max size), we do not need to split it.
// {3} If the parent node is full (= max size), we need to split it recursively.
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *tree_page, BPlusTreePage *new_tree_page, const KeyType &key,
                                      Transaction *txn) {
  // The old tree page is the root. Now we need to create a new root with two children.
  if (tree_page->IsRootPage()) {
    // Create a new root page
    auto new_root_tree_page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto new_root_tree_internal_page = reinterpret_cast<InternalPage *>(new_root_tree_page->GetData());
    new_root_tree_internal_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

    auto tree_leaf_page = reinterpret_cast<LeafPage *>(tree_page);
    auto new_tree_leaf_page = reinterpret_cast<LeafPage *>(new_tree_page);

    // Insert two children into the new root
    new_root_tree_internal_page->InsertFirst(tree_leaf_page->KeyAt(0), tree_page->GetPageId());
    new_root_tree_internal_page->InsertAfterValue(tree_leaf_page->GetPageId(), key, new_tree_leaf_page->GetPageId());

    // Update the parent page id of the two children
    tree_page->SetParentPageId(new_root_tree_page->GetPageId());
    new_tree_page->SetParentPageId(new_root_tree_page->GetPageId());

    buffer_pool_manager_->UnpinPage(new_root_tree_page->GetPageId(), true);

    // Now transaction's page set contains only tree_page.
    ReleaseAllLatchesFromQueue(txn);
    return;
  }

  // old tree page is not the root. We first fetch its parent page.
  // Then we insert the new split page into the parent page.
  auto *parent_tree_page = buffer_pool_manager_->FetchPage(tree_page->GetParentPageId());
  auto *parent_tree_internal_page = reinterpret_cast<InternalPage *>(parent_tree_page->GetData());

  // Release latch on "tree_page"
  ReleaseOneLatchFromQueue(txn);

  parent_tree_internal_page->InsertAfterValue(tree_page->GetPageId(), key, new_tree_page->GetPageId());

  // no need to split
  if (parent_tree_internal_page->GetSize() < parent_tree_internal_page->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(parent_tree_internal_page->GetPageId(), true);
    ReleaseAllLatchesFromQueue(txn);
    return;
  }

  auto new_right_sibling = Split(parent_tree_internal_page);
  const auto &new_key = new_right_sibling->KeyAt(0);
  InsertIntoParent(parent_tree_internal_page, new_right_sibling, new_key, txn);
}

// (1) new page
// (2) move half
// (3) return the new page
// This page will be on the left, and the new page will be on the right.
// We do not update the parent page here.
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);

  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  if (node->IsLeafPage()) {
    new_node->SetPageType(IndexPageType::LEAF_PAGE);
  } else {
    new_node->SetPageType(IndexPageType::INTERNAL_PAGE);
  }

  if (node->IsLeafPage()) {
    auto *leaf_page = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf_page = reinterpret_cast<LeafPage *>(new_node);
    new_leaf_page->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    new_leaf_page->SetNextPageId(INVALID_PAGE_ID);
    leaf_page->MoveHalfTo(new_leaf_page);
  } else {
    auto *internal_page = reinterpret_cast<InternalPage *>(node);
    auto *new_internal_page = reinterpret_cast<InternalPage *>(new_node);
    new_internal_page->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
    internal_page->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  }
  return new_node;
}

// (1) try to steal one item from the left sibling
// (2) if not possible, try to coalesce with the left sibling
// (3) if not possible, try to steal one item from the right sibling
// (4) if not possible, try to coalesce with the right sibling
// (5) if still not possible, we are at the root. Just return.
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::RedistributeOrCoalesce(N *node, Transaction *txn) -> bool {
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  auto index = parent_node->ValueIndex(node->GetPageId());

  // "node" has a left sibling
  if (index > 0) {
    auto left_sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(index - 1));
    auto left_sibling_node = reinterpret_cast<N *>(left_sibling_page->GetData());

    // It's fine to steal one item from the left sibling
    if (left_sibling_node->GetSize() > left_sibling_node->GetMinSize()) {
      Redistribute(node, left_sibling_node, true, parent_node, index);

      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);

      ReleaseAllLatchesFromQueue(txn);
      return true;
    }

    // coalesce with the left sibling
    Coalesce(node, left_sibling_node, true, parent_node, index, txn);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
    return true;
  }

  // "node" has a right sibling
  if (index < parent_node->GetSize() - 1) {
    auto right_sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(index + 1));
    auto right_sibling_node = reinterpret_cast<N *>(right_sibling_page->GetData());

    if (right_sibling_node->GetSize() > right_sibling_node->GetMinSize()) {
      Redistribute(node, right_sibling_node, false, parent_node, index);

      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);

      ReleaseAllLatchesFromQueue(txn);
      return true;
    }

    Coalesce(node, right_sibling_node, false, parent_node, index, txn);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
    return true;
  }

  return false;
}

// After we remove an item from a node, we need to redistribute or coalesce.
// We failed to redistribute, so we now coalesce with the sibling node.
// If we coalesce with the left sibling, we move all items of "node" to the left sibling and delete "node".
// if we coalesce with the right sibling, we move all items of the right sibling to "node" and delete the right sibling.
// "index": parent[index] points to "node"
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(N *node, N *sibling, bool is_left_sibling, InternalPage *parent, int index,
                              Transaction *txn) {
  std::cout << "xxx" << std::endl;
  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto sibling_leaf_node = reinterpret_cast<LeafPage *>(sibling);
    if (is_left_sibling) {
      leaf_node->MoveAllTo(sibling_leaf_node);
      parent->RemoveAt(index);
    } else {
      sibling_leaf_node->MoveAllTo(leaf_node);
      parent->RemoveAt(index + 1);
    }
  } else {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto sibling_internal_node = reinterpret_cast<InternalPage *>(sibling);
    if (is_left_sibling) {
      internal_node->SetKeyAt(0, parent->KeyAt(index));
      auto separator_key = parent->KeyAt(index);
      internal_node->MoveAllTo(sibling_internal_node, buffer_pool_manager_, separator_key);
      parent->RemoveAt(index);
    } else {
      auto separator_key = parent->KeyAt(index + 1);
      sibling_internal_node->MoveAllTo(internal_node, buffer_pool_manager_, separator_key);
      parent->RemoveAt(index + 1);
    }
  }


  if (parent->GetSize() < parent->GetMinSize()) {
    ReleaseOneLatchFromQueue(txn);
    RedistributeOrCoalesce(parent, txn);
  } else {
    ReleaseAllLatchesFromQueue(txn);
  }
}

// "index": the index of "node" in "parent"
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *node, N *sibling, bool is_left_sibling, InternalPage *parent, int index) {
  std::cout << "bbb" << std::endl;
  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    auto sibling_leaf_node = reinterpret_cast<LeafPage *>(sibling);
    if (is_left_sibling) {
      sibling_leaf_node->MoveLastToHeadOf(leaf_node);
      parent->SetKeyAt(index, leaf_node->KeyAt(0));
    } else {
      sibling_leaf_node->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(index, sibling_leaf_node->KeyAt(0));
    }
  } else {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    auto sibling_internal_node = reinterpret_cast<InternalPage *>(sibling);

    if (is_left_sibling) {
      sibling_internal_node->MoveLastToFrontOf(internal_node, buffer_pool_manager_, parent->KeyAt(index));
      parent->SetKeyAt(index, internal_node->KeyAt(0));
    } else {
      sibling_internal_node->MoveFirstToEndOf(internal_node, buffer_pool_manager_, parent->KeyAt(index + 1));
      parent->SetKeyAt(index + 1, sibling_internal_node->KeyAt(0));
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
