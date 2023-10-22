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
  SetNextPageId(INVALID_PAGE_ID);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }


/*
  * Helper method to get the pair associated with input "index"(a.k.a array offset)
*/

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPair(int index) -> MappingType& {
  return array_[index];
}


/*
 * Custom method to find value inside
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Find(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  for (int i = 1; i < GetSize(); i++) {
    if (comparator(array_[i].first, key) > 0) {
      return array_[i - 1].second;
    }
  }
  return array_[GetSize() - 1].second;
}

/*
 * Custom method to insert KV pair in the array
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const int index,
                                        const KeyComparator &comparator) -> bool {
  if (comparator(array_[index].first, key) == 0) {
    return false;
  }
  // keep array sorted, slide elems to right until find the right place
  for (int i = GetSize() - 1; i >= index; i--) {
    array_[i + 1].first = array_[i].first;
    array_[i + 1].second = array_[i].second;
  }
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
  return true;
}

/*
 * Custom method to delete KV pair in the array
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = KeyIndex(key, comparator);
  if (index >= GetSize() || comparator(array_[index].first, key) != 0) {
    return false;
  }
  // slide elems to left
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i].first = array_[i + 1].first;
    array_[i].second = array_[i + 1].second;
  }
  IncreaseSize(-1);
  return true;
}

/*
 * Custom method to find the index of the key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  int l = 0;
  int r = GetSize();
  if (l >= r) {
    return GetSize();
  }
  while (l < r) {
    int mid = (l + r) / 2;
    if (comparator(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l;
}

/*
 * Custom method to transfer the right half of the original page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Split(Page *sibling) {
  int mid = GetSize() / 2;

  auto sibling_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(sibling->GetData());
  for (int i = mid, j = 0; i < GetMaxSize(); i++, j++) {
    sibling_leaf->array_[j] = array_[i];
    IncreaseSize(-1);
  }
  sibling_leaf->SetSize(GetMaxSize() - mid);

  sibling_leaf->SetNextPageId(GetNextPageId());
  SetNextPageId(sibling_leaf->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(Page *page, BufferPoolManager *buffer_pool_manager_) {
  auto page_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  for (int i = GetSize(), j = 0; j < page_node->GetSize(); j++, i++) {
    array_[i] = std::make_pair(page_node->KeyAt(j), page_node->ValueAt(j));
    IncreaseSize(1);
  }
  page_node->SetSize(0);
  buffer_pool_manager_->UnpinPage(page_node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(page_node->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteLast(const KeyType &key, const KeyComparator &comparator) -> bool {
  if (comparator(array_[GetSize() - 1].first, key) != 0) {
    return false;
  }
  IncreaseSize(-1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteFirst(const KeyType &key, const KeyComparator &comparator) -> bool {
  if (comparator(array_[0].first, key) != 0) {
    return false;
  }
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i].first = array_[i + 1].first;
    array_[i].second = array_[i + 1].second;
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) -> void {
  for (int i = GetSize(); i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertLast(const KeyType &key, const ValueType &value) -> void {
  array_[GetSize()] = std::make_pair(key, value);
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
