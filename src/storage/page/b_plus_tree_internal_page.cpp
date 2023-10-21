//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void {
  array_[index].second = value;
}

/*
 * Custom method to find value inside
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Find(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const int index,
                                            const KeyComparator &comparator) -> void {
  // keep array sorted, slide elems to right until find the right place
  for (int i = GetSize() - 1; i >= index; i--) {
    array_[i + 1].first = array_[i].first;
    array_[i + 1].second = array_[i].second;
  }
  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
}

/*
 * Custom method to delete KV pair in the array
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> bool {
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  // binary search
  int l = 1;
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
 * We still insert in the whole array then we split the values
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(const KeyType &key, Page *child, Page *parent_child_page,
                                           BufferPoolManager *buffer_pool_manager_, const KeyComparator &comparator)
    -> void { 
  // DANGER, MIGHT KILL 
  std::vector<std::pair<KeyType, ValueType>> temp(GetMaxSize() + 1);

  for (int i = 0; i < GetMaxSize(); i++) {
    temp[i] = array_[i];
  }

  int i;
  for (i = GetMaxSize() - 1; i > 0; i--) {
    if (comparator(temp[i].first, key) > 0) {
      temp[i + 1] = temp[i];
    } else {
      temp[i + 1] = std::make_pair(key, child->GetPageId());
      break;
    }
  }
  if (i == 0) {
    temp[1] = std::make_pair(key, child->GetPageId());
  }
  IncreaseSize(1);

  int mid = (GetMaxSize() + 1) / 2;
  auto parent_child_inter = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_child_page->GetData());
  auto child_inter = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child->GetData());
  child_inter->SetParentPageId(GetPageId());
  for (i = 0; i < mid; i++) {
    array_[i] = temp[i];
  }
  i = 0;
  while (mid <= (GetMaxSize())) {
    Page *child = buffer_pool_manager_->FetchPage(temp[mid].second);
    auto child_inter = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child->GetData());
    child_inter->SetParentPageId(parent_child_inter->GetPageId());
    parent_child_inter->array_[i] = temp[mid];
    i++;
    mid++;
    IncreaseSize(-1);
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  }
  parent_child_inter->SetSize(i);
}

/*
 * Custom method to find neighbor page
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindNeighbor(page_id_t page_id, Page *&NP_page, KeyType &KeyPrime, bool &is_pred,
                                                  BufferPoolManager *buffer_pool_manager_) -> void {
  int i;
  for (i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == page_id) {
      break;
    }
  }
  if ((i - 1) >= 0) {
    NP_page = buffer_pool_manager_->FetchPage(ValueAt(i - 1));
    KeyPrime = KeyAt(i - 1);
    is_pred = true;
  } else {
    NP_page = buffer_pool_manager_->FetchPage(ValueAt(i + 1));
    KeyPrime = KeyAt(i);
    is_pred = false;
  }
}

/*
 * Custom method to merge two internal pages
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(Page *sibling, KeyType &KeyPrime, BufferPoolManager *buffer_pool_manager_)
    -> void {
  int size = GetSize();
  auto sibling_inter = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(sibling->GetData());
  array_[size] = std::make_pair(KeyPrime, sibling_inter->ValueAt(0));
  IncreaseSize(1);

  for (int i = GetSize(), j = 1; j < sibling_inter->GetSize(); i++, j++) {
    array_[i] = std::make_pair(sibling_inter->KeyAt(j), sibling_inter->ValueAt(j));
    IncreaseSize(1);
  }

  buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  buffer_pool_manager_->DeletePage(sibling->GetPageId());
  for (int i = size; i < GetSize(); i++) {
    auto child_page = buffer_pool_manager_->FetchPage(ValueAt(i));
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteLast(const KeyType &key, const KeyComparator &comparator) -> bool {
  if (comparator(array_[GetSize() - 1].first, key) != 0) {
    return false;
  }
  IncreaseSize(-1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirst(const KeyType &key, const KeyComparator &comparator) -> bool {
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) -> void {
  for (int i = GetSize(); i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  SetValueAt(0, value);
  SetKeyAt(1, key);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertLast(const KeyType &key, const ValueType &value) -> void {
  array_[GetSize()] = std::make_pair(key, value);
  IncreaseSize(1);
}

/*INDEX_TEMPLATE_ARGUMENTSnode->
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::FixPointers(Page* page, BufferPoolManager *buffer_pool_manager_) {
  auto child_inter = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  for (int i = 0 ; i < GetSize() ; i++) {
    Page* child = buffer_pool_manager_->FetchPage(child_inter->array_[i].second);
    auto child_inter = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child->GetData());
    child_inter->SetParentPageId(child_inter->GetPageId());
  }
}*/

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
