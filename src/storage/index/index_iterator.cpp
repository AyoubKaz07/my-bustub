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
INDEXITERATOR_TYPE::IndexIterator() : page_id_(INVALID_PAGE_ID) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *curr_page, page_id_t page_id, int index, BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id), index_(index), curr_page_(curr_page), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  auto curr_leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(curr_page_->GetData());
  return static_cast<bool>(index_ == curr_leaf_node->GetSize() && curr_leaf_node->GetNextPageId() == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto curr_leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(curr_page_->GetData());
  return curr_leaf_node->GetPair(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  auto curr_leaf_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(curr_page_->GetData());
  // at end -> go to next page
  if (index_ == curr_leaf_node->GetSize() && curr_leaf_node->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page = buffer_pool_manager_->FetchPage(curr_leaf_node->GetNextPageId());
    next_page->RLatch();
    curr_page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(curr_leaf_node->GetPageId(), false);
    curr_page_ = next_page;
    page_id_ = curr_page_->GetPageId();
    index_ = 0;
  } else /* at the end but no next */ {
    if (index_ == curr_leaf_node->GetSize() && curr_leaf_node->GetNextPageId() == INVALID_PAGE_ID) {
      curr_page_->RUnlatch();
      buffer_pool_manager_->UnpinPage(curr_leaf_node->GetPageId(), false);
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
