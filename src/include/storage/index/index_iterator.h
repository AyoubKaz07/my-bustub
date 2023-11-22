//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(Page* curr_page, page_id_t page_id, int index, BufferPoolManager* buffer_pool_manager);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  // Equal if they are pointing to the same index entry at the same page
  auto operator==(const IndexIterator &itr) const -> bool { 
    return page_id_ == itr.page_id_ && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { 
    return !(page_id_ == itr.page_id_ && index_ == itr.index_);
  }

 private:
  page_id_t page_id_ = INVALID_PAGE_ID;
  // KV pair index
  int index_ = 0;
  Page* curr_page_ = nullptr;
  BufferPoolManager* buffer_pool_manager_ = nullptr;
};

}  // namespace bustub
