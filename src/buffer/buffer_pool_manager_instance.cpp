//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!GetFrame(&frame_id)) {
    return nullptr;
  }

  page_id_t new_page_id = AllocatePage();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(new_page_id, frame_id);

  *page_id = new_page_id;
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    // trying this for data-race (like reserving the frame)
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  }
  if (GetFrame(&frame_id)) {
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    pages_[frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page_table_->Insert(page_id, frame_id);
    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPgImp(pages_[i].page_id_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    // Page not in buffer pool, so do nothing and return true
    return true;
  }

  // Check if page is pinned
  if (pages_[frame_id].pin_count_ > 0) {
    return false;  // cannot delete a pinned page
  }

  // Remove page from page table and replacer
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);

  // Reset page memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  // Add frame back to free list
  free_list_.push_back(frame_id);

  // Deallocate page to imitate freeing it on disk
  DeallocatePage(page_id);

  // Return true to indicate success
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetFrame(frame_id_t *frame_id) -> bool {
  frame_id_t res_frame_id;
  if (!free_list_.empty()) {
    res_frame_id = free_list_.front();
    free_list_.pop_front();
    *frame_id = res_frame_id;
    return true;
  }

  if (replacer_->Evict(&res_frame_id)) {
    if (pages_[res_frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[res_frame_id].page_id_, pages_[res_frame_id].data_);
      pages_[res_frame_id].is_dirty_ = false;
    }
    page_table_->Remove(pages_[res_frame_id].page_id_);
    *frame_id = res_frame_id;
    return true;
  }
  return false;
}
}  // namespace bustub
