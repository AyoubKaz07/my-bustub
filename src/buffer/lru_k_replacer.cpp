//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "iostream"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // we look from the back -> front for the frame with largest backward k-distance
  bool found_one = false;
  // first try to evict from history list (+inf) frames
  if (!history_list_.empty()) {
    for (auto rit = history_list_.rbegin(); rit != history_list_.rend(); ++rit) {
      if (set_entries_[*rit].is_evictable_) {
        *frame_id = *rit;
        history_list_.erase(std::next(rit).base());
        found_one = true;
        break;
      }
    }
  }

  if (!found_one && !cache_list_.empty()) {
    for (auto rit = cache_list_.rbegin(); rit != cache_list_.rend(); ++rit) {
      if (set_entries_[*rit].is_evictable_) {
        *frame_id = *rit;
        cache_list_.erase(std::next(rit).base());
        found_one = true;
        break;
      }
    }
  }

  if (found_one) {
    set_entries_.erase(*frame_id);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    BUSTUB_ASSERT(false, "INVALID FRAME ID");
  }
  size_t access_count = ++set_entries_[frame_id].access_count;
  // new frame
  if (access_count == 1) {
    history_list_.emplace_front(frame_id);
    set_entries_[frame_id].position_ = history_list_.begin();
    curr_size_++;
  } else {
    // move from history_list_ to cache queue list
    if (access_count == k_) {
      history_list_.erase(set_entries_[frame_id].position_);
      cache_list_.emplace_front(frame_id);
      set_entries_[frame_id].position_ = cache_list_.begin();
    }
    // move to front of the cache queue (LRU)
    else if (access_count > k_) {
      cache_list_.erase(set_entries_[frame_id].position_);
      cache_list_.emplace_front(frame_id);
      set_entries_[frame_id].position_ = cache_list_.begin();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    BUSTUB_ASSERT(false, "INVALID FRAME ID");
  }
  if (set_entries_.find(frame_id) == set_entries_.end()) {
    return;
  }
  bool old_status = set_entries_[frame_id].is_evictable_;
  if (old_status && !set_evictable) {
    curr_size_--;
  } else if (!old_status && set_evictable) {
    curr_size_++;
  }
  set_entries_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    BUSTUB_ASSERT(false, "INVALID FRAME ID");
  }
  if (set_entries_.find(frame_id) == set_entries_.end()) {
    return;
  }
  auto &frame_entry = set_entries_[frame_id];
  if (!frame_entry.is_evictable_) {
    return;
  }
  if (frame_entry.access_count < k_) {
    history_list_.erase(frame_entry.position_);
  } else {
    cache_list_.erase(frame_entry.position_);
  }
  curr_size_--;
  set_entries_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
