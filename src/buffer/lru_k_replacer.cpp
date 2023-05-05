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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    // we look from the back -> front for the frame with largest backward k-distance
    bool found_one = false;
    // first try to evict from history list (+inf) frames
    if (!history_list.empty()){
        for (auto rit = history_list.rbegin() ; rit != history_list.rend() ; ++rit){
            if (set_entries[*rit].is_evictable){
                *frame_id = *rit ;
                history_list.erase(std::next(rit).base());
                found_one = true;
                break;
            }
        }
    }

    if (!found_one && !cache_list.empty()){
        for (auto rit = cache_list.rbegin() ; rit != cache_list.rend() ; ++rit){
            if (set_entries[*rit].is_evictable){
                *frame_id = *rit ;
                cache_list.erase(std::next(rit).base());
                found_one = true;
                break;
            }
        }
    }
    
    if (found_one){
        set_entries.erase(*frame_id);
        curr_size_--;
        return true;
    }
    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    if (frame_id > static_cast<int>(replacer_size_)){
        BUSTUB_ASSERT(false,"INVALID FRAME ID");
    }
    size_t access_count = ++set_entries[frame_id].access_count;
    // new frame
    if (access_count == 1){
        history_list.emplace_front(frame_id);
        set_entries[frame_id].position = history_list.begin();
        curr_size_++;
    }
    else { 
        // move from history_list to cache queue list
        if (access_count == k_){
            history_list.erase(set_entries[frame_id].position);
            cache_list.emplace_front(frame_id);
            set_entries[frame_id].position = cache_list.begin();
        }
        // move to front of the cache queue (LRU)
        else if (access_count > k_){ 
            cache_list.erase(set_entries[frame_id].position);
            cache_list.emplace_front(frame_id);
            set_entries[frame_id].position = cache_list.begin();
        }
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    if (frame_id > static_cast<int>(replacer_size_)){
        BUSTUB_ASSERT(false,"INVALID FRAME ID");
    }
    if (set_entries.find(frame_id) == set_entries.end()){
        return;
    }
    auto& frame_entry = set_entries[frame_id];
    bool old_status = frame_entry.is_evictable ;
    if (old_status && !set_evictable){
        curr_size_--;
    }
    else if (!old_status && set_evictable){
        curr_size_++;
    }
    frame_entry.is_evictable = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    if (frame_id > static_cast<int>(replacer_size_)){
        BUSTUB_ASSERT(false,"INVALID FRAME ID");
    }
    if (set_entries.find(frame_id) == set_entries.end()){
        return;
    }
    auto& frame_entry = set_entries[frame_id];
    if (!frame_entry.is_evictable){
        return;
    }
    if (frame_entry.access_count < k_){
        history_list.erase(frame_entry.position);
    }
    else{
        cache_list.erase(frame_entry.position);
    }
    curr_size_--;
    set_entries.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
