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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return false;
  }

  auto p = lru_list_.begin();
  while (p != lru_list_.end()) {
    if (is_evictable_map[*p]) {
      break;
    }
    p++;
  }
  if (p != lru_list_.end()) {
    access_count_map_.erase(*p);
    is_evictable_map.erase(*p);
    *frame_id = *p;
    lru_list_.erase(p);
    curr_size_--;
    return true;
  }

  p = lru_k_list_.begin();
  while (p != lru_k_list_.end()) {
    if (is_evictable_map[*p]) {
      break;
    }
    p++;
  }

  if (p != lru_k_list_.end()) {
    access_count_map_.erase(*p);
    is_evictable_map.erase(*p);
    *frame_id = *p;
    lru_k_list_.erase(p);
    curr_size_--;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  access_count_map_[frame_id]++;

  if (access_count_map_[frame_id] == 1) {
    lru_list_.push_back(frame_id);
  } else if (access_count_map_[frame_id] < k_) {
    lru_list_.remove(frame_id);
    lru_list_.push_back(frame_id);
  } else if (access_count_map_[frame_id] == k_) {
    lru_list_.remove(frame_id);
    lru_k_list_.push_back(frame_id);
  } else {
    lru_k_list_.remove(frame_id);
    lru_k_list_.push_back(frame_id);
  }

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  if (is_evictable_map.find(frame_id) == is_evictable_map.end()) {
    is_evictable_map[frame_id] = set_evictable;
    if (set_evictable) {
      curr_size_++;
    }
  }

  if (!is_evictable_map[frame_id]&& set_evictable) {
    is_evictable_map[frame_id] = true;
    curr_size_++;
  }

  if (is_evictable_map[frame_id] && !set_evictable) {
    is_evictable_map[frame_id] = false;
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  if (!is_evictable_map[frame_id]) {
    return;
  }

  auto access_count = access_count_map_[frame_id];

  if (access_count < k_) {
    lru_list_.remove(frame_id);
  } else if (access_count >= k_) {
    lru_k_list_.remove(frame_id);
  }

  access_count_map_[frame_id]--;
  if (access_count_map_[frame_id] == 0) {
    access_count_map_.erase(frame_id);
  }

  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
