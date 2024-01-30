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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, uint64_t k)
    : node_store_(num_frames), replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  uint64_t time_k = std::numeric_limits<uint64_t>::max();
  for (size_t i = 0; i < replacer_size_; ++i) {
    auto &node = node_store_[i];
    if (node.is_evictable_ && node.history_.size() < k_ && node.fid_ != INVALID_TXN_ID) {
      if (time_k > node.history_.front()) {
        time_k = node.history_.front();
        *frame_id = i;
      }
    }
  }
  if (time_k == std::numeric_limits<uint64_t>::max()) {
    for (size_t i = 0; i < replacer_size_; ++i) {
      auto &node = node_store_[i];
      if (node.is_evictable_ && node.history_.size() >= k_ && node.fid_ != INVALID_TXN_ID) {
        if (time_k > node.history_.front()) {
          time_k = node.history_.front();
          *frame_id = i;
        }
      }
    }
  }
  if (time_k != std::numeric_limits<uint64_t>::max()) {
    node_store_[*frame_id].history_.clear();
    node_store_[*frame_id].fid_ = INVALID_TXN_ID;
    node_store_[*frame_id].k_ = 0;
    node_store_[*frame_id].is_evictable_ = false;
    curr_size_--;
  }
  return (time_k != std::numeric_limits<uint64_t>::max());
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    BUSTUB_ASSERT(true, "RecordAccess error");
  }
  current_timestamp_++;
  if (node_store_[frame_id].fid_ != INVALID_TXN_ID) {
    node_store_[frame_id].history_.push_back(current_timestamp_);
    node_store_[frame_id].k_++;
    if (node_store_[frame_id].history_.size() > k_) {
      node_store_[frame_id].history_.pop_front();
    }
  } else {
    node_store_[frame_id].fid_ = frame_id;
    node_store_[frame_id].k_ = 1;
    node_store_[frame_id].history_.push_back(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    std::abort();
  }
  if (node_store_[frame_id].fid_ == INVALID_TXN_ID) {
    return;
  }
  if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    curr_size_++;
  } else if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    curr_size_--;
  }
  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    return;
  }
  if (node_store_[frame_id].fid_ == INVALID_TXN_ID) {
    return;
  }
  if (!node_store_[frame_id].is_evictable_) {
    BUSTUB_ASSERT(true, "is non-evictable");
  } else {
    node_store_[frame_id].history_.clear();
    node_store_[frame_id].fid_ = INVALID_TXN_ID;
    node_store_[frame_id].k_ = 0;
    node_store_[frame_id].is_evictable_ = false;
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock lock(latch_);
  return curr_size_;
}

}  // namespace bustub