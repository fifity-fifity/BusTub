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
    : node_store_(num_frames), replacer_size_(num_frames), k_(k), latch_(num_frames) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock lock(mutex_);
  count_++;
  if (curr_size_ == 0) {
    return false;
  }
  if (!cold_list_.empty()) {
    for (auto &i : cold_list_) {
      coun_t_++;
      auto &node = node_store_[i];
      if (node.is_evictable_ && node.fid_ != INVALID_TXN_ID) {
        *frame_id = i;
        node_store_[*frame_id].history_.clear();
        node_store_[*frame_id].fid_ = INVALID_TXN_ID;
        node_store_[*frame_id].k_ = 0;
        node_store_[*frame_id].is_evictable_ = false;
        curr_size_--;
        cold_list_.erase(cold_mp_[*frame_id]);
        cold_mp_.erase(*frame_id);
        return true;
      }
    }
  }
  if (!warm_list_.empty()) {
    for (auto &i : warm_list_) {
      coun_t_++;
      auto &lru_node = node_store_[i];
      if (lru_node.is_evictable_ && lru_node.fid_ != INVALID_TXN_ID) {
        *frame_id = i;
        node_store_[*frame_id].history_.clear();
        node_store_[*frame_id].fid_ = INVALID_TXN_ID;
        node_store_[*frame_id].k_ = 0;
        node_store_[*frame_id].is_evictable_ = false;
        warm_list_.erase(warm_mp_[*frame_id]);
        warm_mp_.erase(*frame_id);
        curr_size_--;
        return true;
      }
    }
  }
  uint64_t time_k = std::numeric_limits<uint64_t>::max();
  for (auto &i : hot_list_) {
    coun_t_++;
    auto &node = node_store_[i];
    if (node.is_evictable_ && node.fid_ != INVALID_TXN_ID) {
      if (time_k > node.history_.front()) {
        time_k = node.history_.front();
        *frame_id = i;
      }
    }
  }
  if (time_k != std::numeric_limits<uint64_t>::max()) {
    node_store_[*frame_id].history_.clear();
    node_store_[*frame_id].fid_ = INVALID_TXN_ID;
    node_store_[*frame_id].k_ = 0;
    node_store_[*frame_id].is_evictable_ = false;
    hot_list_.erase(hot_mp_[*frame_id]);
    hot_mp_.erase(*frame_id);
    curr_size_--;
  }
  std::cout << "tmie_k " << time_k << '\n';
  return (time_k != std::numeric_limits<uint64_t>::max());
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    BUSTUB_ASSERT(true, "RecordAccess error");
  }
  std::unique_lock lock(mutex_);
  // std::cout << cold_list.size() + warm_list_.size() + hot_list_.size() << std::endl;
  current_timestamp_++;
  if (node_store_[frame_id].fid_ != INVALID_TXN_ID) {
    node_store_[frame_id].history_.push_back(current_timestamp_);
    node_store_[frame_id].k_++;
    if (node_store_[frame_id].k_ > k_) {
      node_store_[frame_id].history_.pop_front();
    } else if (node_store_[frame_id].k_ == k_) {
      if (cold_mp_.find(frame_id) != cold_mp_.end()) {
        cold_list_.erase(cold_mp_[frame_id]);
        cold_mp_.erase(frame_id);
      } else {
        warm_list_.erase(warm_mp_[frame_id]);
        warm_mp_.erase(frame_id);
      }
      hot_list_.push_back(frame_id);
      hot_mp_[frame_id] = std::prev(hot_list_.end());
    } else if (AccessType::Unknown != access_type && AccessType::Scan != access_type) {
      if (cold_mp_.find(frame_id) != cold_mp_.end()) {
        cold_list_.erase(cold_mp_[frame_id]);
        cold_mp_.erase(frame_id);
        warm_list_.push_front(frame_id);
        warm_mp_[frame_id] = warm_list_.begin();
      }
    }
  } else {
    if (AccessType::Unknown == access_type) {
      cold_list_.push_back(frame_id);
      cold_mp_[frame_id] = std::prev(cold_list_.end());
    } else if (AccessType::Scan == access_type) {
      cold_list_.push_front(frame_id);
      cold_mp_[frame_id] = cold_list_.begin();
      // free_queue_.try_enqueue(frame_id);
    } else {
      warm_list_.push_back(frame_id);
      warm_mp_[frame_id] = std::prev(warm_list_.end());
    }
    node_store_[frame_id].fid_ = frame_id;
    node_store_[frame_id].k_ = 1;
    node_store_[frame_id].history_.push_back(current_timestamp_);
  }
}

void bustub::LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    std::abort();
  }
  std::unique_lock lock(mutex_);
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

void bustub::LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    return;
  }
  std::unique_lock lock(mutex_);
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

auto bustub::LRUKReplacer::Size() -> size_t {
  std::unique_lock lock(mutex_);
  return curr_size_;
}

}  // namespace bustub