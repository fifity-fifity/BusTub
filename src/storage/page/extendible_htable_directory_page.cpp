//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  // std::cout << "now init " << sizeof(bucket_page_ids_) << std::endl;
  max_depth_ = max_depth;
  for (int &bucket_page_id : bucket_page_ids_) {
    bucket_page_id = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  // std::cout << "hash is " << hash <<", result is"<< (hash >> (32 - global_depth_) )<< std::endl;
  return (hash % (1 << global_depth_));
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_page_ids_[bucket_idx];
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // std::cout<<"error you are incre GlobalDepth"<<std::endl;
  for (size_t i = 0; i < Size(); ++i) {
    bucket_page_ids_[i + (1 << global_depth_)] = bucket_page_ids_[i];
    local_depths_[i + (1 << global_depth_)] = local_depths_[i];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  for (size_t i = 0; i < Size(); ++i) {
    bucket_page_ids_[i + (1 << global_depth_)] = INVALID_PAGE_ID;
    local_depths_[i + (1 << global_depth_)] = 0;
  }
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (size_t i = 0; i < Size(); ++i) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return (1 << global_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // if (local_depths_[bucket_idx] == global_depth_) {
  //  IncrGlobalDepth();
  // }
  // std::cout<<"error you are incre localDepth"<<std::endl;
  // local_depths_[bucket_idx]++;
  size_t suf = bucket_idx % (1 << local_depths_[bucket_idx]);
  size_t add = (1 << local_depths_[bucket_idx]);
  for (size_t i = suf; i < Size(); i += add) {
    local_depths_[i]++;
  }
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  // local_depths_[bucket_idx]--;
  size_t suf = bucket_idx % (1 << (local_depths_[bucket_idx] - 1));
  size_t add = (1 << (local_depths_[bucket_idx] - 1));
  for (size_t i = suf; i < Size(); i += add) {
    local_depths_[i]--;
  }
}
auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

}  // namespace bustub