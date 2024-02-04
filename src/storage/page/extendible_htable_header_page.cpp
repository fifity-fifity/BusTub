//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  for (int &directory_page_id : directory_page_ids_) {
    directory_page_id = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  if (max_depth_ == 0) {
    // std::cout << "UB, max_depth is " << max_depth_ << std::endl;
    return 0;
  }
  // std::cout << max_depth_ << " " << ((hash >> (32 - max_depth_))) << std::endl;
  return (hash >> (32 - max_depth_));
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  // std::cout << "warning!! you are setting DirctoryPageID" << std::endl;
  directory_page_ids_[directory_idx] = directory_page_id;
  // std::cout << directory_page_ids_[directory_idx] << std::endl;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t {
  return std::min(static_cast<int>(HTABLE_HEADER_ARRAY_SIZE), (1 << max_depth_));
}

}  // namespace bustub