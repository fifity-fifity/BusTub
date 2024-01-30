//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock lk(latch_);
  frame_id_t free_id;
  if (!free_list_.empty()) {
    free_id = free_list_.back();
    free_list_.pop_back();
  } else if (!(*replacer_).Evict(&free_id)) {
    return nullptr;
  }
  *page_id = AllocatePage();
  if (pages_[free_id].is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, pages_[free_id].GetData(), pages_[free_id].GetPageId(), std::move(promise)});
    future.get();
  }
  page_table_.erase(pages_[free_id].page_id_);
  page_table_[*page_id] = (free_id);

  pages_[free_id].ResetMemory();
  pages_[free_id].page_id_ = *page_id;
  pages_[free_id].is_dirty_ = false;
  pages_[free_id].pin_count_ = 1;

  (*replacer_).RecordAccess(free_id);
  (*replacer_).SetEvictable(free_id, false);
  return &pages_[free_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock lk(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    pages_[page_table_[page_id]].pin_count_++;
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    return &pages_[page_table_[page_id]];
  }
  frame_id_t free_id;
  if (!free_list_.empty()) {
    free_id = free_list_.back();
    free_list_.pop_back();
  } else if (!(*replacer_).Evict(&free_id)) {
    return nullptr;
  }
  if (pages_[free_id].is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, pages_[free_id].GetData(), pages_[free_id].GetPageId(), std::move(promise)});
    future.get();
  }
  page_table_.erase(pages_[free_id].page_id_);
  page_table_[page_id] = (free_id);
  pages_[free_id].ResetMemory();
  pages_[free_id].page_id_ = page_id;
  pages_[free_id].is_dirty_ = false;
  pages_[free_id].pin_count_ = 1;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, pages_[free_id].GetData(), pages_[free_id].GetPageId(), std::move(promise)});
  future.get();
  (*replacer_).RecordAccess(free_id);
  (*replacer_).SetEvictable(free_id, false);
  return &pages_[free_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock lk(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  if (pages_[page_table_[page_id]].pin_count_ == 0) {
    return false;
  }
  pages_[page_table_[page_id]].pin_count_--;
  if (pages_[page_table_[page_id]].pin_count_ <= 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  if (is_dirty) {
    pages_[page_table_[page_id]].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock lk(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, pages_[page_table_[page_id]].data_, page_id, std::move(promise)});
  future.get();
  pages_[page_table_[page_id]].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock lk(latch_);
  for (auto &[page_id, frame_id] : page_table_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule(
        {true, pages_[page_table_[page_id]].data_, pages_[page_table_[page_id]].GetPageId(), std::move(promise)});
    future.get();
    pages_[page_table_[page_id]].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock lk(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto id = page_table_[page_id];
  auto &page = pages_[id];
  if (page.GetPinCount() > 0) {
    return false;
  }
  if (page.IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule(
        {true, pages_[page_table_[page_id]].data_, pages_[page_table_[page_id]].GetPageId(), std::move(promise)});
    future.get();
  }
  page.ResetMemory();
  DeallocatePage(page_id);
  page_table_.erase(page_id);
  replacer_->Remove(id);
  free_list_.push_back(id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // std::cout << "now fetching" << std::endl;
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub