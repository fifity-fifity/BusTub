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
    : pool_size_(pool_size),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager),
      f_latch_(pool_size) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(i);
    // cv_.notify_one();
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock lock(latch_);
  frame_id_t free_id;
  if (!free_list_.empty()) {
    free_id = free_list_.back();
    free_list_.pop_back();
  } else if (!(*replacer_).Evict(&free_id)) {
    return nullptr;
  }
  /*
  auto timeout = std::chrono::system_clock::now() + std::chrono::milliseconds(1);
  if (cv_.wait_until(lock, timeout, [&] { return !free_list_.empty() || (*replacer_).Evict(&free_id); })) {
    if (!free_list_.empty()) {
      free_id = free_list_.back();
      free_list_.pop_back();
    }
  } else {
    return nullptr;
  } */

  *page_id = AllocatePage();
  // std::unique_lock p_lock(latch_mp_[*page_id]);
  // std::unique_lock p_lock(f_latch_[free_id]);
  // lock.unlock();
  if (pages_[free_id].is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule(
        {true, pages_[free_id].GetData(), pages_[free_id].GetPageId(), std::move(promise), free_id});
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
  std::unique_lock lock(latch_);
  // std::unique_lock lk(mtx_);
  // std::unique_lock lk(mtx_);
  // std::unique_lock p_lock1(latch_mp_[page_id]);
  // std::cout << "FetchPage " << page_id << std::endl;
  if (page_table_.find(page_id) != page_table_.end()) {
    std::unique_lock f_lock(f_latch_[page_table_[page_id]]);
    pages_[page_table_[page_id]].pin_count_++;
    replacer_->RecordAccess(page_table_[page_id], access_type);
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
  /*
  auto timeout = std::chrono::system_clock::now() + std::chrono::milliseconds(1);
  if (cv_.wait_until(lock, timeout, [&] { return !free_list_.empty() || (*replacer_).Evict(&free_id); })) {
    if (!free_list_.empty()) {
      free_id = free_list_.back();
      free_list_.pop_back();
    }
  } else {
    return nullptr;
  } */
  std::unique_lock f_lock(f_latch_[free_id]);
  // std::unique_lock p_lock(latch_mp_[pages_[free_id].page_id_]);

  page_table_.erase(pages_[free_id].page_id_);
  page_table_[page_id] = (free_id);
  (*replacer_).RecordAccess(free_id, access_type);
  (*replacer_).SetEvictable(free_id, false);
  lock.unlock();
  // lk.unlock();
  if (pages_[free_id].is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule(
        {true, pages_[free_id].GetData(), pages_[free_id].GetPageId(), std::move(promise), free_id});
    future.get();
  }
  // lock.unlock();
  pages_[free_id].ResetMemory();
  pages_[free_id].page_id_ = page_id;
  pages_[free_id].is_dirty_ = false;
  pages_[free_id].pin_count_ = 1;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule(
      {false, pages_[free_id].GetData(), pages_[free_id].GetPageId(), std::move(promise), free_id});
  future.get();
  return &pages_[free_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock lock(latch_);
  // std::cout << "UnpinPage " << page_id << std::endl;
  page_id_t frame_id = page_table_[page_id];
  std::unique_lock f_lock(f_latch_[frame_id]);
  // std::unique_lock p_lock(latch_mp_[page_id]);
  // lock.unlock();
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
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::unique_lock lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  page_id_t frame_id = page_table_[page_id];
  std::unique_lock f_lock(f_latch_[frame_id]);
  // std::unique_lock p_lock(latch_mp_[page_id]);
  pages_[page_table_[page_id]].is_dirty_ = false;
  lock.unlock();
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, pages_[page_table_[page_id]].data_, page_id, std::move(promise), frame_id});
  future.get();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock lock(latch_);

  /*std::unique_lock<std::mutex> f_locks[pool_size_];
  for (size_t i = 0; i < static_cast<size_t>(pool_size_); ++i) {
    f_locks[i] = std::unique_lock(f_latch_[i]);
  }*/
  /*int mx = next_page_id_.load();
  std::unique_lock<std::mutex> p_locks[mx];
  for (size_t i = 0; i < static_cast<size_t>(mx); ++i) {
    p_locks[i] = std::unique_lock(latch_mp_[i]);
  }*/
  for (auto &[page_id, frame_id] : page_table_) {
    std::unique_lock f_lock(f_latch_[frame_id]);
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, pages_[page_table_[page_id]].data_, pages_[page_table_[page_id]].GetPageId(),
                               std::move(promise), frame_id});
    future.get();
    pages_[page_table_[page_id]].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto id = page_table_[page_id];
  page_id_t frame_id = id;
  std::unique_lock f_lock(f_latch_[frame_id]);
  // std::unique_lock p_lock(latch_mp_[page_id]);
  // lock.unlock();
  auto &page = pages_[id];
  if (page.GetPinCount() > 0) {
    return false;
  }
  if (page.IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, pages_[page_table_[page_id]].data_, pages_[page_table_[page_id]].GetPageId(),
                               std::move(promise), frame_id});
    future.get();
  }
  page.ResetMemory();
  DeallocatePage(page_id);
  page_table_.erase(page_id);
  replacer_->Remove(id);
  free_list_.push_back(id);
  cv_.notify_one();
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