
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "readerwriterqueue/readerwriterqueue.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //    "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
  //    "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::move(r)); }

[[maybe_unused]] void DiskScheduler::Work(std::thread th) { th.join(); }

void DiskScheduler::StartWorkerThread() {
  // size_t pool_size = 64;
  // ThreadPool threadpool(disk_manager_, pool_size);
  while (true) {
    // std::cout << "yes" << std::endl;
    auto task = request_queue_.Get();
    if (!task.has_value()) {
      break;
    }

    // threadpool.task_q_.Put(std::move(task));
    page_id_t frame_id = task->frame_id_;
    if (mp_.find(frame_id) == mp_.end()) {
      mp_[frame_id] = std::make_unique<PageThread>(disk_manager_);
    }
    mp_[frame_id]->q_.Put(std::move(task));
  }
  /*
  for (size_t i = 0; i < pool_size; ++i) {
    threadpool.task_q_.Put(std::nullopt);
  }
   */
  for (auto &pair : mp_) {
    pair.second->stop_ = true;
    pair.second->q_.Put(std::nullopt);
  }

  std::cout << "No, thread map szie is " << mp_.size() << '\n';
}

}  // namespace bustub