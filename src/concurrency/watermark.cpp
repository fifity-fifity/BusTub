#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  current_reads_[read_ts]++;
  // TODO(fall2023): implement me!
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if (current_reads_.find(read_ts) != current_reads_.end()) {
    current_reads_[read_ts]--;
    if (current_reads_[read_ts] == 0) {
      current_reads_.erase(read_ts);
    }
  }
  if (current_reads_.empty()) {
    watermark_ = commit_ts_;
    return;
  }
  while (current_reads_.find(watermark_) == current_reads_.end()) {
    watermark_++;
  }
}

}  // namespace bustub
