//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  max_size_ = max_size;
  size_ = 0;
  for (size_t i = 0; i < max_size_; ++i) {
    array_[i] = {K{-1}, V{}};
  }
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::GetAndDelAll(std::vector<std::pair<K, V>> &vec, const KC &cmp) -> void {
  for (size_t i = 0; i < size_; ++i) {
    vec.push_back(array_[i]);
    array_[i] = {K{-1}, V{}};
  }
  size_ = 0;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for (size_t i = 0; i < size_; ++i) {
    // std::cout<<"loo up " << "key is " << key << ", value is " << value <<std::endl;
    if (cmp(key, array_[i].first) == 0) {
      value = array_[i].second;
      // std::cout << "look up success" << std::endl;
      return true;
    }
  }
  // std::cout << "look up failed" << std::endl;
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  // std::cout << &array_[0].first << " " << array_[0].first << std::endl;
  if (size_ == max_size_) {
    // std::cout << "bucket is full" << std::endl;
    return false;
  }
  for (size_t i = 0; i < size_; ++i) {
    if (cmp(key, array_[i].first) == 0) {
      // std::cout << "Insert  failed" << std::endl;
      return false;
    }
  }
  array_[size_] = {key, value};
  size_++;
  // std::cout << "Insert success, key is " << key << "value is" << value << std::endl;
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  for (size_t i = 0; i < size_; ++i) {
    // std::cout << "map[i].key is " << array_[i].first << std::endl;
    if (cmp(key, array_[i].first) == 0) {
      array_[i] = {K{-1}, V{}};
      std::swap(array_[i], array_[size_ - 1]);
      // std::cout << "Remove success, key is " << key << std::endl;
      size_--;
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  array_[bucket_idx] = {K{-1}, V{}};
  std::swap(array_[bucket_idx], array_[size_ - 1]);
  size_--;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return (size_ == max_size_);
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return (size_ == 0);
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub