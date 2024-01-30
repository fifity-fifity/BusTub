//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  bpm_->FlushAllPages();
  // std::cout << "Init" << std::endl;
  // std::cout << header_max_depth << " " << directory_max_depth << " " << bucket_max_size << std::endl;
  // std::cout << bpm_->GetPoolSize() << " " << name << std::endl;
  auto header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.template As<ExtendibleHTableHeaderPage>();
  size_t dir_idx = header_page->HashToDirectoryIndex(Hash(key));
  if (static_cast<int>(header_page->GetDirectoryPageId(dir_idx)) == INVALID_PAGE_ID) {
    return false;
  }
  auto dir_guard = bpm_->FetchPageRead(header_page->GetDirectoryPageId(dir_idx));
  header_guard.Drop();
  auto dir_page = dir_guard.template As<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = dir_page->HashToBucketIndex(Hash(key));

  if (dir_page->GetBucketPageId(bucket_idx) == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageRead(dir_page->GetBucketPageId(bucket_idx));
  dir_guard.Drop();
  auto bucket_page = bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  V ans;
  if (bucket_page->Lookup(key, ans, cmp_)) {
    result->push_back(ans);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::vector<V> check;
  if (GetValue(key, &check, transaction)) {
    return false;
  }
  // std::cout << "Insert key is " << key << " , value is " << value << " hash is " << Hash(key) << std::endl;
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.template AsMut<ExtendibleHTableHeaderPage>();
  size_t dir_idx = header_page->HashToDirectoryIndex(Hash(key));
  if (static_cast<int>(header_page->GetDirectoryPageId(dir_idx)) == INVALID_PAGE_ID) {
    page_id_t set_id;
    bpm_->NewPageGuarded(&set_id);
    // std::cout << "now need a new page for dir, the new page_id is " << set_id << std::endl;
    // std::cout << "bucket_idx is " << dir_idx << std::endl;
    header_page->SetDirectoryPageId(dir_idx, set_id);
    auto dir_guard = bpm_->FetchPageWrite(header_page->GetDirectoryPageId(dir_idx));
    auto dir_page = dir_guard.template AsMut<ExtendibleHTableDirectoryPage>();
    dir_page->Init(directory_max_depth_);
  }

  if (static_cast<int>(header_page->GetDirectoryPageId(dir_idx)) == INVALID_PAGE_ID) {
    return false;
  }
  auto dir_guard = bpm_->FetchPageWrite(header_page->GetDirectoryPageId(dir_idx));
  header_guard.Drop();
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = dir_page->HashToBucketIndex(Hash(key));

  if (dir_page->GetBucketPageId(bucket_idx) == INVALID_PAGE_ID) {
    page_id_t set_id;
    bpm_->NewPageGuarded(&set_id);
    // std::cout << "now need a new page for bucket, the new page_id is " << set_id << std::endl;
    // std::cout << "bucket_idx is " << bucket_idx << std::endl;
    dir_page->SetBucketPageId(bucket_idx, set_id);
    auto bucket_guard = bpm_->FetchPageWrite(dir_page->GetBucketPageId(bucket_idx));
    auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(bucket_max_size_);
  }
  if (static_cast<int>(dir_page->GetBucketPageId(bucket_idx)) == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageWrite(dir_page->GetBucketPageId(bucket_idx));
  dir_guard.Drop();
  auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (!bucket_page->IsFull()) {
    bucket_page->Insert(key, value, cmp_);
  } else {
    // std::cout << "page_id " << dir_page->GetBucketPageId(bucket_idx) << " is full" << std::endl;
    std::vector<std::pair<K, V>> vec;
    bucket_page->GetAndDelAll(vec, cmp_);

    page_id_t new_page_id;
    bpm_->NewPageGuarded(&new_page_id);
    auto new_bucket_guard = bpm_->FetchPageWrite(new_page_id);
    auto new_bucket_page = new_bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);

    if (dir_page->GetGlobalDepth() <= dir_page->GetLocalDepth(bucket_idx)) {
      if (dir_page->GetGlobalDepth() < dir_page->GetMaxDepth()) {
        dir_page->IncrGlobalDepth();
      } else {
        // std::cout << "NO MORE GROWTH" << std::endl;
        return false;
      }
    }

    auto tmp_local = dir_page->GetLocalDepth(bucket_idx);
    auto tmp_size = dir_page->Size();

    size_t suf = bucket_idx % (1 << tmp_local);
    size_t add = (1 << tmp_local);

    // std::cout << "reset bucket_page_ids to " << new_page_id << std::endl;
    for (size_t i = suf + add; i < tmp_size; i += add) {
      if (((i - suf) / add & 1) == 1) {
        dir_page->SetBucketPageId(i, new_page_id);
      }
    }
    dir_page->IncrLocalDepth(bucket_idx);
    // std::cout << "vec size is" << vec.size() << std::endl;
    vec.push_back({key, value});
    for (auto &[k, v] : vec) {
      auto insert_bucket_idx = dir_page->HashToBucketIndex(Hash(k));
      if (insert_bucket_idx == bucket_idx) {
        bucket_page->Insert(k, v, cmp_);
      } else {
        new_bucket_page->Insert(k, v, cmp_);
      }
    }
  }

  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // std::cout << "Remove key is " << key << std::endl;
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.template As<ExtendibleHTableHeaderPage>();
  size_t dir_idx = header_page->HashToDirectoryIndex(Hash(key));
  if (static_cast<int>(header_page->GetDirectoryPageId(dir_idx)) == INVALID_PAGE_ID) {
    return false;
  }
  auto dir_guard = bpm_->FetchPageWrite(header_page->GetDirectoryPageId(dir_idx));
  header_guard.Drop();
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = dir_page->HashToBucketIndex(Hash(key));
  if (dir_page->GetBucketPageId(bucket_idx) == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageWrite(dir_page->GetBucketPageId(bucket_idx));
  auto bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  bool res = bucket_page->Remove(key, cmp_);
  if (!res) {
    return res;
  }

  if (bucket_page->IsEmpty()) {
    size_t tmp_local = dir_page->GetLocalDepth(bucket_idx);
    if (tmp_local == 0) {
      page_id_t del_page_id = dir_page->GetBucketPageId(0);
      dir_page->SetBucketPageId(0, -1);
      // std::cout << "del page" << del_page_id << std::endl;
      bpm_->DeletePage(del_page_id);
      return res;
    }
  }
  std::vector<std::pair<K, V>> vec;
  while (bucket_page->IsEmpty()) {
    size_t tmp_local = dir_page->GetLocalDepth(bucket_idx);
    if (tmp_local == 0) {
      break;
    }
    auto tmp_size = dir_page->Size();
    size_t suf = bucket_idx % (1 << (tmp_local - 1));
    size_t add = (1 << (tmp_local - 1));

    if (dir_page->GetLocalDepth(suf) != dir_page->GetLocalDepth(suf + add)) {
      break;
    }
    // page_id_t old_page_id = dir_page->GetBucketPageId(bucket_idx);
    page_id_t keep_page_id = dir_page->GetBucketPageId(suf);
    {
      bucket_guard.Drop();
      page_id_t del_page_id = dir_page->GetBucketPageId(suf + add);
      // std::cout << "now need to reinsert " << del_page_id << std::endl;
      auto del_bucket_guard = bpm_->FetchPageWrite(del_page_id);
      auto del_bucket_page = del_bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      // std::cout << "del_bucket_page size is " << del_bucket_page->Size() << std::endl;
      del_bucket_page->GetAndDelAll(vec, cmp_);
      // std::cout << "we will del page " << del_page_id << std::endl;
      for (size_t i = suf + add; i < tmp_size; i += add) {
        if (((i - suf) / add & 1) == 1) {
          dir_page->SetBucketPageId(i, INVALID_PAGE_ID);
        }
      }
      dir_page->DecrLocalDepth(suf);
      bpm_->DeletePage(del_page_id);
    }
    bucket_guard.Drop();
    bucket_guard = bpm_->FetchPageWrite(keep_page_id);
    bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    // std::cout << "vec.size() is " << vec.size() << std::endl;
  }
  while (dir_page->CanShrink()) {
    // std::cout << "can do Shrink" << std::endl;
    dir_page->DecrGlobalDepth();
  }
  bucket_guard.Drop();
  dir_guard.Drop();
  for (auto &[k, v] : vec) {
    Insert(k, v, transaction);
  }
  return res;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub