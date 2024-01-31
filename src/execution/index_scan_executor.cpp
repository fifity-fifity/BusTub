//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  // auto index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->table_oid_);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> result;
  htable_->ScanKey(*tuple, &result, 0);
  if (result.empty()) {
    return false;
  }
  rid = result.data();
  auto [tuple_meta, tmp_tuple] = table_info_->table_->GetTuple(*rid);
  // tuple = &tmp_tuple;
  return !tuple_meta.is_deleted_;
}

}  // namespace bustub

