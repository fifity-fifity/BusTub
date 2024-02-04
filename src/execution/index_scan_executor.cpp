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
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  // std::cout << plan_->output_schema_->GetColumnCount() << std::endl;
  std::vector<Value> values;
  values.emplace_back(INTEGER, std::stoi(plan_->pred_key_->val_.ToString()));
  std::vector<Column> v;
  v.emplace_back("KEY", INTEGER);
  Schema tmp_schema(v);
  std::vector<RID> result;
  htable_->ScanKey(Tuple(values, &tmp_schema), &result, nullptr);
  // std::cout << "no" << std::endl;
  if (result.empty()) {
    return false;
  }
  *rid = *result.data();
  auto [tuple_meta, tmp_tuple] = table_info_->table_->GetTuple(*rid);
  *tuple = tmp_tuple;
  // std::cout << tuple_meta.is_deleted_ << std::endl;
  // std::cout << tmp_tuple.ToString(&GetOutputSchema()) << std::endl;
  // std::cout << tuple->ToString(&GetOutputSchema()) << std::endl;
  finished_ = true;
  return !tuple_meta.is_deleted_;
}

}  // namespace bustub