
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)) {}

void SeqScanExecutor::Init() { it_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!it_->IsEnd()) {
    *rid = it_->GetRID();
    auto [tuple_meta, tmp_tuple] = it_->GetTuple();
    ++(*it_);
    bool ok = !tuple_meta.is_deleted_;
    if (ok && plan_->filter_predicate_) {
      auto value = plan_->filter_predicate_->Evaluate(&tmp_tuple, GetOutputSchema());
      ok = !value.IsNull() && value.GetAs<bool>();
    }
    if (ok) {
      *tuple = tmp_tuple;
      return true;
    }
  }
  return false;
}
}  // namespace bustub