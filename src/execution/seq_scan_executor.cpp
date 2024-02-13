
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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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
    if (exec_ctx_->GetTransaction()->GetTransactionId() != tuple_meta.ts_ &&
        tuple_meta.ts_ > exec_ctx_->GetTransaction()->GetReadTs()) {
      // std::cout << "rid is " << rid->ToString() << ", and begin find version" << std::endl;
      auto undolink = exec_ctx_->GetTransactionManager()->GetUndoLink(*rid);
      int log_id;
      txn_id_t pre_txid;
      auto &txn_map = exec_ctx_->GetTransactionManager()->txn_map_;
      std::shared_ptr<Transaction> pre_txrf = nullptr;
      do {
        log_id = undolink->prev_log_idx_;
        pre_txid = undolink->prev_txn_;
        if (txn_map.find(pre_txid) != txn_map.end()) {
          pre_txrf = txn_map[pre_txid];
        } else {
          ok = false;
          // std::cout << "don't find for has't version" << std::endl;
          break;
        }
        auto prev_tuple = ReconstructTuple(&GetOutputSchema(), tmp_tuple, tuple_meta, {pre_txrf->GetUndoLog(log_id)});
        if (prev_tuple.has_value()) {
          tmp_tuple = prev_tuple.value();
          // if (!pre_txrf->GetUndoLog(log_id).is_deleted_)
          ok = !pre_txrf->GetUndoLog(log_id).is_deleted_;
        } else {
          // std::cout << "can't find for is deleted " << std::endl;
          ok = false;
        }
        undolink = pre_txrf->GetUndoLog(log_id).prev_version_;

      } while (pre_txrf->GetUndoLog(log_id).ts_ > exec_ctx_->GetTransaction()->GetReadTs());
    }
    // std::cout << "RID is " << rid->ToString() << ", ok is " << ok << std::endl;
    if (ok) {
      *tuple = tmp_tuple;
      return true;
    }
  }
  return false;
}
}  // namespace bustub