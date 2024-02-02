#include "execution/expressions/column_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  // return plan;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    std::cout << "1" << std::endl;
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    // std::cout<<"here filter_predicate is " << seq_scan_plan.filter_predicate_->ToString() << std::endl;
    if (seq_scan_plan.filter_predicate_ == nullptr) {
      std::cout << "filter_predicate is nullptr" << std::endl;
      return optimized_plan;
    }
    if (seq_scan_plan.filter_predicate_->children_.size() < 2) {
      std::cout << "why size < 2" << std::endl;
      return optimized_plan;
    }
    auto column_value_expression_ptr = dynamic_cast<ColumnValueExpression *>
        (seq_scan_plan.filter_predicate_->children_[0].get());
    if (column_value_expression_ptr == nullptr) {
      std::cout << "why nullptr" << std::endl;
      return optimized_plan;
    }
    auto constant_value_expression_ptr = dynamic_cast<ConstantValueExpression *>
        (seq_scan_plan.filter_predicate_->children_[1].get());
    // BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one child");
    if (constant_value_expression_ptr == nullptr) {
      std::cout << "why nullptr" << std::endl;
      return optimized_plan;
    }
    auto col_idx = column_value_expression_ptr->GetColIdx();
    auto check = MatchIndex(seq_scan_plan.table_name_, col_idx);
    if (check != std::nullopt) {
      index_oid_t index_oid = std::get<0>(check.value());
      return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index_oid,
                                                 seq_scan_plan.filter_predicate_, constant_value_expression_ptr);
    }
  }
  std::cout << "2" << std::endl;
  return optimized_plan;
}

}  // namespace bustub
