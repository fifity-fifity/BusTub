#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void FillKeyExpressions(std::vector<AbstractExpressionRef> &left_key_expressions,
                        std::vector<AbstractExpressionRef> &right_key_expressions,
                        const ColumnValueExpression *left_expr, const ColumnValueExpression *right_expr) {
  if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
    left_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType()));
    right_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(1, right_expr->GetColIdx(), right_expr->GetReturnType()));
  } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
    left_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType()));
    right_key_expressions.emplace_back(
        std::make_shared<ColumnValueExpression>(1, left_expr->GetColIdx(), left_expr->GetReturnType()));
  } else {
    BUSTUB_ASSERT(false, "left_expr or right_expr error");
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    auto tmp_predicate = nlj_plan.Predicate();
    auto *tmp_expr = dynamic_cast<const LogicExpression *>(tmp_predicate.get());
    while (tmp_expr != nullptr) {
      // std::cout << "recusive " << std::endl;
      if (tmp_expr->logic_type_ == LogicType::And) {
        auto *equal_expr = dynamic_cast<ComparisonExpression *>(tmp_expr->children_[1].get());
        if (equal_expr != nullptr && equal_expr->comp_type_ == ComparisonType::Equal) {
          const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(equal_expr->children_[0].get());
          const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(equal_expr->children_[1].get());
          if (left_expr != nullptr && right_expr != nullptr) {
            FillKeyExpressions(left_key_expressions, right_key_expressions, left_expr, right_expr);
          }
        } else {
          return optimized_plan;
        }
        tmp_predicate = tmp_expr->children_[0];
        tmp_expr = dynamic_cast<const LogicExpression *>(tmp_predicate.get());
      } else {
        return optimized_plan;
      }
    }
    auto *equal_expr = dynamic_cast<ComparisonExpression *>(tmp_predicate.get());
    if (equal_expr != nullptr && equal_expr->comp_type_ == ComparisonType::Equal) {
      const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(equal_expr->children_[0].get());
      const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(equal_expr->children_[1].get());
      if (left_expr != nullptr && right_expr != nullptr) {
        FillKeyExpressions(left_key_expressions, right_key_expressions, left_expr, right_expr);
      }
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                std::move(right_key_expressions), nlj_plan.GetJoinType());
    }
    // std::cout << "this is for test" << std::endl;
    // std::cout << nlj_plan.predicate_->ToString()<<std::endl;
  }
  return optimized_plan;
}

}  // namespace bustub