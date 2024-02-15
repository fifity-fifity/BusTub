#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
    std::cout << "tuple.ToString() " << tuple.ToString(&child_executor_->GetOutputSchema()) << std::endl;
    // std::cout << GetOutputSchema().ToString() << std::endl;
    // std::cout << child_executor_->GetOutputSchema().ToString() << std::endl;
    // std::cout << " " << tuple.GetLength() << std::endl;
  }

  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by;
  for (const auto &window_function : plan_->window_functions_) {
    if (!window_function.second.order_by_.empty()) {
      order_by = window_function.second.order_by_;
    }
  }

  std::sort(tuples_.begin(), tuples_.end(), [this, &order_by](const Tuple &left_tuple, const Tuple &right_tuple) {
    for (const auto &[type, expr] : order_by) {
      auto left_value = expr->Evaluate(&left_tuple, this->child_executor_->GetOutputSchema());
      auto right_value = expr->Evaluate(&right_tuple, this->child_executor_->GetOutputSchema());
      if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
        return type != OrderByType::DESC;
      }
      if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    // BUSTUB_ASSERT(false, "duplicated tuple key");
    return true;
  });

  std::vector<std::vector<Value>> vals(tuples_.size());

  if (!order_by.empty()) {
    for (auto window_function : plan_->window_functions_) {
      std::unordered_map<std::string, std::vector<Value>> mp;
      for (auto &tmp_tule : tuples_) {
        if (!window_function.second.partition_by_.empty()) {
          auto val = window_function.second.partition_by_[0]->Evaluate(&tmp_tule, child_executor_->GetOutputSchema());
          mp[val.ToString()].push_back(
              window_function.second.function_->Evaluate(&tmp_tule, child_executor_->GetOutputSchema()));
        } else {
          mp["null"].push_back(
              window_function.second.function_->Evaluate(&tmp_tule, child_executor_->GetOutputSchema()));
        }
      }
      switch (window_function.second.type_) {
        case WindowFunctionType::CountStarAggregate: {
          std::unordered_map<std::string, int> tmp_mp;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            auto val_tofind =
                window_function.second.function_->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }
            tmp_mp[key]++;
            vals[i].push_back(ValueFactory::GetIntegerValue(tmp_mp[key]));
          }
          break;
        }
        case WindowFunctionType::CountAggregate: {
          std::unordered_map<std::string, int> tmp_mp;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            auto val_tofind =
                window_function.second.function_->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }
            tmp_mp[key]++;
            vals[i].push_back(ValueFactory::GetIntegerValue(tmp_mp[key]));
          }
          break;
        }
        case WindowFunctionType::SumAggregate: {
          std::unordered_map<std::string, int> tmp_mp;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            auto val_tofind =
                window_function.second.function_->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }
            tmp_mp[key] += val_tofind.GetAs<int>();
            std::cout << "tmp_mp[key] is" << tmp_mp[key] << std::endl;
            vals[i].push_back(ValueFactory::GetIntegerValue(tmp_mp[key]));
          }
          break;
        }
        case WindowFunctionType::MinAggregate: {
          std::unordered_map<std::string, int> tmp_mp;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            auto val_tofind =
                window_function.second.function_->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }
            if (tmp_mp.find(key) == tmp_mp.end()) {
              tmp_mp[key] = 1e8;
            }
            tmp_mp[key] = std::min(tmp_mp[key], val_tofind.GetAs<int>());
            vals[i].push_back(ValueFactory::GetIntegerValue(tmp_mp[key]));
          }
          break;
        }
        case WindowFunctionType::MaxAggregate: {
          std::unordered_map<std::string, int> tmp_mp;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            auto val_tofind =
                window_function.second.function_->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }
            if (tmp_mp.find(key) == tmp_mp.end()) {
              tmp_mp[key] = -1e8;
            }
            tmp_mp[key] = std::max(tmp_mp[key], val_tofind.GetAs<int>());
            vals[i].push_back(ValueFactory::GetIntegerValue(tmp_mp[key]));
          }
          break;
        }
        case WindowFunctionType::Rank: {
          int rank = 1;
          std::unordered_map<std::string, int> tmp_mp;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            // std::cout << "?? " << window_function.second.function_->ToString() << std::endl;
            auto val_tofind = plan_->columns_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            std::string key = val_tofind.ToString();
            std::cout << "key is " << key << std::endl;
            if (tmp_mp.find(key) == tmp_mp.end()) {
              tmp_mp[key] = rank;
            }
            rank++;
            vals[i].push_back(ValueFactory::GetIntegerValue(tmp_mp[key]));
          }
          break;
        }
        default: {
          for (size_t i = 0; i < tuples_.size(); ++i) {
            auto val_tofind = plan_->columns_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            vals[i].push_back(val_tofind);
          }
          std::cout << "default" << std::endl;
          break;
        }
      }
    }
  } else {
    for (auto window_function : plan_->window_functions_) {
      std::unordered_map<std::string, std::vector<Value>> mp;
      for (auto &tmp_tule : tuples_) {
        if (!window_function.second.partition_by_.empty()) {
          auto val = window_function.second.partition_by_[0]->Evaluate(&tmp_tule, child_executor_->GetOutputSchema());
          mp[val.ToString()].push_back(
              window_function.second.function_->Evaluate(&tmp_tule, child_executor_->GetOutputSchema()));
        } else {
          mp["null"].push_back(
              window_function.second.function_->Evaluate(&tmp_tule, child_executor_->GetOutputSchema()));
        }
      }
      switch (window_function.second.type_) {
        case WindowFunctionType::CountStarAggregate: {
          // size_t count = 1;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            auto val_tofind =
                window_function.second.function_->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }
            vals[i].push_back(ValueFactory::GetIntegerValue(static_cast<int>(mp[key].size())));
          }
          break;
        }
        case WindowFunctionType::CountAggregate: {
          // size_t count = 1;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            auto val_tofind =
                window_function.second.function_->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }
            vals[i].push_back(ValueFactory::GetIntegerValue(static_cast<int>(mp[key].size())));
          }
          break;
        }
        case WindowFunctionType::SumAggregate: {
          std::unordered_map<std::string, Value> tmp_mp;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            if (!window_function.second.partition_by_.empty()) {
              auto expr = window_function.second.partition_by_[0];
              auto val = expr->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }
            if (tmp_mp.find(key) == tmp_mp.end()) {
              int sum = 0;
              for (const auto &x : mp[key]) {
                sum += x.GetAs<int>();
              }
              std::cout << "sum is " << sum << std::endl;
              tmp_mp[key] = (ValueFactory::GetIntegerValue(sum));
            }
            vals[i].push_back(tmp_mp[key]);
          }
          break;
        }
        case WindowFunctionType::MinAggregate: {
          std::unordered_map<std::string, Value> tmp_mp;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }

            if (tmp_mp.find(key) == tmp_mp.end()) {
              int mi = 1e8;
              for (const auto &x : mp[key]) {
                mi = std::min(mi, x.GetAs<int>());
              }
              std::cout << "mi is " << mi << std::endl;
              tmp_mp[key] = ValueFactory::GetIntegerValue(mi);
            }
            vals[i].push_back(tmp_mp[key]);
          }
          break;
        }
        case WindowFunctionType::MaxAggregate: {
          std::unordered_map<std::string, Value> tmp_mp;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }

            if (tmp_mp.find(key) == tmp_mp.end()) {
              int mx = -1e8;
              for (const auto &x : mp[key]) {
                mx = std::max(mx, x.GetAs<int>());
              }
              std::cout << "mx is " << mx << std::endl;
              tmp_mp[key] = ValueFactory::GetIntegerValue(mx);
            }
            vals[i].push_back(tmp_mp[key]);
          }
          break;
        }
        case WindowFunctionType::Rank: {
          // size_t count = 1;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string key = "null";
            auto val_tofind =
                window_function.second.function_->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              key = val.ToString();
            }
            for (size_t j = 0; j < mp[key].size(); ++j) {
              if (mp[key][j].CompareEquals(val_tofind) == CmpBool::CmpTrue) {
                vals[i].push_back(ValueFactory::GetIntegerValue(static_cast<int>(j) + 1));
              }
            }
          }
          break;
        }
      }
    }
  }
  for (auto &x : plan_->columns_) {
    std::cout << x->ToString() << std::endl;
    if (x->ToString() != "#0.4294967295") {
      for (size_t i = 0; i < tuples_.size(); ++i) {
        vals[i].push_back(x->Evaluate(&tuples_[i], child_executor_->GetOutputSchema()));
      }
    }
  }
  std::cout << vals[0].size() << " " << GetOutputSchema().GetColumnCount() << std::endl;
  for (auto &x : vals) {
    std::reverse(x.begin(), x.end());
    res_tuples_.emplace_back(x, &GetOutputSchema());
  }
  res_it_ = res_tuples_.begin();
  std::cout << std::endl;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (res_it_ != res_tuples_.end()) {
    *tuple = *(res_it_++);
    return true;
  }
  return false;
}

}  // namespace bustub
