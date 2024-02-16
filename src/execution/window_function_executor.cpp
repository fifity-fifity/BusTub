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
  const int top_n = 10;
  child_executor_->Init();
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by;
  for (const auto &window_function : plan_->window_functions_) {
    if (!window_function.second.order_by_.empty()) {
      order_by = window_function.second.order_by_;
    }
  }

  Tuple tuple;
  RID rid;
  auto cmp = [this, &order_by](const Tuple &left_tuple, const Tuple &right_tuple) {
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
  };
  std::unordered_map<std::string, std::vector<Tuple>> part_count;
  while (child_executor_->Next(&tuple, &rid)) {
    std::string key = "null";
    auto it = plan_->window_functions_.begin();
    auto window_function = it->second;
    if (!window_function.partition_by_.empty()) {
      auto val = window_function.partition_by_[0]->Evaluate(&tuple, child_executor_->GetOutputSchema());
      key = val.ToString();
    }
    part_count[key].emplace_back(tuple);
    // td::cout << "tuple.ToString() " << tuple.ToString(&child_executor_->GetOutputSchema()) << std::endl;
  }
  for (auto &[key, vec] : part_count) {
    std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> heap(cmp);
    for (auto &t : vec) {
      heap.push(t);
      if (heap.size() > top_n) {
        heap.pop();
      }
    }
    while (!heap.empty()) {
      tuples_.emplace_back(heap.top());
      heap.pop();
    }
  }
  std::reverse(tuples_.begin(), tuples_.end());
  std::cout << "tuples.size() is " << tuples_.size() << std::endl;
  std::vector<std::vector<Value>> vals(tuples_.size());
  std::vector<size_t> index_vec;

  if (!order_by.empty()) {
    for (auto window_function : plan_->window_functions_) {
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
          std::unordered_map<std::string, int> part_mp;
          // std::priority_queue<int> q;
          for (size_t i = 0; i < tuples_.size(); ++i) {
            std::string fd = "null";
            if (!window_function.second.partition_by_.empty()) {
              auto val =
                  window_function.second.partition_by_[0]->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
              fd = val.ToString();
            }

            if (part_mp.find(fd) == part_mp.end()) {
              rank = 1;
              part_mp[fd] = 1;
            }
            auto val_tofind = order_by[0].second->Evaluate(&tuples_[i], child_executor_->GetOutputSchema());
            /*if (!q.empty() && val_tofind.GetAs<int>() > q.top() && q.size() >= top_n) {
              continue;
            }
            q.push(val_tofind.GetAs<int>());
            if (q.size() > top_n) {
              q.pop();
            }*/
            std::string key = val_tofind.ToString();
            if (tmp_mp.find(key) == tmp_mp.end()) {
              tmp_mp[key] = rank;
            }
            rank++;
            index_vec.push_back(i);
            // std::cout << val_tofind.ToString() << std::endl;
            vals[i].push_back(ValueFactory::GetIntegerValue(tmp_mp[key]));
          }
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
              tmp_mp[key] = ValueFactory::GetIntegerValue(mx);
            }
            vals[i].push_back(tmp_mp[key]);
          }
          break;
        }
        case WindowFunctionType::Rank: {
          break;
        }
      }
    }
  }
  for (auto it = plan_->columns_.rbegin(); it != plan_->columns_.rend(); ++it) {
    auto &x = *it;
    if (x->ToString() != "#0.4294967295") {
      for (auto i : index_vec) {
        vals[i].push_back(x->Evaluate(&tuples_[i], child_executor_->GetOutputSchema()));
      }
    }
  }
  for (auto &x : vals) {
    std::reverse(x.begin(), x.end());
    if (x.size() != GetOutputSchema().GetColumnCount()) {
      continue;
    }
    res_tuples_.emplace_back(x, &GetOutputSchema());
  }
  res_it_ = res_tuples_.begin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (res_it_ != res_tuples_.end()) {
    *tuple = *(res_it_++);
    return true;
  }
  // std::cout << "window_function_executor is finished" << std::endl;
  return false;
}

}  // namespace bustub
