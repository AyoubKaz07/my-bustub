//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class TopNExecutor;

struct TupleComparator {
  const TopNExecutor *executor_;  // Reference to the parent TopNExecutor instance

  TupleComparator(const TopNExecutor *executor) : executor_(executor) {}

  bool operator()(const Tuple &t1, const Tuple &t2) const;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;

  /** The child executor used to retrieve tuples from */
  std::unique_ptr<AbstractExecutor> child_executor_;

  // Custom comparator function for the priority queue
  struct TupleComparator {
    const TopNExecutor *executor_;  // Reference to the parent TopNExecutor instance

    TupleComparator(const TopNExecutor *executor) : executor_(executor) {}

    bool operator()(const Tuple &t1, const Tuple &t2) const {
      for (const auto &[order_by_type, expr] : executor_->plan_->GetOrderBy()) {
        if (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT) {
          if (expr->Evaluate(&t1, executor_->child_executor_->GetOutputSchema())
                  .CompareLessThan(expr->Evaluate(&t2, executor_->child_executor_->GetOutputSchema())) ==
              CmpBool::CmpTrue) {
            return true;
          } else if (expr->Evaluate(&t1, executor_->child_executor_->GetOutputSchema())
                         .CompareGreaterThan(expr->Evaluate(&t2, executor_->child_executor_->GetOutputSchema())) ==
                     CmpBool::CmpTrue) {
            return false;
          }
        }
        if (order_by_type == OrderByType::DESC) {
          if (expr->Evaluate(&t1, executor_->child_executor_->GetOutputSchema())
                  .CompareLessThan(expr->Evaluate(&t2, executor_->child_executor_->GetOutputSchema())) ==
              CmpBool::CmpTrue) {
            return false;
          } else if (expr->Evaluate(&t1, executor_->child_executor_->GetOutputSchema())
                         .CompareGreaterThan(expr->Evaluate(&t2, executor_->child_executor_->GetOutputSchema())) ==
                     CmpBool::CmpTrue) {
            return true;
          }
        }
      }
      return false;
    }
  };

  /** Tuple comparator */
  TupleComparator comparator_;

  /** The topn heap */
  std::priority_queue<Tuple, std::vector<Tuple>, TupleComparator> topNHeap_;

  /** Tuples result (stack)*/
  std::vector<Tuple> sorted_tuples_;
};
}  // namespace bustub
