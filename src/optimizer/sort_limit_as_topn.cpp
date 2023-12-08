#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(children);

  if (optimized_plan->GetType() == PlanType::Limit) {
    // Cast the plan to a limit plan node
    const auto &limit_plan = dynamic_cast<LimitPlanNode &>(*optimized_plan);
    if (limit_plan.GetChildPlan()->GetType() == PlanType::Sort) {
      // Cast the child plan to a sort plan node
      auto &sort_plan = dynamic_cast<const SortPlanNode &>(*limit_plan.GetChildPlan());
      // Return a new top-n plan node
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildPlan(), sort_plan.GetOrderBy(),
                                            limit_plan.GetLimit());
    }
  }
  // Return original plan
  return optimized_plan;
}

}  // namespace bustub
