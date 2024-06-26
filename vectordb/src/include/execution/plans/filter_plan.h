#pragma once

#include <memory>
#include <string>
#include <utility>

#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace vdbms {

/**
 * The FilterPlanNode represents a filter operation.
 * It retains any tuple that satisfies the predicate in the child.
 */
class FilterPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new FilterPlanNode instance.
   * @param output The output schema of this filter plan node
   * @param predicate The predicate applied during the scan operation
   * @param child The child plan node
   */
  FilterPlanNode(SchemaRef output,
				 AbstractExpressionRef predicate,
				 AbstractPlanNodeRef child)
      : AbstractPlanNode(std::move(output), {std::move(child)}), predicate_{std::move(predicate)} {}

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Filter; }

  /** @return The predicate to test tuples against; tuples should only be returned if they evaluate to true */
  auto GetPredicate() const -> const AbstractExpressionRef & { return predicate_; }

  /** @return The child plan node */
  auto GetChildPlan() const -> AbstractPlanNodeRef {
    vdbms_ASSERT(GetChildren().size() == 1, "Filter should have exactly one child plan.");
    return GetChildAt(0);
  }

  vdbms_PLAN_NODE_CLONE_WITH_CHILDREN(FilterPlanNode);

  /** The predicate that all returned tuples must satisfy */
  AbstractExpressionRef predicate_;

 protected:
  auto PlanNodeToString() const -> std::string override {
    return fmt::format("Filter {{ predicate={} }}", *predicate_);
  }
};

}  // namespace vdbms
