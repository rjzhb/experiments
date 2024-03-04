#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * ProjectionPlanNode 表示投影操作。
 * 它基于输入计算表达式。
 */
class ProjectionPlanNode : public AbstractPlanNode {
 public:
  /**
   * 构造一个新的 ProjectionPlanNode 实例。
   * @param output 此投影节点的输出模式
   * @param expressions 要评估的表达式
   * @param child 子计划节点
   */
  ProjectionPlanNode(SchemaRef output,
					 std::vector<AbstractExpressionRef> expressions,
					 AbstractPlanNodeRef child)
	  : AbstractPlanNode(std::move(output), {std::move(child)}), expressions_(std::move(expressions)) {}

  /** @return 计划节点的类型 */
  auto GetType() const -> PlanType override { return PlanType::Projection; }

  /** @return 子计划节点 */
  auto GetChildPlan() const -> AbstractPlanNodeRef {
	BUSTUB_ASSERT(GetChildren().size() == 1, "Projection should have exactly one child plan.");
	return GetChildAt(0);
  }

  /** @return 投影表达式 */
  auto GetExpressions() const -> const std::vector<AbstractExpressionRef> & { return expressions_; }

  static auto InferProjectionSchema(const std::vector<AbstractExpressionRef> &expressions) -> Schema;

  static auto RenameSchema(const Schema &schema, const std::vector<std::string> &col_names) -> Schema;

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(ProjectionPlanNode);

  std::vector<AbstractExpressionRef> expressions_;

 protected:
  auto PlanNodeToString() const -> std::string override;
};

}  // namespace bustub