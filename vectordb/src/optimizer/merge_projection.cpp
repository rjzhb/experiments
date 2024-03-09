#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

// 定义Optimizer类中的一个方法，用于优化合并投影操作
auto Optimizer::OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  // 遍历当前计划节点的所有子节点，并对它们递归地应用优化合并投影操作
  for (const auto &child : plan->GetChildren()) {
	children.emplace_back(OptimizeMergeProjection(child));
  }
  // 使用优化后的子节点创建当前节点的克隆
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 如果当前节点是投影操作
  if (optimized_plan->GetType() == PlanType::Projection) {
	const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
	// 确保投影节点只有一个子节点
	BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Projection with multiple children?? That's weird!");
	// 如果子节点的输出模式和投影节点的输出模式相同（忽略列名）
	const auto &child_plan = optimized_plan->children_[0];
	const auto &child_schema = child_plan->OutputSchema();
	const auto &projection_schema = projection_plan.OutputSchema();
	const auto &child_columns = child_schema.GetColumns();
	const auto &projection_columns = projection_schema.GetColumns();
	// 比较子节点和投影节点的列类型是否完全相同
	if (std::equal(child_columns.begin(), child_columns.end(), projection_columns.begin(), projection_columns.end(),
				   [](auto &&child_col, auto &&proj_col) {
					 // TODO: 考虑VARCHAR长度的比较
					 return child_col.GetType() == proj_col.GetType();
				   })) {
	  const auto &exprs = projection_plan.GetExpressions();
	  // 检查所有表达式项是否都是列值表达式，并且与其对应的列完全匹配
	  bool is_identical = true;
	  for (size_t idx = 0; idx < exprs.size(); idx++) {
		auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(exprs[idx].get());
		if (column_value_expr != nullptr) {
		  if (column_value_expr->GetTupleIdx() == 0 && column_value_expr->GetColIdx() == idx) {
			continue;
		  }
		}
		is_identical = false;
		break;
	  }
	  // 如果所有项都完全匹配，则可以将当前的投影操作替换为其子节点，并采用投影的输出模式
	  if (is_identical) {
		auto plan = child_plan->CloneWithChildren(child_plan->GetChildren());
		plan->output_schema_ = std::make_shared<Schema>(projection_schema);
		return plan;
	  }
	}
  }
  // 如果不满足优化条件，则返回原计划节点
  return optimized_plan;
}

}  // namespace bustub
