#include <algorithm>
#include <memory>
#include <unordered_map>

#include "binder/bound_expression.h"
#include "binder/statement/delete_statement.h"
#include "binder/statement/insert_statement.h"
#include "binder/statement/select_statement.h"
#include "binder/statement/update_statement.h"
#include "binder/tokens.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/update_plan.h"
#include "execution/plans/values_plan.h"
#include "planner/planner.h"
#include "type/type_id.h"

namespace vdbms {

auto Planner::PlanInsert(const InsertStatement &statement) -> AbstractPlanNodeRef {
  // 规划插入操作
  auto select = PlanSelect(*statement.select_);

  // 检查表模式是否匹配
  const auto &table_schema = statement.table_->schema_.GetColumns();
  const auto &child_schema = select->OutputSchema().GetColumns();
  if (!std::equal(table_schema.cbegin(), table_schema.cend(), child_schema.cbegin(), child_schema.cend(),
				  [](auto &&col1, auto &&col2) { return col1.GetType() == col2.GetType(); })) {
	throw vdbms::Exception("table schema mismatch");
  }

  // 创建插入操作的模式
  auto insert_schema = std::make_shared<Schema>(std::vector{Column("__vdbms_internal.insert_rows", TypeId::INTEGER)});

  // 返回插入操作的执行计划节点
  return std::make_shared<InsertPlanNode>(std::move(insert_schema), std::move(select), statement.table_->oid_);
}

auto Planner::PlanDelete(const DeleteStatement &statement) -> AbstractPlanNodeRef {
  // 规划删除操作
  auto table = PlanTableRef(*statement.table_);
  auto [_, condition] = PlanExpression(*statement.expr_, {table});
  auto filter = std::make_shared<FilterPlanNode>(table->output_schema_, std::move(condition), std::move(table));
  auto delete_schema = std::make_shared<Schema>(std::vector{Column("__vdbms_internal.delete_rows", TypeId::INTEGER)});

  // 返回删除操作的执行计划节点
  return std::make_shared<DeletePlanNode>(std::move(delete_schema), std::move(filter), statement.table_->oid_);
}

auto Planner::PlanUpdate(const UpdateStatement &statement) -> AbstractPlanNodeRef {
  // 规划更新操作
  auto table = PlanTableRef(*statement.table_);
  auto [_, condition] = PlanExpression(*statement.filter_expr_, {table});
  AbstractPlanNodeRef filter =
	  std::make_shared<FilterPlanNode>(table->output_schema_, std::move(condition), std::move(table));

  auto scope = std::vector{filter};

  std::vector<AbstractExpressionRef> target_exprs;
  target_exprs.resize(filter->output_schema_->GetColumnCount());

  // 遍历更新目标表达式
  for (const auto &[col, target_expr] : statement.target_expr_) {
	// 规划目标表达式
	auto [_1, target_abstract_expr] = PlanExpression(*target_expr, scope);
	auto [_2, col_abstract_expr] = PlanColumnRef(*col, scope);
	target_exprs[col_abstract_expr->GetColIdx()] = std::move(target_abstract_expr);
  }

  // 将未指定的目标表达式填充为列值表达式
  for (size_t idx = 0; idx < target_exprs.size(); idx++) {
	if (target_exprs[idx] == nullptr) {
	  target_exprs[idx] = std::make_shared<ColumnValueExpression>(0, idx, filter->output_schema_->GetColumn(idx));
	}
  }

  // 创建更新操作的模式
  auto update_schema = std::make_shared<Schema>(std::vector{Column("__vdbms_internal.update_rows", TypeId::INTEGER)});

  // 返回更新操作的执行计划节点
  return std::make_shared<UpdatePlanNode>(std::move(update_schema), std::move(filter), statement.table_->oid_,
										  std::move(target_exprs));
}

}  // namespace vdbms

