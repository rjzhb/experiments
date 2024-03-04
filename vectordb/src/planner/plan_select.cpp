#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "binder/bound_expression.h"
#include "binder/bound_order_by.h"
#include "binder/bound_statement.h"
#include "binder/bound_table_ref.h"
#include "binder/expressions/bound_constant.h"
#include "binder/statement/insert_statement.h"
#include "binder/statement/select_statement.h"
#include "binder/tokens.h"
#include "catalog/schema.h"
#include "common/enums/statement_type.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/util/string_util.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/values_plan.h"
#include "fmt/format.h"
#include "planner/planner.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * 为 SELECT 语句规划生成计划节点。
 * @param statement 待规划的 SELECT 语句
 * @return 生成的计划节点
 */
auto Planner::PlanSelect(const SelectStatement &statement) -> AbstractPlanNodeRef {
  auto ctx_guard = NewContext();
  if (!statement.ctes_.empty()) {
	ctx_.cte_list_ = &statement.ctes_;
  }

  AbstractPlanNodeRef plan = nullptr;

  switch (statement.table_->type_) {
	case TableReferenceType::EMPTY:
	  plan = std::make_shared<ValuesPlanNode>(
		  std::make_shared<Schema>(std::vector<Column>{}),
		  std::vector<std::vector<AbstractExpressionRef>>{std::vector<AbstractExpressionRef>{}});
	  break;
	default:
	  plan = PlanTableRef(*statement.table_);
	  break;
  }

  if (!statement.where_->IsInvalid()) {
	auto schema = plan->OutputSchema();
	auto [_, expr] = PlanExpression(*statement.where_, {plan});
	plan = std::make_shared<FilterPlanNode>(std::make_shared<Schema>(schema), std::move(expr), std::move(plan));
  }

  bool has_agg = false;
  bool has_window_agg = false;
  // Binder already checked that normal aggregations and window aggregations cannot coexist.
  for (const auto &item : statement.select_list_) {
	if (item->HasAggregation()) {
	  has_agg = true;
	  break;
	}
	if (item->HasWindowFunction()) {
	  has_window_agg = true;
	  break;
	}
  }

  if (has_window_agg) {
	if (!statement.having_->IsInvalid()) {
	  throw Exception("HAVING 对窗口函数的使用暂不支持。");
	}
	if (!statement.group_by_.empty()) {
	  throw Exception("不允许与窗口函数一起使用 GROUP BY 子句。");
	}
	plan = PlanSelectWindow(statement, std::move(plan));
  } else if (!statement.having_->IsInvalid() || !statement.group_by_.empty() || has_agg) {
	// 规划聚合操作
	plan = PlanSelectAgg(statement, std::move(plan));
  } else {
	// 规划普通 SELECT
	std::vector<AbstractExpressionRef> exprs;
	std::vector<std::string> column_names;
	std::vector<AbstractPlanNodeRef> children = {plan};
	for (const auto &item : statement.select_list_) {
	  auto [name, expr] = PlanExpression(*item, {plan});
	  if (name == UNNAMED_COLUMN) {
		name = fmt::format("__unnamed#{}", universal_id_++);
	  }
	  exprs.emplace_back(std::move(expr));
	  column_names.emplace_back(std::move(name));
	}
	plan = std::make_shared<ProjectionPlanNode>(std::make_shared<Schema>(ProjectionPlanNode::RenameSchema(
													ProjectionPlanNode::InferProjectionSchema(exprs), column_names)),
												std::move(exprs), std::move(plan));
  }

  // 规划 DISTINCT 为分组聚合
  if (statement.is_distinct_) {
	auto child = std::move(plan);

	std::vector<AbstractExpressionRef> distinct_exprs;
	size_t col_idx = 0;
	for (const auto &col : child->OutputSchema().GetColumns()) {
	  distinct_exprs.emplace_back(std::make_shared<ColumnValueExpression>(0, col_idx++, col));
	}

	plan = std::make_shared<AggregationPlanNode>(std::make_shared<Schema>(child->OutputSchema()), child,
												 std::move(distinct_exprs), std::vector<AbstractExpressionRef>{},
												 std::vector<AggregationType>{});
  }

  // 规划 ORDER BY
  if (!statement.sort_.empty()) {
	std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys;
	for (const auto &order_by : statement.sort_) {
	  auto [_, expr] = PlanExpression(*order_by->expr_, {plan});
	  auto abstract_expr = std::move(expr);
	  order_bys.emplace_back(std::make_pair(order_by->type_, abstract_expr));
	}
	plan = std::make_shared<SortPlanNode>(std::make_shared<Schema>(plan->OutputSchema()), plan, std::move(order_bys));
  }

  // 规划 LIMIT
  if (!statement.limit_count_->IsInvalid() || !statement.limit_offset_->IsInvalid()) {
	std::optional<size_t> offset = std::nullopt;
	std::optional<size_t> limit = std::nullopt;

	if (!statement.limit_count_->IsInvalid()) {
	  if (statement.limit_count_->type_ == ExpressionType::CONSTANT) {
		const auto &constant_expr = dynamic_cast<BoundConstant &>(*statement.limit_count_);
		const auto val = constant_expr.val_.CastAs(TypeId::INTEGER);
		if (constant_expr.val_.GetTypeId() == TypeId::INTEGER) {
		  limit = std::make_optional(constant_expr.val_.GetAs<int32_t>());
		} else {
		  throw NotImplementedException("LIMIT 子句必须是整数常量。");
		}
	  } else {
		throw NotImplementedException("LIMIT 子句必须是整数常量。");
	  }
	}

	if (!statement.limit_offset_->IsInvalid()) {
	  if (statement.limit_offset_->type_ == ExpressionType::CONSTANT) {
		const auto &constant_expr = dynamic_cast<BoundConstant &>(*statement.limit_offset_);
		const auto val = constant_expr.val_.CastAs(TypeId::INTEGER);
		if (constant_expr.val_.GetTypeId() == TypeId::INTEGER) {
		  offset = std::make_optional(constant_expr.val_.GetAs<int32_t>());
		} else {
		  throw NotImplementedException("OFFSET 子句必须是整数常量。");
		}
	  } else {
		throw NotImplementedException("OFFSET 子句必须是整数常量。");
	  }
	}

	if (offset != std::nullopt) {
	  throw NotImplementedException("OFFSET 子句暂不支持。");
	}

	plan = std::make_shared<LimitPlanNode>(std::make_shared<Schema>(plan->OutputSchema()), plan, *limit);
  }

  return plan;
}

}  // namespace bustub
