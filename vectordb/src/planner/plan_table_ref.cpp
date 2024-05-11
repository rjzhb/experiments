#include <memory>
#include <utility>

#include "binder/bound_order_by.h"
#include "binder/bound_statement.h"
#include "binder/bound_table_ref.h"
#include "binder/statement/select_statement.h"
#include "binder/table_ref/bound_base_table_ref.h"
#include "binder/table_ref/bound_cross_product_ref.h"
#include "binder/table_ref/bound_cte_ref.h"
#include "binder/table_ref/bound_expression_list_ref.h"
#include "binder/table_ref/bound_join_ref.h"
#include "binder/table_ref/bound_subquery_ref.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/util/string_util.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "planner/planner.h"
#include "type/value_factory.h"

namespace vdbms {

// NOLINTNEXTLINE - weird error on clang-tidy.
auto Planner::PlanTableRef(const BoundTableRef &table_ref) -> AbstractPlanNodeRef {
  switch (table_ref.type_) {
	// 处理基本表引用
	case TableReferenceType::BASE_TABLE: {
	  const auto &base_table_ref = dynamic_cast<const BoundBaseTableRef &>(table_ref);
	  return PlanBaseTableRef(base_table_ref);
	}
	  // 处理交叉连接引用
	case TableReferenceType::CROSS_PRODUCT: {
	  const auto &cross_product = dynamic_cast<const BoundCrossProductRef &>(table_ref);
	  return PlanCrossProductRef(cross_product);
	}
	  // 处理连接引用
	case TableReferenceType::JOIN: {
	  const auto &join = dynamic_cast<const BoundJoinRef &>(table_ref);
	  return PlanJoinRef(join);
	}
	  // 处理表达式列表引用
	case TableReferenceType::EXPRESSION_LIST: {
	  const auto &expression_list = dynamic_cast<const BoundExpressionListRef &>(table_ref);
	  return PlanExpressionListRef(expression_list);
	}
	  // 处理子查询引用
	case TableReferenceType::SUBQUERY: {
	  const auto &subquery = dynamic_cast<const BoundSubqueryRef &>(table_ref);
	  return PlanSubquery(subquery, subquery.alias_);
	}
	  // 处理CTE引用
	case TableReferenceType::CTE: {
	  const auto &cte = dynamic_cast<const BoundCTERef &>(table_ref);
	  return PlanCTERef(cte);
	}
	default:
	  break;
  }
  throw Exception(fmt::format("the table ref type {} is not supported in planner yet", table_ref.type_));
}

// 计划子查询引用的逻辑
auto Planner::PlanSubquery(const BoundSubqueryRef &table_ref, const std::string &alias) -> AbstractPlanNodeRef {
  auto select_node = PlanSelect(*table_ref.subquery_);
  std::vector<std::string> output_column_names;
  std::vector<AbstractExpressionRef> exprs;
  size_t idx = 0;

  // 重命名列并添加到新的ProjectionPlanNode
  for (const auto &col : select_node->OutputSchema().GetColumns()) {
	auto expr = std::make_shared<ColumnValueExpression>(0, idx, col);
	output_column_names.emplace_back(fmt::format("{}.{}", alias, fmt::join(table_ref.select_list_name_[idx], ".")));
	exprs.push_back(std::move(expr));
	idx++;
  }

  auto saved_child = std::move(select_node);

  return std::make_shared<ProjectionPlanNode>(
	  std::make_shared<Schema>(
		  ProjectionPlanNode::RenameSchema(ProjectionPlanNode::InferProjectionSchema(exprs), output_column_names)),
	  std::move(exprs), saved_child);
}

// 计划基本表引用的逻辑
auto Planner::PlanBaseTableRef(const BoundBaseTableRef &table_ref) -> AbstractPlanNodeRef {
  // 始终扫描表的所有列，并使用投影执行器删除其中的一些列，从而简化规划过程
  auto table = catalog_.GetTable(table_ref.table_);
  vdbms_ASSERT(table, "table not found");

  // 如果是模拟表，则计划为MockScanPlanNode
  if (StringUtil::StartsWith(table->name_, "__")) {
	if (StringUtil::StartsWith(table->name_, "__mock")) {
	  return std::make_shared<MockScanPlanNode>(std::make_shared<Schema>(SeqScanPlanNode::InferScanSchema(table_ref)),
												table->name_);
	}
	throw vdbms::Exception(fmt::format("unsupported internal table: {}", table->name_));
  }
  // 否则，计划为正常的SeqScanPlanNode
  return std::make_shared<SeqScanPlanNode>(std::make_shared<Schema>(SeqScanPlanNode::InferScanSchema(table_ref)),
										   table->oid_, table->name_);
}

// 计划交叉连接引用的逻辑
auto Planner::PlanCrossProductRef(const BoundCrossProductRef &table_ref) -> AbstractPlanNodeRef {
  auto left = PlanTableRef(*table_ref.left_);
  auto right = PlanTableRef(*table_ref.right_);
  // 返回NestedLoopJoinPlanNode以表示交叉连接
  return std::make_shared<NestedLoopJoinPlanNode>(
	  std::make_shared<Schema>(NestedLoopJoinPlanNode::InferJoinSchema(*left, *right)), std::move(left),
	  std::move(right), std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)),
	  JoinType::INNER);
}

// 计划CTE引用的逻辑
auto Planner::PlanCTERef(const BoundCTERef &table_ref) -> AbstractPlanNodeRef {
  // 遍历CTE列表，找到匹配的CTE，然后执行子查询
  for (const auto &cte : *ctx_.cte_list_) {
	if (cte->alias_ == table_ref.cte_name_) {
	  return PlanSubquery(*cte, table_ref.alias_);
	}
  }
  UNREACHABLE("CTE not found");
}

// 计划连接引用的逻辑
auto Planner::PlanJoinRef(const BoundJoinRef &table_ref) -> AbstractPlanNodeRef {
  auto left = PlanTableRef(*table_ref.left_);
  auto right = PlanTableRef(*table_ref.right_);
  auto [_, join_condition] = PlanExpression(*table_ref.condition_, {left, right});
  // 返回NestedLoopJoinPlanNode以表示连接
  auto nlj_node = std::make_shared<NestedLoopJoinPlanNode>(
	  std::make_shared<Schema>(NestedLoopJoinPlanNode::InferJoinSchema(*left, *right)), std::move(left),
	  std::move(right), std::move(join_condition), table_ref.join_type_);
  return nlj_node;
}

// 计划表达式列表引用的逻辑
auto Planner::PlanExpressionListRef(const BoundExpressionListRef &table_ref) -> AbstractPlanNodeRef {
  std::vector<std::vector<AbstractExpressionRef>> all_exprs;
  for (const auto &row : table_ref.values_) {
	std::vector<AbstractExpressionRef> row_exprs;
	for (const auto &col : row) {
	  auto [_, expr] = PlanExpression(*col, {});
	  row_exprs.push_back(std::move(expr));
	}
	all_exprs.emplace_back(std::move(row_exprs));
  }

  const auto &first_row = all_exprs[0];
  std::vector<Column> cols;
  cols.reserve(first_row.size());
  size_t idx = 0;
  for (const auto &col : first_row) {
	auto col_name = fmt::format("{}.{}", table_ref.identifier_, idx);
	cols.emplace_back(col->GetReturnType().WithColumnName(col_name));
	idx += 1;
  }
  auto schema = std::make_shared<Schema>(cols);

  // 返回ValuesPlanNode以表示表达式列表
  return std::make_shared<ValuesPlanNode>(std::move(schema), std::move(all_exprs));
}


}  // namespace vdbms
