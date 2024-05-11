#include <memory>
#include <optional>
#include <tuple>
#include <vector>
#include "binder/bound_expression.h"
#include "binder/bound_statement.h"
#include "binder/expressions/bound_agg_call.h"
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_column_ref.h"
#include "binder/expressions/bound_constant.h"
#include "binder/expressions/bound_func_call.h"
#include "binder/expressions/bound_unary_op.h"
#include "binder/expressions/bound_window.h"
#include "binder/statement/select_statement.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/util/string_util.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "fmt/format.h"
#include "planner/planner.h"
#include "type/value_factory.h"

namespace vdbms {

/**
 *  列引用（COLUMN_REF）表示引用数据库表中的列。
	常量（CONSTANT）表示SQL语句中的一个常量值，比如字符串、整数、浮点数等。
	函数调用（FUNC_CALL）表示调用数据库中的函数，比如聚合函数、数学函数等。
	二元操作（BINARY_OP）表示两个表达式之间的二元操作，比如加法、减法、乘法等。
	聚合函数调用（AGG_CALL）表示聚合函数的调用，比如 SUM、COUNT、AVG 等。
 */

auto Planner::PlanBinaryOp(const BoundBinaryOp &expr,
						   const std::vector<AbstractPlanNodeRef> &children)
-> AbstractExpressionRef {
  auto [_1, left] = PlanExpression(*expr.larg_, children); // 规划左表达式
  auto [_2, right] = PlanExpression(*expr.rarg_, children); // 规划右表达式
  const auto &op_name = expr.op_name_; // 获取运算符名称
  return GetBinaryExpressionFromFactory(op_name, std::move(left), std::move(right)); // 通过工厂方法获取二元表达式
}

auto Planner::PlanColumnRef(const BoundColumnRef &expr,
							const std::vector<AbstractPlanNodeRef> &children)
-> std::tuple<std::string, std::shared_ptr<ColumnValueExpression>> {
  if (children.empty()) {
	throw Exception("column ref should have at least one child"); // 判断子节点是否为空
  }

  auto col_name = expr.ToString(); // 获取列名

  if (children.size() == 1) {
	// 对于只有一个子节点的情况，通常是在投影、过滤等执行器中使用表达式
	const auto &child = children[0];
	auto schema = child->OutputSchema();
	// 在调用 `schema.GetColIdx` 之前，需要确保没有重复的列名
	bool found = false;
	for (const auto &col : schema.GetColumns()) {
	  if (col_name == col.GetName()) {
		if (found) {
		  throw vdbms::Exception("duplicated column found in schema"); // 如果发现重复的列名，则抛出异常
		}
		found = true;
	  }
	}
	uint32_t col_idx = schema.GetColIdx(col_name); // 获取列索引
	auto col_type = schema.GetColumn(col_idx); // 获取列类型
	return std::make_tuple(col_name, std::make_shared<ColumnValueExpression>(0, col_idx, col_type)); // 返回列名和对应的列值表达式
  }
  if (children.size() == 2) {
	/*
	 * 对于联接操作，使用此分支来规划表达式。
	 *
	 * 如果一个表达式用于联接条件，例如：
	 * SELECT * from test_1 inner join test_2 on test_1.colA = test_2.col2
	 * 规划结果将会是：
	 * ```
	 * NestedLoopJoin condition={ ColumnRef 0.0=ColumnRef 1.1 }
	 *   SeqScan colA, colB
	 *   SeqScan col1, col2
	 * ```
	 * 在 `ColumnRef n.m` 中，当执行器使用该表达式时，它将从第n个子节点的第m个列中获取数据。
	 */

	const auto &left = children[0];
	const auto &right = children[1];
	auto left_schema = left->OutputSchema();
	auto right_schema = right->OutputSchema();

	auto col_idx_left = left_schema.TryGetColIdx(col_name); // 尝试从左侧子节点获取列索引
	auto col_idx_right = right_schema.TryGetColIdx(col_name); // 尝试从右侧子节点获取列索引
	if (col_idx_left && col_idx_right) {
	  throw vdbms::Exception(fmt::format("ambiguous column name {}", col_name)); // 如果在左右两个节点中都找到了同名的列，则抛出异常
	}
	if (col_idx_left) {
	  auto col_type = left_schema.GetColumn(*col_idx_left);
	  return std::make_tuple(col_name, std::make_shared<ColumnValueExpression>(0, *col_idx_left, col_type)); // 返回列名和对应的列值表达式
	}
	if (col_idx_right) {
	  auto col_type = right_schema.GetColumn(*col_idx_right);
	  return std::make_tuple(col_name, std::make_shared<ColumnValueExpression>(1, *col_idx_right, col_type)); // 返回列名和对应的列值表达式
	}
	throw vdbms::Exception(fmt::format("column name {} not found", col_name)); // 如果无法在左右子节点中找到列名，则抛出异常
  }
  UNREACHABLE("no executor with expression has more than 2 children for now"); // 对于现有的执行器，表达式不会有超过两个子节点的情况
}

auto Planner::PlanConstant(const BoundConstant &expr,
						   const std::vector<AbstractPlanNodeRef> &children)
-> AbstractExpressionRef {
  return std::make_shared<ConstantValueExpression>(expr.val_); // 返回常量值表达式
}

void Planner::AddAggCallToContext(BoundExpression &expr) {
  switch (expr.type_) {
	case ExpressionType::AGG_CALL: {
	  auto &agg_call_expr = dynamic_cast<BoundAggCall &>(expr);
	  auto agg_name = fmt::format("__pseudo_agg#{}", ctx_.aggregations_.size());
	  auto agg_call =
		  BoundAggCall(agg_name, agg_call_expr.is_distinct_, std::vector<std::unique_ptr<BoundExpression>>{});
	  // 将原始绑定表达式中的聚合调用替换为伪聚合调用，并将聚合调用添加到上下文中
	  ctx_.AddAggregation(std::make_unique<BoundAggCall>(std::exchange(agg_call_expr, std::move(agg_call))));
	  return;
	}
	case ExpressionType::COLUMN_REF: {
	  return;
	}
	case ExpressionType::BINARY_OP: {
	  auto &binary_op_expr = dynamic_cast<BoundBinaryOp &>(expr);
	  AddAggCallToContext(*binary_op_expr.larg_);
	  AddAggCallToContext(*binary_op_expr.rarg_);
	  return;
	}
	case ExpressionType::FUNC_CALL: {
	  auto &func_call_expr = dynamic_cast<BoundFuncCall &>(expr);
	  for (const auto &child : func_call_expr.args_) {
		AddAggCallToContext(*child);
	  }
	  return;
	}
	case ExpressionType::CONSTANT: {
	  return;
	}
	case ExpressionType::ALIAS: {
	  auto &alias_expr = dynamic_cast<const BoundAlias &>(expr);
	  AddAggCallToContext(*alias_expr.child_);
	  return;
	}
	default:
	  break;
  }
  throw Exception(fmt::format("expression type {} not supported in planner yet", expr.type_));
}

auto Planner::PlanExpression(const BoundExpression &expr,
							 const std::vector<AbstractPlanNodeRef> &children)
-> std::tuple<std::string, AbstractExpressionRef> {
  switch (expr.type_) {
	case ExpressionType::AGG_CALL: {
	  if (ctx_.next_aggregation_ >= ctx_.expr_in_agg_.size()) {
		throw vdbms::Exception("unexpected agg call");
	  }
	  return std::make_tuple(UNNAMED_COLUMN, std::move(ctx_.expr_in_agg_[ctx_.next_aggregation_++])); // 返回未命名的列名和聚合表达式
	}
	case ExpressionType::COLUMN_REF: {
	  const auto &column_ref_expr = dynamic_cast<const BoundColumnRef &>(expr);
	  return PlanColumnRef(column_ref_expr, children); // 规划列引用表达式
	}
	case ExpressionType::BINARY_OP: {
	  const auto &binary_op_expr = dynamic_cast<const BoundBinaryOp &>(expr);
	  return std::make_tuple(UNNAMED_COLUMN, PlanBinaryOp(binary_op_expr, children)); // 规划二元操作表达式
	}
	case ExpressionType::FUNC_CALL: {
	  const auto &func_call_expr = dynamic_cast<const BoundFuncCall &>(expr);
	  return std::make_tuple(UNNAMED_COLUMN, PlanFuncCall(func_call_expr, children)); // 规划函数调用表达式
	}
	case ExpressionType::CONSTANT: {
	  const auto &constant_expr = dynamic_cast<const BoundConstant &>(expr);
	  return std::make_tuple(UNNAMED_COLUMN, PlanConstant(constant_expr, children)); // 规划常量表达式
	}
	case ExpressionType::ALIAS: {
	  const auto &alias_expr = dynamic_cast<const BoundAlias &>(expr);
	  auto [_1, expr] = PlanExpression(*alias_expr.child_, children); // 规划别名表达式的子表达式
	  return std::make_tuple(alias_expr.alias_, std::move(expr)); // 返回别名和表达式
	}
	case ExpressionType::WINDOW: {
	  throw Exception("should not parse window expressions here"); // 不应该在此处解析窗口表达式
	}
	default:
	  break;
  }
  throw Exception(fmt::format("expression type {} not supported in planner yet", expr.type_)); // 抛出不支持的表达式类型异常
}


}  // namespace vdbms
