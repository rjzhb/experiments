#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "binder/table_ref/bound_subquery_ref.h"
#include "binder/tokens.h"
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"

namespace vdbms {

class BoundStatement;
class SelectStatement;
class DeleteStatement;
class AbstractPlanNode;
class InsertStatement;
class BoundExpression;
class BoundTableRef;
class BoundBinaryOp;
class BoundConstant;
class BoundColumnRef;
class BoundUnaryOp;
class BoundBaseTableRef;
class BoundSubqueryRef;
class BoundCrossProductRef;
class BoundJoinRef;
class BoundExpressionListRef;
class BoundAggCall;
class BoundCTERef;
class BoundFuncCall;
class ColumnValueExpression;

/**
 * 用于plan的上下文。用于plan聚合调用。
 */
class PlannerContext {
 public:
  PlannerContext() = default;

  void AddAggregation(std::unique_ptr<BoundExpression> expr);

  /** 指示此上下文是否允许聚合。 */
  bool allow_aggregation_{false};

  /** 指示在此上下文中要处理的下一个聚合调用。 */
  size_t next_aggregation_{0};

  /**
   * 在聚合规划的第一阶段中，我们将所有聚合调用表达式放入此向量中。
   * 此向量中的表达式应该用于原始过滤器/表扫描计划节点的输出上。
   */
  std::vector<std::unique_ptr<BoundExpression>> aggregations_;

  /**
   * 在聚合规划的第二阶段中，我们从 `aggregations_` 计划聚合调用，并生成一个聚合计划节点。
   * 向量中的表达式应该用于聚合计划节点的输出。
   */
  std::vector<AbstractExpressionRef> expr_in_agg_;

  /**
   * 范围内的 CTE。
   */
  const CTEList *cte_list_{nullptr};
};

/**
 * 规划器接受一个已绑定的语句，并将其转换为 vdbms 计划树。
 * 计划树将由执行引擎执行该语句。
 */
class Planner {
 public:
  explicit Planner(const Catalog &catalog) : catalog_(catalog) {}

  // 以下部分未记录。一个 `PlanXXX` 函数简单地对应于绑定器中的一个绑定对象。

  void PlanQuery(const BoundStatement &statement);

  auto PlanSelect(const SelectStatement &statement) -> AbstractPlanNodeRef;

  /**
   * @brief 规划一个 `BoundTableRef`
   *
   * - 对于 BaseTableRef，此函数将返回一个 `SeqScanPlanNode`。请注意，所有以 `__` 开头的表将被计划为 `MockScanPlanNode`。
   * - 对于 `JoinRef` 或 `CrossProductRef`，此函数将返回一个 `NestedLoopJoinNode`。
   * @param table_ref 绑定器中的绑定表引用。
   * @return 此绑定表引用的计划节点。
   */
  auto PlanTableRef(const BoundTableRef &table_ref) -> AbstractPlanNodeRef;

  auto PlanSubquery(const BoundSubqueryRef &table_ref, const std::string &alias) -> AbstractPlanNodeRef;

  auto PlanBaseTableRef(const BoundBaseTableRef &table_ref) -> AbstractPlanNodeRef;

  auto PlanCrossProductRef(const BoundCrossProductRef &table_ref) -> AbstractPlanNodeRef;

  auto PlanJoinRef(const BoundJoinRef &table_ref) -> AbstractPlanNodeRef;

  auto PlanCTERef(const BoundCTERef &table_ref) -> AbstractPlanNodeRef;

  auto PlanExpressionListRef(const BoundExpressionListRef &table_ref) -> AbstractPlanNodeRef;

  void AddAggCallToContext(BoundExpression &expr);

  auto PlanExpression(const BoundExpression &expr, const std::vector<AbstractPlanNodeRef> &children)
  -> std::tuple<std::string, AbstractExpressionRef>;

  auto PlanBinaryOp(const BoundBinaryOp &expr, const std::vector<AbstractPlanNodeRef> &children)
  -> AbstractExpressionRef;

  auto PlanFuncCall(const BoundFuncCall &expr, const std::vector<AbstractPlanNodeRef> &children)
  -> AbstractExpressionRef;

  auto PlanColumnRef(const BoundColumnRef &expr, const std::vector<AbstractPlanNodeRef> &children)
  -> std::tuple<std::string, std::shared_ptr<ColumnValueExpression>>;

  auto PlanConstant(const BoundConstant &expr, const std::vector<AbstractPlanNodeRef> &children)
  -> AbstractExpressionRef;

  auto PlanSelectAgg(const SelectStatement &statement, AbstractPlanNodeRef child) -> AbstractPlanNodeRef;

  auto PlanSelectWindow(const SelectStatement &statement, AbstractPlanNodeRef child) -> AbstractPlanNodeRef;

  auto PlanAggCall(const BoundAggCall &agg_call, const std::vector<AbstractPlanNodeRef> &children)
  -> std::tuple<AggregationType, std::vector<AbstractExpressionRef>>;

  auto GetAggCallFromFactory(const std::string &func_name, std::vector<AbstractExpressionRef> args)
  -> std::tuple<AggregationType, std::vector<AbstractExpressionRef>>;

  auto GetWindowAggCallFromFactory(const std::string &func_name, std::vector<AbstractExpressionRef> args)
  -> std::tuple<WindowFunctionType, std::vector<AbstractExpressionRef>>;

  auto GetBinaryExpressionFromFactory(const std::string &op_name, AbstractExpressionRef left,
									  AbstractExpressionRef right) -> AbstractExpressionRef;

  auto GetFuncCallFromFactory(const std::string &func_name, std::vector<AbstractExpressionRef> args)
  -> AbstractExpressionRef;

  auto PlanInsert(const InsertStatement &statement) -> AbstractPlanNodeRef;

  auto PlanDelete(const DeleteStatement &statement) -> AbstractPlanNodeRef;

  auto PlanUpdate(const UpdateStatement &statement) -> AbstractPlanNodeRef;

  /** 计划树的根节点 */
  AbstractPlanNodeRef plan_;

 private:
  PlannerContext ctx_;

  class ContextGuard {
   public:
	explicit ContextGuard(PlannerContext *ctx) : old_ctx_(std::move(*ctx)), ctx_ptr_(ctx) {
	  *ctx = PlannerContext();
	  ctx->cte_list_ = old_ctx_.cte_list_;
	}
	~ContextGuard() { *ctx_ptr_ = std::move(old_ctx_); }

	DISALLOW_COPY_AND_MOVE(ContextGuard);

   private:
	PlannerContext old_ctx_;
	PlannerContext *ctx_ptr_;
  };

  /** 如果任何函数需要修改范围，它必须持有上下文保护器，以便
   * 上下文将在函数返回后恢复。目前在
   * `BindFrom` 和 `BindJoin` 中使用。
   */
  auto NewContext() -> ContextGuard { return ContextGuard(&ctx_); }

  auto MakeOutputSchema(const std::vector<std::pair<std::string, TypeId>> &exprs) -> SchemaRef;

  /** 在规划过程中将使用 Catalog。应仅在
   * `PlanQuery` 的代码路径中使用，否则它是一个悬空引用。
   */
  const Catalog &catalog_;

  /** 用于所有无名称事物的 ID */
  size_t universal_id_{0};
};

static constexpr const char *const UNNAMED_COLUMN = "<unnamed>";

}  // namespace vdbms
