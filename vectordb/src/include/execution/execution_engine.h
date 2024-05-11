#pragma once

#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executor_factory.h"
#include "execution/executors/init_check_executor.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace vdbms {

/**
 * The ExecutionEngine 类负责执行查询计划。
 */
class ExecutionEngine {
 public:
  /**
   * 构造一个新的ExecutionEngine实例。
   * @param bpm 执行引擎使用的缓冲池管理器
   * @param txn_mgr 执行引擎使用的事务管理器
   * @param catalog 执行引擎使用的目录
   */
  ExecutionEngine(BufferPoolManager *bpm,
				  TransactionManager *txn_mgr,
				  Catalog *catalog)
	  : bpm_{bpm}, txn_mgr_{txn_mgr}, catalog_{catalog} {}

  DISALLOW_COPY_AND_MOVE(ExecutionEngine);

  /**
   * 执行一个查询计划。
   * @param plan 要执行的查询计划
   * @param result_set 执行计划后产生的元组集合
   * @param txn 执行查询的事务上下文
   * @param exec_ctx 执行查询的执行上下文
   * @return 如果查询计划执行成功返回`true`，否则返回`false`
   */
  // NOLINTNEXTLINE
  auto Execute(const AbstractPlanNodeRef &plan,
			   std::vector<Tuple> *result_set,
			   Transaction *txn,
			   ExecutorContext *exec_ctx) -> bool {
	vdbms_ASSERT((txn == exec_ctx->GetTransaction()), "Invariant violation: Transaction mismatch.");

	// 为抽象计划节点构造执行器
	auto executor = ExecutorFactory::CreateExecutor(exec_ctx, plan);

	// 初始化执行器
	auto executor_succeeded = true;

	try {
	  executor->Init(); // 初始化执行器
	  PollExecutor(executor.get(), plan, result_set); // 轮询执行器，直到执行完成
	  PerformChecks(exec_ctx); // 执行后的检查
	} catch (const ExecutionException &ex) {
	  executor_succeeded = false;
	  if (result_set != nullptr) {
		result_set->clear(); // 如果发生异常，清空结果集
	  }
	}

	return executor_succeeded;
  }

  /**
   * 执行上下文中的检查。
   * @param exec_ctx 执行上下文
   */
  void PerformChecks(ExecutorContext *exec_ctx) {
	// 遍历所有嵌套循环连接（NLJ）检查执行器集合
	for (const auto &[left_executor, right_executor] : exec_ctx->GetNLJCheckExecutorSet()) {
	  auto casted_left_executor = dynamic_cast<const InitCheckExecutor *>(left_executor);
	  auto casted_right_executor = dynamic_cast<const InitCheckExecutor *>(right_executor);
	  // 断言检查初始化次数和下一次计数之间的关系
	  vdbms_ASSERT(casted_right_executor->GetInitCount() + 1 >= casted_left_executor->GetNextCount(),
					"NLJ check failed: Are you initializing the right executor every time there is a left tuple? "
					"(off-by-one is acceptable)");
	}
  }

 private:
  /**
   * 轮询执行器直到耗尽或异常逃逸。
   * @param executor 根执行器
   * @param plan 要执行的计划
   * @param result_set 元组结果集
   */
  static void PollExecutor(AbstractExecutor *executor,
						   const AbstractPlanNodeRef &plan,
						   std::vector<Tuple> *result_set) {
	RID rid{};
	Tuple tuple{};
	while (executor->Next(&tuple, &rid)) { // 持续获取下一个元组，直到没有更多元组
	  if (result_set != nullptr) {
		result_set->push_back(tuple); // 将元组添加到结果集
	  }
	}
  }

  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] TransactionManager *txn_mgr_;
  [[maybe_unused]] Catalog *catalog_;
};

}  // namespace vdbms