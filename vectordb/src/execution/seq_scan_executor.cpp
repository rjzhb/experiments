#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "storage/table/table_iterator.h"

namespace bustub {

/**
 * 构造一个序列扫描执行器 (SeqScanExecutor)。
 * @param exec_ctx 执行上下文，包含执行查询所需的所有上下文信息
 * @param plan 指向序列扫描计划节点的指针，包含执行序列扫描所需的信息
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx,
								 const SeqScanPlanNode *plan)
	: AbstractExecutor(exec_ctx), plan_{plan} {
  // 从执行上下文获取数据库的目录，然后根据计划节点中指定的表 OID 获取表，并存储表堆的指针
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
}

/**
 * 初始化执行器，准备进行表扫描。
 */
void SeqScanExecutor::Init() {
  // 创建一个表迭代器，用于遍历表中的所有元组
  iter_ = std::make_unique<TableIterator>(table_heap_->MakeIterator());
}

/**
 * 获取表中的下一条元组。
 * @param tuple 用于存储获取到的元组的指针
 * @param rid 用于存储获取到的元组的行标识符（RID）的指针
 * @return 如果表中有更多元组，则返回 true；否则，如果迭代器已经到达表的末尾，返回 false
 */
auto SeqScanExecutor::Next(Tuple *tuple,
						   RID *rid) -> bool {
  // 检查迭代器是否已到达表的末尾
  if (iter_->IsEnd()) {
	return false; // 如果已到末尾，返回 false
  }
  // 获取迭代器当前指向的元组
  auto [_, tup] = iter_->GetTuple();
  *tuple = tup; // 将获取到的元组存储到调用者提供的指针中
  *rid = iter_->GetRID(); // 将元组的行标识符（RID）存储到调用者提供的指针中
  ++(*iter_); // 将迭代器移动到下一个元组
  return true; // 返回 true，表示成功获取到元组
}
}  // namespace bustub
