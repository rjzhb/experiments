#include <cassert>
#include <mutex>  // NOLINT
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "fmt/format.h"
#include "storage/page/page_guard.h"
#include "storage/page/table_page.h"
#include "storage/table/table_heap.h"

namespace bustub {

// TableHeap 类的构造函数，初始化了表堆。
// @param bpm 缓冲池管理器，用于页面的管理和I/O操作。
TableHeap::TableHeap(BufferPoolManager *bpm) : bpm_(bpm) {
  // 初始化表的第一个页面。
  auto guard = bpm->NewPage(&first_page_id_);  // 创建一个新的页面，并将页面ID存储在first_page_id_中。
  last_page_id_ = first_page_id_;  // 初始化时，第一个页面也是最后一个页面。
  auto first_page = reinterpret_cast<TablePage *>(guard->GetData());  // 将新页面的数据转换为TablePage类型。
  // 确保创建页面成功。
  BUSTUB_ASSERT(first_page != nullptr,
				"Couldn't create a page for the table heap. Have you completed the buffer pool manager project?");
  first_page->Init();  // 初始化表页面。
}

// 简化的构造函数，可能用于测试或特殊情况，不创建实际的表堆。
TableHeap::TableHeap(bool create_table_heap) : bpm_(nullptr) {}

// 向表中插入一个新元组。
// @param meta 元组的元数据。
// @param tuple 要插入的元组。
// @param lock_mgr 锁管理器，用于事务的并发控制。
// @param txn 当前操作的事务。
// @param oid 表的唯一标识符。
// @return 插入元组的RID，如果失败则返回空值。
auto TableHeap::InsertTuple(const TupleMeta &meta, const Tuple &tuple, LockManager *lock_mgr, Transaction *txn,
							table_oid_t oid) -> std::optional<RID> {
  std::unique_lock<std::mutex> guard(latch_);  // 锁定互斥量以保护对last_page_id_的访问。
  auto page_guard = bpm_->FetchPage(last_page_id_);  // 获取最后一个页面。
  page_guard->WLatch();  // 对页面加写锁。
  while (true) {
	auto page = reinterpret_cast<TablePage *>(page_guard->GetData());  // 获取页面数据。
	// 尝试获取元组在页面中的下一个偏移量，如果成功则跳出循环。
	if (page->GetNextTupleOffset(meta, tuple) != std::nullopt) {
	  break;
	}
	// 如果当前页面没有元组并且无法插入新元组，那么新元组太大无法插入。
	BUSTUB_ENSURE(page->GetNumTuples() != 0, "tuple is too large, cannot insert");

	// 分配一个新的页面来存储无法在当前页面中插入的元组。
	page_id_t next_page_id = INVALID_PAGE_ID;
	auto npg = bpm_->NewPage(&next_page_id);  // 创建一个新的页面。
	BUSTUB_ENSURE(next_page_id != INVALID_PAGE_ID, "cannot allocate page");

	page->SetNextPageId(next_page_id);  // 将新页面设置为当前页面的下一个页面。

	auto next_page = reinterpret_cast<TablePage *>(npg->GetData());  // 获取新页面数据。
	next_page->Init();  // 初始化新页面。

	page_guard->WUnlatch();  // 解除当前页面的写锁。

	// 获取新页面的写锁。
	npg->WLatch();

	last_page_id_ = next_page_id;  // 更新最后一个页面的ID。
	page_guard = npg;  // 更新页面保护器以指向新页面。
  }
  auto last_page_id = last_page_id_;  // 获取最后一个页面的ID。

  auto page = reinterpret_cast<TablePage *>(page_guard->GetData());  // 获取页面数据。
  auto slot_id = *page->InsertTuple(meta, tuple);  // 在页面中插入元组，并获取插槽ID。

  // 解锁互斥量，允许其他插入操作进行。
  guard.unlock();

  // 如果启用了锁管理器，则对新插入的元组加锁。
#ifndef DISABLE_LOCK_MANAGER
  if (lock_mgr != nullptr) {
    BUSTUB_ENSURE(lock_mgr->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, RID{last_page_id, slot_id}),
                  "failed to lock when inserting new tuple");
  }
#endif

  page_guard->WUnlatch();  // 解除页面的写锁。

  return RID(last_page_id, slot_id);  // 返回新插入元组的RID。
}

// 更新元组的元数据。
// @param meta 新的元组元数据。
// @param rid 元组的行标识符。
void TableHeap::UpdateTupleMeta(const TupleMeta &meta, RID rid) {
  auto page_guard = bpm_->FetchPage(rid.GetPageId());  // 获取包含元组的页面。
  auto page = reinterpret_cast<TablePage *>(page_guard);  // 获取页面数据。
  page->UpdateTupleMeta(meta, rid);  // 更新元组的元数据。
}

// 从表中获取一个元组及其元数据。
// @param rid 元组的行标识符。
// @return 元组的元数据和元组本身。
auto TableHeap::GetTuple(RID rid) -> std::pair<TupleMeta, Tuple> {
  auto page_guard = bpm_->FetchPage(rid.GetPageId());  // 获取包含元组的页面。
  auto page = reinterpret_cast<TablePage *>(page_guard->GetData());  // 获取页面数据。
  auto [meta, tuple] = page->GetTuple(rid);  // 获取元组及其元数据。
  tuple.rid_ = rid;  // 设置元组的RID。
  return std::make_pair(meta, std::move(tuple));  // 返回元组及其元数据。
}

// 从表中获取一个元组的元数据。
// @param rid 元组的行标识符。
// @return 元组的元数据。
auto TableHeap::GetTupleMeta(RID rid) -> TupleMeta {
  auto page_guard = bpm_->FetchPageRead(rid.GetPageId());  // 获取包含元组的页面（读模式）。
  auto page = page_guard.As<TablePage>();  // 获取页面数据。
  return page->GetTupleMeta(rid);  // 返回元组的元数据。
}

// 创建表的迭代器。
// @return 表的迭代器，可用于遍历表中的元组。
auto TableHeap::MakeIterator() -> TableIterator {
  std::unique_lock<std::mutex> guard(latch_);  // 锁定互斥量以保护对last_page_id_的访问。
  auto last_page_id = last_page_id_;  // 获取最后一个页面的ID。
  guard.unlock();  // 解锁互斥量。

  auto page_guard = bpm_->FetchPage(last_page_id);  // 获取最后一个页面。
  auto page = reinterpret_cast<TablePage *>(page_guard->GetData());  // 获取页面数据。
  auto num_tuples = page->GetNumTuples();  // 获取页面中元组的数量。
  return {this, {first_page_id_, 0}, {last_page_id, num_tuples}};  // 创建并返回迭代器。
}

// 创建一个急切的表迭代器，即立即迭代到表的末尾。
// @return 表的迭代器。
auto TableHeap::MakeEagerIterator() -> TableIterator { return {this, {first_page_id_, 0}, {INVALID_PAGE_ID, 0}}; }

// 在原地更新一个元组。
// @param meta 新的元组元数据。
// @param tuple 新的元组。
// @param rid 元组的行标识符。
// @param check 在实际更新之前要执行的检查函数。
// @return 是否成功更新元组。
auto TableHeap::UpdateTupleInPlace(const TupleMeta &meta, const Tuple &tuple, RID rid,
								   std::function<bool(const TupleMeta &meta, const Tuple &table, RID rid)> &&check)
-> bool {
  auto page_guard = bpm_->FetchPage(rid.GetPageId());  // 获取包含元组的页面。
  auto page = reinterpret_cast<TablePage *>(page_guard->GetData());  // 获取页面数据。
  auto [old_meta, old_tup] = page->GetTuple(rid);  // 获取旧的元组及其元数据。
  // 如果没有提供检查函数或检查函数返回true，则执行更新。
  if (check == nullptr || check(old_meta, old_tup, rid)) {
	page->UpdateTupleInPlaceUnsafe(meta, tuple, rid);  // 在原地更新元组。
	return true;  // 返回更新成功。
  }
  return false;  // 返回更新失败。
}

// 获取读锁定的表页面。
// @param rid 元组的行标识符。
// @return 读锁定的页面保护器。
auto TableHeap::AcquireTablePageReadLock(RID rid) -> ReadPageGuard { return bpm_->FetchPageRead(rid.GetPageId()); }

// 获取写锁定的表页面。
// @param rid 元组的行标识符。
// @return 写锁定的页面保护器。
auto TableHeap::AcquireTablePageWriteLock(RID rid) -> WritePageGuard { return bpm_->FetchPageWrite(rid.GetPageId()); }

// 在已经获得锁的情况下更新原地的元组。
// @param meta 新的元组元数据。
// @param tuple 新的元组。
// @param rid 元组的行标识符。
// @param page 元组所在的页面。
void TableHeap::UpdateTupleInPlaceWithLockAcquired(const TupleMeta &meta, const Tuple &tuple, RID rid,
												   TablePage *page) {
  page->UpdateTupleInPlaceUnsafe(meta, tuple, rid);  // 在原地更新元组。
}

// 在已经获得锁的情况下获取一个元组及其元数据。
// @param rid 元组的行标识符。
// @param page 元组所在的页面。
// @return 元组及其元数据。
auto TableHeap::GetTupleWithLockAcquired(RID rid, const TablePage *page) -> std::pair<TupleMeta, Tuple> {
  auto [meta, tuple] = page->GetTuple(rid);  // 获取元组及其元数据。
  tuple.rid_ = rid;  // 设置元组的RID。
  return std::make_pair(meta, std::move(tuple));  // 返回元组及其元数据。
}

// 在已经获得锁的情况下获取一个元组的元数据。
// @param rid 元组的行标识符。
// @param page 元组所在的页面。
// @return 元组的元数据。
auto TableHeap::GetTupleMetaWithLockAcquired(RID rid, const TablePage *page) -> TupleMeta {
  return page->GetTupleMeta(rid);  // 返回元组的元数据。
}

}  // namespace bustub
