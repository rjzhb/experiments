//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_heap.h
//
// Identification: src/include/storage/table/table_heap.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "recovery/log_manager.h"
#include "storage/page/page_guard.h"
#include "storage/page/table_page.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

class TablePage;

/**
 * TableHeap 表示存储在磁盘上的物理表。
 * 它实质上是一个由页面组成的双向链表。
 */
class TableHeap {
  friend class TableIterator; // TableIterator 类可以访问 TableHeap 的私有成员

 public:
  ~TableHeap() = default;

  /**
   * 在没有事务的情况下创建一个表堆（打开表）。
   * @param buffer_pool_manager 缓冲池管理器
   * @param first_page_id 第一个页面的ID
   */
  explicit TableHeap(BufferPoolManager *bpm);

  /**
   * 向表中插入一个元组。如果元组太大（>=页面大小），则返回 std::nullopt。
   * @param meta 元组的元数据
   * @param tuple 要插入的元组
   * @param lock_mgr 锁管理器，用于并发控制（可选）
   * @param txn 事务，如果存在（可选）
   * @param oid 表的OID（可选）
   * @return 插入元组的RID（行标识符），如果插入失败则返回空值
   */
  auto InsertTuple(const TupleMeta &meta, const Tuple &tuple, LockManager *lock_mgr = nullptr,
				   Transaction *txn = nullptr, table_oid_t oid = 0) -> std::optional<RID>;

  /**
   * 更新元组的元数据。
   * @param meta 新的元组元数据
   * @param rid 要更新的元组的RID（行标识符）
   */
  void UpdateTupleMeta(const TupleMeta &meta, RID rid);

  /**
   * 从表中读取一个元组。
   * @param rid 要读取的元组的RID
   * @return 元组的元数据和元组本身
   */
  auto GetTuple(RID rid) -> std::pair<TupleMeta, Tuple>;

  /**
   * 从表中读取一个元组的元数据。注意：如果你想同时获取元组和元数据，请使用 `GetTuple` 方法
   * 以确保原子性。
   * @param rid 要读取的元组的RID
   * @return 元组的元数据
   */
  auto GetTupleMeta(RID rid) -> TupleMeta;

  /**
   * 返回这个表的迭代器。创建此迭代器时，它会记录当前表堆中的最后一个元组，
   * 并在该点停止迭代，以避免万圣节问题。这个函数通常用于项目3。
   * @return 表的迭代器
   */
  auto MakeIterator() -> TableIterator;

  /**
   * 返回这个表的迭代器。迭代器将在迭代时停在最后一个元组处。
   * @return 表的迭代器
   */
  auto MakeEagerIterator() -> TableIterator;

  /**
   * 返回此表的第一个页面的ID。
   * @return 第一个页面的ID
   */
  inline auto GetFirstPageId() const -> page_id_t { return first_page_id_; }

  /**
   * 原地更新一个元组。不应在项目3中使用。在项目3中，更新执行器应该实现为删除和插入。
   * 在项目4中，你将需要使用这个函数。
   * @param meta 新的元组元数据
   * @param tuple 新的元组
   * @param rid 要更新的元组的RID
   * @param check 在实际更新之前要执行的检查。
   * @return 是否成功更新元组
   */
  auto UpdateTupleInPlace(const TupleMeta &meta, const Tuple &tuple, RID rid,
						  std::function<bool(const TupleMeta &meta, const Tuple &table, RID rid)> &&check = nullptr)
  -> bool;

  /** 用于绑定器测试 */
  static auto CreateEmptyHeap(bool create_table_heap = false) -> std::unique_ptr<TableHeap> {
	// 输入参数应为false以生成一个空堆
	assert(!create_table_heap);
	return std::unique_ptr<TableHeap>(new TableHeap(create_table_heap));
  }

  // 下面的函数在你想要以移除undo日志的方式实现中止时很有用。
  // 如果你不确定它们的用途，请不要使用它们。

  auto AcquireTablePageReadLock(RID rid) -> ReadPageGuard;

  auto AcquireTablePageWriteLock(RID rid) -> WritePageGuard;

  void UpdateTupleInPlaceWithLockAcquired(const TupleMeta &meta, const Tuple &tuple, RID rid, TablePage *page);

  auto GetTupleWithLockAcquired(RID rid, const TablePage *page) -> std::pair<TupleMeta, Tuple>;

  auto GetTupleMetaWithLockAcquired(RID rid, const TablePage *page) -> TupleMeta;

 private:
  /** 用于绑定器测试 */
  explicit TableHeap(bool create_table_heap = false);

  BufferPoolManager *bpm_; // 缓冲池管理器
  page_id_t first_page_id_{INVALID_PAGE_ID}; // 第一个页面的ID

  std::mutex latch_; // 用于同步访问的互斥锁
  page_id_t last_page_id_{INVALID_PAGE_ID}; /* 由latch_保护 */
};


}  // namespace bustub
