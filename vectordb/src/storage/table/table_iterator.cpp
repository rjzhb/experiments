
#include <cassert>
#include <optional>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"

namespace bustub {

TableIterator::TableIterator(TableHeap *table_heap, RID rid, RID stop_at_rid)
    : table_heap_(table_heap), rid_(rid), stop_at_rid_(stop_at_rid) {
  // If the rid doesn't correspond to a tuple (i.e., the table has just been initialized), then
  // we set rid_ to invalid.
  if (rid.GetPageId() == INVALID_PAGE_ID) {
    rid_ = RID{INVALID_PAGE_ID, 0};
  } else {
    auto page_guard = table_heap_->bpm_->FetchPage(rid_.GetPageId());
    auto page = reinterpret_cast<TablePage *>(page_guard->GetData());
    if (rid_.GetSlotNum() >= page->GetNumTuples()) {
      rid_ = RID{INVALID_PAGE_ID, 0};
    }
  }
}

auto TableIterator::GetTuple() -> std::pair<TupleMeta, Tuple> { return table_heap_->GetTuple(rid_); }

auto TableIterator::GetRID() -> RID { return rid_; }

auto TableIterator::IsEnd() -> bool { return rid_.GetPageId() == INVALID_PAGE_ID; }

auto TableIterator::operator++() -> TableIterator & {
  // 获取当前记录的页
  auto page_guard = table_heap_->bpm_->FetchPage(rid_.GetPageId());
  // 将页面解释为TablePage类型
  auto page = reinterpret_cast<TablePage *>(page_guard->GetData());
  // 获取下一个元组的ID
  auto next_tuple_id = rid_.GetSlotNum() + 1;

  // 如果存在停止位置，则检查是否越界
  if (stop_at_rid_.GetPageId() != INVALID_PAGE_ID) {
	BUSTUB_ASSERT(
	/* 情况1：游标在停止元组所在页之前 */ rid_.GetPageId() < stop_at_rid_.GetPageId() ||
		/* 情况2：游标在停止元组之前的页面 */
		(rid_.GetPageId() == stop_at_rid_.GetPageId() && next_tuple_id <= stop_at_rid_.GetSlotNum()),
										 "迭代超出范围");
  }

  // 更新游标位置为下一个元组的位置
  rid_ = RID{rid_.GetPageId(), next_tuple_id};

  // 如果当前位置等于停止位置，则将游标置为无效页的第一个元组
  if (rid_ == stop_at_rid_) {
	rid_ = RID{INVALID_PAGE_ID, 0};
  } else if (next_tuple_id < page->GetNumTuples()) {
	// 在当前页中还有下一个元组，不需要进行额外操作
  } else {
	// 获取下一页的ID
	auto next_page_id = page->GetNextPageId();
	// 如果下一页的ID无效，则将RID设置为无效页；否则，设置为该页的第一个元组
	rid_ = RID{next_page_id, 0};
  }

  return *this;
}

}  // namespace bustub
