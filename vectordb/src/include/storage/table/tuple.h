#pragma once

#include <string>
#include <vector>

#include "catalog/schema.h"
#include "common/config.h"
#include "common/rid.h"
#include "type/value.h"

namespace vdbms {

using timestamp_t = int64_t; // 定义时间戳类型
const timestamp_t INVALID_TS = -1; // 无效时间戳的常量定义

static constexpr size_t TUPLE_META_SIZE = 16; // 元组元数据的大小

// 元组元数据的结构体定义
struct TupleMeta {
  timestamp_t ts_; // 元组的时间戳或事务ID，用于版本控制或并发控制
  bool is_deleted_; // 标记元组是否已从表堆中删除

  // 比较两个元组元数据是否相等的操作符重载
  friend auto operator==(const TupleMeta &a, const TupleMeta &b) {
	return a.ts_ == b.ts_ && a.is_deleted_ == b.is_deleted_;
  }

  // 比较两个元组元数据是否不等的操作符重载
  friend auto operator!=(const TupleMeta &a, const TupleMeta &b) { return !(a == b); }
};

// 静态断言以确保TupleMeta的大小符合预期
static_assert(sizeof(TupleMeta) == TUPLE_META_SIZE);

/**
 * 元组的格式描述：
 * ---------------------------------------------------------------------
 * | 固定大小或可变大小字段的偏移量 | 可变大小字段的有效载荷 |
 * ---------------------------------------------------------------------
 */
class Tuple {
  // 元组类的友元类，它们可以访问元组的私有成员
  friend class TablePage;
  friend class TableHeap;
  friend class TableIterator;

 public:
  // 默认构造函数，用于创建虚拟元组
  Tuple() = default;

  // 构造函数，用于创建指向表堆中特定元组的元组对象
  explicit Tuple(RID rid) : rid_(rid) {}

  // 创建一个空元组的静态方法
  static auto Empty() -> Tuple { return Tuple{RID{INVALID_PAGE_ID, 0}}; }

  // 根据值和模式创建新元组的构造函数
  Tuple(std::vector<Value> values, const Schema *schema);

  // 拷贝构造函数
  Tuple(const Tuple &other) = default;

  // 移动构造函数
  Tuple(Tuple &&other) noexcept = default;

  // 赋值操作符，深拷贝
  auto operator=(const Tuple &other) -> Tuple & = default;

  // 移动赋值操作符
  auto operator=(Tuple &&other) noexcept -> Tuple & = default;

  // 序列化元组数据到存储空间
  void SerializeTo(char *storage) const;

  // 从存储空间反序列化元组数据（深拷贝）
  void DeserializeFrom(const char *storage);

  // 获取当前元组的RID
  inline auto GetRid() const -> RID { return rid_; }

  // 设置当前元组的RID
  inline auto SetRid(RID rid) { rid_ = rid; }

  // 获取表存储中此元组的地址
  inline auto GetData() const -> const char * { return data_.data(); }

  // 获取元组的长度，包括varchar字段的长度
  inline auto GetLength() const -> uint32_t { return data_.size(); }

  // 根据模式获取指定列的值（const版本）
  auto GetValue(const Schema *schema, uint32_t column_idx) const -> Value;

  // 使用模式和属性生成键元组
  auto KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs) -> Tuple;

  // 判断指定列值是否为空
  inline auto IsNull(const Schema *schema, uint32_t column_idx) const -> bool {
	Value value = GetValue(schema, column_idx);
	return value.IsNull();
  }

  // 将元组转换为字符串表示
  auto ToString(const Schema *schema) const -> std::string;

  // 判断两个元组的内容是否相等
  friend inline auto IsTupleContentEqual(const Tuple &a, const Tuple &b) { return a.data_ == b.data_; }

 private:
  // 获取特定列的起始存储地址
  auto GetDataPtr(const Schema *schema, uint32_t column_idx) const -> const char *;

  RID rid_{};  // 如果指向表堆，则rid有效
  std::vector<char> data_;  // 元组的数据内容
};

}  // namespace vdbms
