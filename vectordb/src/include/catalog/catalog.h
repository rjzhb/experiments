#pragma once

#include <exception>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "container/hash/hash_function.h"
#include "execution/expressions/vector_expression.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/index/hnsw_index.h"
#include "storage/index/index.h"
#include "storage/index/ivfflat_index.h"
#include "storage/index/stl_ordered.h"
#include "storage/index/stl_unordered.h"
#include "storage/index/vector_index.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace vdbms {

/**
 * 类型定义
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

enum class IndexType {
  BPlusTreeIndex,
  HashTableIndex,
  STLOrderedIndex,
  STLUnorderedIndex,
  VectorIVFFlatIndex,
  VectorHNSWIndex
};

/**
 * TableInfo 类维护一个表的元数据。
 */
struct TableInfo {
  /**
   * 构造一个新的 TableInfo 实例。
   * @param schema 表结构
   * @param name 表名
   * @param table 指向表堆的拥有指针
   * @param oid 表的唯一OID
   */
  TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
	  : schema_{std::move(schema)}, name_{std::move(name)}, table_{std::move(table)}, oid_{oid} {}
  /** 表结构 */
  Schema schema_;
  /** 表名 */
  const std::string name_;
  /** 指向表堆的拥有指针 */
  std::unique_ptr<TableHeap> table_;
  /** 表OID */
  const table_oid_t oid_;
};

/**
 * IndexInfo 类维护一个索引的元数据。
 */
struct IndexInfo {
  /**
   * 构造一个新的 IndexInfo 实例。
   * @param key_schema 索引键的结构
   * @param name 索引的名称
   * @param index 指向索引的拥有指针
   * @param index_oid 索引的唯一OID
   * @param table_name 创建索引的表的名称
   * @param key_size 索引键的大小，以字节为单位
   */
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
			std::string table_name, size_t key_size, bool is_primary_key, IndexType index_type)
	  : key_schema_{std::move(key_schema)},
		name_{std::move(name)},
		index_{std::move(index)},
		index_oid_{index_oid},
		table_name_{std::move(table_name)},
		key_size_{key_size},
		is_primary_key_{is_primary_key},
		index_type_(index_type) {}
  /** 索引键的结构 */
  Schema key_schema_;
  /** 索引的名称 */
  std::string name_;
  /** 指向索引的拥有指针 */
  std::unique_ptr<Index> index_;
  /** 索引的唯一OID */
  index_oid_t index_oid_;
  /** 创建索引的表的名称 */
  std::string table_name_;
  /** 索引键的大小，以字节为单位 */
  const size_t key_size_;
  /** 是否为主键索引？ */
  bool is_primary_key_;
  /** 索引类型 */
  IndexType index_type_;
};

/**
 * Catalog 是一个非持久化的目录，旨在被数据库管理系统执行引擎中的执行器使用。它处理表的创建、表的查找、索引的创建和索引的查找。
 */
class Catalog {
 public:
  /** 表明一个返回 `TableInfo*` 的操作失败了 */
  static constexpr TableInfo *NULL_TABLE_INFO{nullptr};

  /** 表明一个返回 `IndexInfo*` 的操作失败了 */
  static constexpr IndexInfo *NULL_INDEX_INFO{nullptr};

  /**
   * 构造一个新的 Catalog 实例。
   * @param bpm 此目录创建的表所使用的缓冲池管理器
   * @param lock_manager 系统使用的锁管理器
   * @param log_manager 系统使用的日志管理器
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
	  : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * 创建一个新表并返回其元数据。
   * @param txn 正在创建表的事务
   * @param table_name 新表的名称，注意所有以 `__` 开头的表名都为系统保留。
   * @param schema 新表的结构
   * @param create_table_heap 是否为新表创建一个表堆
   * @return 指向表元数据的（非拥有）指针
   */
  auto CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema, bool create_table_heap = true)
  -> TableInfo * {
	if (table_names_.count(table_name) != 0) {
	  return NULL_TABLE_INFO;
	}

	// 构造表堆
	std::unique_ptr<TableHeap> table = nullptr;

	// 当 create_table_heap == false 时，表示我们正在运行绑定器测试（其中不会提供 txn）或
	// 我们在没有缓冲池的情况下运行 shell。在这种情况下，我们不需要创建 TableHeap。
	if (create_table_heap) {
	  table = std::make_unique<TableHeap>(bpm_);
	} else {
	  // 否则，仅为绑定器测试创建一个空堆
	  table = TableHeap::CreateEmptyHeap(create_table_heap);
	}

	// 获取新表的表 OID
	const auto table_oid = next_table_oid_.fetch_add(1);

	// 构造表信息
	auto meta = std::make_unique<TableInfo>(schema, table_name, std::move(table), table_oid);
	auto *tmp = meta.get();

	// 更新内部跟踪机制
	tables_.emplace(table_oid, std::move(meta));
	table_names_.emplace(table_name, table_oid);
	index_names_.emplace(table_name, std::unordered_map<std::string, index_oid_t>{});

	return tmp;
  }

  /**
   * 通过名称查询表元数据。
   * @param table_name 表名
   * @return 指向表元数据的（非拥有）指针
   */
  auto GetTable(const std::string &table_name) const -> TableInfo * {
	auto table_oid = table_names_.find(table_name);
	if (table_oid == table_names_.end()) {
	  // 未找到表
	  return NULL_TABLE_INFO;
	}

	auto meta = tables_.find(table_oid->second);
	vdbms_ASSERT(meta != tables_.end(), "Invariant Broken");

	return (meta->second).get();
  }

  /**
   * 通过OID查询表元数据
   * @param table_oid 要查询的表的OID
   * @return 指向表元数据的（非拥有）指针
   */
  auto GetTable(table_oid_t table_oid) const -> TableInfo * {
	auto meta = tables_.find(table_oid);
	if (meta == tables_.end()) {
	  return NULL_TABLE_INFO;
	}

	return (meta->second).get();
  }

  /**
   * 创建一个新索引，填充表的现有数据并返回其元数据。
   * @param txn 正在创建表的事务
   * @param index_name 新索引的名称
   * @param table_name 表的名称
   * @param schema 表的结构
   * @param key_schema 键的结构
   * @param key_attrs 键属性
   * @param keysize 键的大小
   * @param hash_function 索引的哈希函数
   * @return 指向新表元数据的（非拥有）指针
   */
  template <class KeyType, class ValueType, class KeyComparator>
  auto CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name, const Schema &schema,
				   const Schema &key_schema, const std::vector<uint32_t> &key_attrs, std::size_t keysize,
				   HashFunction<KeyType> hash_function, bool is_primary_key = false,
				   IndexType index_type = IndexType::HashTableIndex) -> IndexInfo * {
	// 拒绝不存在表的创建请求
	if (table_names_.find(table_name) == table_names_.end()) {
	  return NULL_INDEX_INFO;
	}

	// 如果表存在，那么 index_names_ 中应该已经有了该表的条目
	vdbms_ASSERT((index_names_.find(table_name) != index_names_.end()), "Invariant Broken");

	// 确定此表是否已存在请求的索引
	auto &table_indexes = index_names_.find(table_name)->second;
	if (table_indexes.find(index_name) != table_indexes.end()) {
	  // 此表已存在请求的索引
	  return NULL_INDEX_INFO;
	}

	// 构造索引元数据
	auto meta = std::make_unique<IndexMetadata>(index_name, table_name, &schema, key_attrs, is_primary_key);

	// 构造索引，拥有元数据的所有权
	// TODO（Kyle）：我们应该更新 CreateIndex 的 API
	// 以允许指定索引类型本身，而不仅仅是键、值和比较器类型

	// TODO（chi）：支持哈希索引和 B 树索引
	std::unique_ptr<Index> index;
	if (index_type == IndexType::HashTableIndex) {
	  index = std::make_unique<ExtendibleHashTableIndex<KeyType, ValueType, KeyComparator>>(std::move(meta), bpm_,
																							hash_function);
	} else if (index_type == IndexType::BPlusTreeIndex) {
	  index = std::make_unique<BPlusTreeIndex<KeyType, ValueType, KeyComparator>>(std::move(meta), bpm_);
	} else if (index_type == IndexType::STLOrderedIndex) {
	  index = std::make_unique<STLOrderedIndex<KeyType, ValueType, KeyComparator>>(std::move(meta), bpm_);
	} else if (index_type == IndexType::STLUnorderedIndex) {
	  index =
		  std::make_unique<STLUnorderedIndex<KeyType, ValueType, KeyComparator>>(std::move(meta), bpm_, hash_function);
	} else {
	  UNIMPLEMENTED("不支持的索引类型");
	}

	// 使用表堆中的所有元组填充索引
	auto *table_meta = GetTable(table_name);
	for (auto iter = table_meta->table_->MakeIterator(); !iter.IsEnd(); ++iter) {
	  auto [meta, tuple] = iter.GetTuple();
	  // 我们必须在这里默默忽略错误，因为很多原因...
	  index->InsertEntry(tuple.KeyFromTuple(schema, key_schema, key_attrs), tuple.GetRid(), txn);
	}

	// 获取新索引的下一个 OID
	const auto index_oid = next_index_oid_.fetch_add(1);

	// 构造索引信息；IndexInfo 拥有 Index 本身的所有权
	auto index_info = std::make_unique<IndexInfo>(key_schema, index_name, std::move(index), index_oid, table_name,
												  keysize, is_primary_key, index_type);
	auto *tmp = index_info.get();

	// 更新内部跟踪
	indexes_.emplace(index_oid, std::move(index_info));
	table_indexes.emplace(index_name, index_oid);

	return tmp;
  }

  auto CreateVectorIndex(Transaction *txn, const std::string &index_name, const std::string &table_name,
						 const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs,
						 const std::string &distance_fn, const std::vector<std::pair<std::string, int>> &options,
						 IndexType index_type) -> IndexInfo * {
	// 拒绝不存在表的创建请求
	if (table_names_.find(table_name) == table_names_.end()) {
	  return NULL_INDEX_INFO;
	}

	// 如果表存在，那么 index_names_ 中应该已经有了该表的条目
	vdbms_ASSERT((index_names_.find(table_name) != index_names_.end()), "Invariant Broken");

	// 确定此表是否已存在请求的索引
	auto &table_indexes = index_names_.find(table_name)->second;
	if (table_indexes.find(index_name) != table_indexes.end()) {
	  // 此表已存在请求的索引
	  return NULL_INDEX_INFO;
	}

	// 构造索引元数据
	auto meta = std::make_unique<IndexMetadata>(index_name, table_name, &schema, key_attrs, false);

	// 构造索引，拥有元数据的所有权
	// TODO（Kyle）：我们应该更新 CreateIndex 的 API
	// 以允许指定索引类型本身，而不仅仅是键、值和比较器类型

	// TODO（chi）：支持哈希索引和 B 树索引
	std::unique_ptr<VectorIndex> index;
	VectorExpressionType vty;
	if (distance_fn == "vector_ip_ops") {
	  vty = VectorExpressionType::InnerProduct;
	} else if (distance_fn == "vector_l2_ops") {
	  vty = VectorExpressionType::L2Dist;
	} else if (distance_fn == "vector_cosine_ops") {
	  vty = VectorExpressionType::CosineSimilarity;
	} else {
	  UNIMPLEMENTED("不支持的距离函数");
	}
	if (index_type == IndexType::VectorHNSWIndex) {
	  index = std::make_unique<HNSWIndex>(std::move(meta), bpm_, vty, options);
	} else if (index_type == IndexType::VectorIVFFlatIndex) {
	  index = std::make_unique<IVFFlatIndex>(std::move(meta), bpm_, vty, options);
	} else {
	  UNIMPLEMENTED("不支持的索引类型");
	}

	// 使用表堆中的所有元组填充索引
	auto *table_meta = GetTable(table_name);
	std::vector<std::pair<std::vector<double>, RID>> data;
	for (auto iter = table_meta->table_->MakeIterator(); !iter.IsEnd(); ++iter) {
	  auto [meta, tuple] = iter.GetTuple();
	  auto value = tuple.GetValue(&table_meta->schema_, key_attrs[0]);
	  data.emplace_back(value.GetVector(), iter.GetRID());
	}
	index->BuildIndex(data);

	// 获取新索引的下一个 OID
	const auto index_oid = next_index_oid_.fetch_add(1);

	// 构造索引信息；IndexInfo 拥有 Index 本身的所有权
	auto index_info = std::make_unique<IndexInfo>(key_schema, index_name, std::move(index), index_oid, table_name, 0,
												  false, index_type);
	auto *tmp = index_info.get();

	// 更新内部跟踪
	indexes_.emplace(index_oid, std::move(index_info));
	table_indexes.emplace(index_name, index_oid);

	return tmp;
  }

  /**
   * 获取表 `table_name` 的索引 `index_name`。
   * @param index_name 要查询的索引的名称
   * @param table_name 要执行查询的表的名称
   * @return 指向索引元数据的（非拥有）指针
   */
  auto GetIndex(const std::string &index_name, const std::string &table_name) -> IndexInfo * {
	auto table = index_names_.find(table_name);
	if (table == index_names_.end()) {
	  vdbms_ASSERT((table_names_.find(table_name) == table_names_.end()), "Invariant Broken");
	  return NULL_INDEX_INFO;
	}

	auto &table_indexes = table->second;

	auto index_meta = table_indexes.find(index_name);
	if (index_meta == table_indexes.end()) {
	  return NULL_INDEX_INFO;
	}

	auto index = indexes_.find(index_meta->second);
	vdbms_ASSERT((index != indexes_.end()), "Invariant Broken");

	return index->second.get();
  }

  /**
   * 获取由 `table_oid` 标识的表的索引 `index_name`。
   * @param index_name 要查询的索引的名称
   * @param table_oid 要执行查询的表的OID
   * @return 指向索引元数据的（非拥有）指针
   */
  auto GetIndex(const std::string &index_name, const table_oid_t table_oid) -> IndexInfo * {
	// 定位指定表 OID 的表元数据
	auto table_meta = tables_.find(table_oid);
	if (table_meta == tables_.end()) {
	  // 未找到表
	  return NULL_INDEX_INFO;
	}

	return GetIndex(index_name, table_meta->second->name_);
  }

  /**
   * Get the index identifier by index OID.
   * @param index_oid The OID of the index for which to query
   * @return A (non-owning) pointer to the metadata for the index
   */
  auto GetIndex(index_oid_t index_oid) -> IndexInfo * {
    auto index = indexes_.find(index_oid);
    if (index == indexes_.end()) {
      return NULL_INDEX_INFO;
    }

    return index->second.get();
  }

  /**
   * Get all of the indexes for the table identified by `table_name`.
   * @param table_name The name of the table for which indexes should be retrieved
   * @return A vector of IndexInfo* for each index on the given table, empty vector
   * in the event that the table exists but no indexes have been created for it
   */
  auto GetTableIndexes(const std::string &table_name) const -> std::vector<IndexInfo *> {
    // Ensure the table exists
    if (table_names_.find(table_name) == table_names_.end()) {
      return std::vector<IndexInfo *>{};
    }

    auto table_indexes = index_names_.find(table_name);
    vdbms_ASSERT((table_indexes != index_names_.end()), "Broken Invariant");

    std::vector<IndexInfo *> indexes{};
    indexes.reserve(table_indexes->second.size());
    for (const auto &index_meta : table_indexes->second) {
      auto index = indexes_.find(index_meta.second);
      vdbms_ASSERT((index != indexes_.end()), "Broken Invariant");
      indexes.push_back(index->second.get());
    }

    return indexes;
  }

  auto GetTableNames() -> std::vector<std::string> {
    std::vector<std::string> result;
    for (const auto &x : table_names_) {
      result.push_back(x.first);
    }
    return result;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /**
   * Map table identifier -> table metadata.
   *
   * NOTE: `tables_` owns all table metadata.
   */
  std::unordered_map<table_oid_t, std::unique_ptr<TableInfo>> tables_;

  /** Map table name -> table identifiers. */
  std::unordered_map<std::string, table_oid_t> table_names_;

  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};

  /**
   * Map index identifier -> index metadata.
   *
   * NOTE: that `indexes_` owns all index metadata.
   */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;

  /** Map table name -> index names -> index identifiers. */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;

  /** The next index identifier to be used. */
  std::atomic<index_oid_t> next_index_oid_{0};
};

}  // namespace vdbms

template <>
struct fmt::formatter<vdbms::IndexType> : formatter<string_view> {
  template <typename FormatContext>
  auto format(vdbms::IndexType c, FormatContext &ctx) const {
    string_view name;
    switch (c) {
      case vdbms::IndexType::BPlusTreeIndex:
        name = "BPlusTree";
        break;
      case vdbms::IndexType::HashTableIndex:
        name = "Hash";
        break;
      case vdbms::IndexType::STLOrderedIndex:
        name = "STLOrdered";
        break;
      case vdbms::IndexType::STLUnorderedIndex:
        name = "STLUnordered";
        break;
      case vdbms::IndexType::VectorHNSWIndex:
        name = "VectorHNSW";
        break;
      case vdbms::IndexType::VectorIVFFlatIndex:
        name = "VectorIVFFlat";
        break;
      default:
        name = "Unknown";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
