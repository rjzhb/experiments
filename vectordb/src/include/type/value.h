#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"

#include "type/limits.h"
#include "type/type.h"

namespace vdbms {

class Column;

inline auto GetCmpBool(bool boolean) -> CmpBool { return boolean ? CmpBool::CmpTrue : CmpBool::CmpFalse; }

// Value是一个抽象类，代表在数据库中存储的SQL数据的视图。
// 它定义了所有值共有的基本接口和属性，例如数据类型和比较函数。
// 具体的类型特定功能由子类实现。
class Value {
  // 允许Type类及其子类访问Value的私有和保护成员。
  friend class Type;
  friend class NumericType;
  friend class IntegerParentType;
  friend class TinyintType;
  friend class SmallintType;
  friend class IntegerType;
  friend class BigintType;
  friend class DecimalType;
  friend class TimestampType;
  friend class BooleanType;
  friend class VarlenType;
  //向量数据库专用
  friend class VectorType;

 public:
  explicit Value(const TypeId type) : manage_data_(false), type_id_(type) { size_.len_ = vdbms_VALUE_NULL; }
  // BOOLEAN and TINYINT
  Value(TypeId type, int8_t i);
  // DECIMAL
  Value(TypeId type, double d);
  Value(TypeId type, float f);
  // SMALLINT
  Value(TypeId type, int16_t i);
  // INTEGER
  Value(TypeId type, int32_t i);
  // BIGINT
  Value(TypeId type, int64_t i);
  // TIMESTAMP
  Value(TypeId type, uint64_t i);
  // VARCHAR
  Value(TypeId type, const char *data, uint32_t len, bool manage_data);
  Value(TypeId type, const std::string &data);
  Value(TypeId type, const std::vector<double> &data);

  Value() : Value(TypeId::INVALID) {}
  Value(const Value &other);
  auto operator=(Value other) -> Value &;
  ~Value();
  // NOLINTNEXTLINE
  friend void Swap(Value &first, Value &second) {
    std::swap(first.value_, second.value_);
    std::swap(first.size_, second.size_);
    std::swap(first.manage_data_, second.manage_data_);
    std::swap(first.type_id_, second.type_id_);
  }
  // check whether value is integer
  auto CheckInteger() const -> bool;
  auto CheckComparable(const Value &o) const -> bool;

  // Get the type of this value
  inline auto GetTypeId() const -> TypeId { return type_id_; }

  // Get the type of this value
  auto GetColumn() const -> Column;

  // Get the length of the variable length data
  inline auto GetStorageSize() const -> uint32_t { return Type::GetInstance(type_id_)->GetStorageSize(*this); }
  // Access the raw variable length data
  inline auto GetData() const -> const char * { return Type::GetInstance(type_id_)->GetData(*this); }

  template <class T>
  inline auto GetAs() const -> T {
    return *reinterpret_cast<const T *>(&value_);
  }

  auto GetVector() const -> std::vector<double>;
  const std::pair<double, double> &GetCache() const { return cache_; }

  inline auto CastAs(const TypeId type_id) const -> Value {
    return Type::GetInstance(type_id_)->CastAs(*this, type_id);
  }
  // You will likely need this in project 4...
  inline auto CompareExactlyEquals(const Value &o) const -> bool {
    if (this->IsNull() && o.IsNull()) {
      return true;
    }
    return (Type::GetInstance(type_id_)->CompareEquals(*this, o)) == CmpBool::CmpTrue;
  }
  // Comparison Methods
  inline auto CompareEquals(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareEquals(*this, o);
  }
  inline auto CompareNotEquals(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareNotEquals(*this, o);
  }
  inline auto CompareLessThan(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareLessThan(*this, o);
  }
  inline auto CompareLessThanEquals(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareLessThanEquals(*this, o);
  }
  inline auto CompareGreaterThan(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareGreaterThan(*this, o);
  }
  inline auto CompareGreaterThanEquals(const Value &o) const -> CmpBool {
    return Type::GetInstance(type_id_)->CompareGreaterThanEquals(*this, o);
  }

  // Other mathematical functions
  inline auto Add(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Add(*this, o); }
  inline auto Subtract(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Subtract(*this, o); }
  inline auto Multiply(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Multiply(*this, o); }
  inline auto Divide(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Divide(*this, o); }
  inline auto Modulo(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Modulo(*this, o); }
  inline auto Min(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Min(*this, o); }
  inline auto Max(const Value &o) const -> Value { return Type::GetInstance(type_id_)->Max(*this, o); }
  inline auto Sqrt() const -> Value { return Type::GetInstance(type_id_)->Sqrt(*this); }

  inline auto OperateNull(const Value &o) const -> Value { return Type::GetInstance(type_id_)->OperateNull(*this, o); }
  inline auto IsZero() const -> bool { return Type::GetInstance(type_id_)->IsZero(*this); }
  inline auto IsNull() const -> bool { return size_.len_ == vdbms_VALUE_NULL; }

  // Serialize this value into the given storage space. The inlined parameter
  // indicates whether we are allowed to inline this value into the storage
  // space, or whether we must store only a reference to this value. If inlined
  // is false, we may use the provided data pool to allocate space for this
  // value, storing a reference into the allocated pool space in the storage.
  inline void SerializeTo(char *storage) const { Type::GetInstance(type_id_)->SerializeTo(*this, storage); }

  // Deserialize a value of the given type from the given storage space.
  inline static auto DeserializeFrom(const char *storage, const TypeId type_id) -> Value {
    return Type::GetInstance(type_id)->DeserializeFrom(storage);
  }

  // Return a string version of this value
  inline auto ToString() const -> std::string { return Type::GetInstance(type_id_)->ToString(*this); }
  // Create a copy of this value
  inline auto Copy() const -> Value { return Type::GetInstance(type_id_)->Copy(*this); }

 protected:
  std::pair<double, double> cache_;

  // The actual value item
  union Val {
    int8_t boolean_;
    int8_t tinyint_;
    int16_t smallint_;
    int32_t integer_;
    int64_t bigint_;
    double decimal_;
    uint64_t timestamp_;
    char *varlen_;
    const char *const_varlen_;
  } value_;

  // 用于存储变长数据长度或元素类型ID的联合体
  union {
    uint32_t len_;
    TypeId elem_type_id_;
  } size_;

  bool manage_data_;
  // The data type
  TypeId type_id_;
};
}  // namespace vdbms

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<vdbms::Value, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const vdbms::Value &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<vdbms::Value, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<vdbms::Value> &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x->ToString(), ctx);
  }
};
