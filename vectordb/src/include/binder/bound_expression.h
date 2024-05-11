#pragma once

#include <memory>
#include <string>
#include "common/macros.h"
#include "fmt/format.h"

namespace vdbms {

/**
 * All types of expressions in binder.
 */
enum class ExpressionType : uint8_t {
  INVALID = 0,    /**< Invalid expression type. */
  CONSTANT = 1,   /**< Constant expression type. */
  COLUMN_REF = 3, /**< A column in a table. */
  TYPE_CAST = 4,  /**< Type cast expression type. */
  FUNCTION = 5,   /**< Function expression type. */
  AGG_CALL = 6,   /**< Aggregation function expression type. */
  STAR = 7,       /**< Star expression type, will be rewritten by binder and won't appear in plan. */
  UNARY_OP = 8,   /**< Unary expression type. */
  BINARY_OP = 9,  /**< Binary expression type. */
  ALIAS = 10,     /**< Alias expression type. */
  FUNC_CALL = 11, /**< Function call expression type. */
  WINDOW = 12,    /**< Window Aggregation expression type. */
};

/**
 * A bound expression.
 */
class BoundExpression {
 public:
  explicit BoundExpression(ExpressionType type) : type_(type) {}
  BoundExpression() = default;
  virtual ~BoundExpression() = default;

  virtual auto ToString() const -> std::string { return ""; };

  auto IsInvalid() const -> bool { return type_ == ExpressionType::INVALID; }

  virtual auto HasAggregation() const -> bool { UNREACHABLE("has aggregation should have been implemented!"); }

  virtual auto HasWindowFunction() const -> bool { return false; }

  /** The type of this expression. */
  ExpressionType type_{ExpressionType::INVALID};
};

}  // namespace vdbms

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<vdbms::BoundExpression, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const T &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<vdbms::BoundExpression, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x->ToString(), ctx);
  }
};

template <>
struct fmt::formatter<vdbms::ExpressionType> : formatter<string_view> {
  template <typename FormatContext>
  auto format(vdbms::ExpressionType c, FormatContext &ctx) const {
    string_view name;
    switch (c) {
      case vdbms::ExpressionType::INVALID:
        name = "Invalid";
        break;
      case vdbms::ExpressionType::CONSTANT:
        name = "Constant";
        break;
      case vdbms::ExpressionType::COLUMN_REF:
        name = "ColumnRef";
        break;
      case vdbms::ExpressionType::TYPE_CAST:
        name = "TypeCast";
        break;
      case vdbms::ExpressionType::FUNCTION:
        name = "Function";
        break;
      case vdbms::ExpressionType::AGG_CALL:
        name = "AggregationCall";
        break;
      case vdbms::ExpressionType::STAR:
        name = "Star";
        break;
      case vdbms::ExpressionType::UNARY_OP:
        name = "UnaryOperation";
        break;
      case vdbms::ExpressionType::BINARY_OP:
        name = "BinaryOperation";
        break;
      case vdbms::ExpressionType::ALIAS:
        name = "Alias";
        break;
      case vdbms::ExpressionType::FUNC_CALL:
        name = "FuncCall";
        break;
      case vdbms::ExpressionType::WINDOW:
        name = "Window";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
