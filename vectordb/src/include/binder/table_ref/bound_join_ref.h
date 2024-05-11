#pragma once

#include <memory>
#include <string>
#include <utility>

#include "binder/bound_expression.h"
#include "binder/bound_table_ref.h"
#include "fmt/format.h"

namespace vdbms {

/**
 * Join types.
 */
enum class JoinType : uint8_t {
  INVALID = 0, /**< Invalid join type. */
  LEFT = 1,    /**< Left join. */
  RIGHT = 3,   /**< Right join. */
  INNER = 4,   /**< Inner join. */
  OUTER = 5    /**< Outer join. */
};

/**
 * A join. e.g., `SELECT * FROM x INNER JOIN y ON ...`, where `x INNER JOIN y ON ...` is `BoundJoinRef`.
 */
class BoundJoinRef : public BoundTableRef {
 public:
  explicit BoundJoinRef(JoinType join_type, std::unique_ptr<BoundTableRef> left, std::unique_ptr<BoundTableRef> right,
                        std::unique_ptr<BoundExpression> condition)
      : BoundTableRef(TableReferenceType::JOIN),
        join_type_(join_type),
        left_(std::move(left)),
        right_(std::move(right)),
        condition_(std::move(condition)) {}

  auto ToString() const -> std::string override {
    return fmt::format("BoundJoin {{ type={}, left={}, right={}, condition={} }}", join_type_, left_, right_,
                       condition_);
  }

  /** Type of join. */
  JoinType join_type_;

  /** The left side of the join. */
  std::unique_ptr<BoundTableRef> left_;

  /** The right side of the join. */
  std::unique_ptr<BoundTableRef> right_;

  /** Join condition. */
  std::unique_ptr<BoundExpression> condition_;
};
}  // namespace vdbms

template <>
struct fmt::formatter<vdbms::JoinType> : formatter<string_view> {
  template <typename FormatContext>
  auto format(vdbms::JoinType c, FormatContext &ctx) const {
    string_view name;
    switch (c) {
      case vdbms::JoinType::INVALID:
        name = "Invalid";
        break;
      case vdbms::JoinType::LEFT:
        name = "Left";
        break;
      case vdbms::JoinType::RIGHT:
        name = "Right";
        break;
      case vdbms::JoinType::INNER:
        name = "Inner";
        break;
      case vdbms::JoinType::OUTER:
        name = "Outer";
        break;
      default:
        name = "Unknown";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
