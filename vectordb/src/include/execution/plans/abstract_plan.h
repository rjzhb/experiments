#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "fmt/format.h"

namespace vdbms {

#define vdbms_PLAN_NODE_CLONE_WITH_CHILDREN(cname)                                                          \
  auto CloneWithChildren(std::vector<AbstractPlanNodeRef> children) const->std::unique_ptr<AbstractPlanNode> \
      override {                                                                                             \
    auto plan_node = cname(*this);                                                                           \
    plan_node.children_ = children;                                                                          \
    return std::make_unique<cname>(std::move(plan_node));                                                    \
  }

/** PlanType 表示系统中计划类型的枚举。 */
enum class PlanType {
  SeqScan,
  IndexScan,
  Insert,
  Update,
  Delete,
  Aggregation,
  Limit,
  NestedLoopJoin,
  NestedIndexJoin,
  HashJoin,
  Filter,
  Values,
  Projection,
  Sort,
  TopN,
  TopNPerGroup,
  MockScan,
  InitCheck,
  Window,
  VectorIndexScan,
};

class AbstractPlanNode;
using AbstractPlanNodeRef = std::shared_ptr<const AbstractPlanNode>;

/**
 * AbstractPlanNode 表示系统中所有可能的计划节点类型。
 * 计划节点被建模为树形结构，因此每个计划节点可以有可变数量的子节点。
 * 按照 Volcano 模型，计划节点接收其子节点的元组。
 * 子节点的顺序可能很重要。
 */
class AbstractPlanNode {
 public:
  /**
   * 创建一个具有指定输出模式和子节点的新 AbstractPlanNode。
   * @param output_schema 此计划节点输出的模式
   * @param children 此计划节点的子节点
   */
  AbstractPlanNode(SchemaRef output_schema,
				   std::vector<AbstractPlanNodeRef> children)
	  : output_schema_(std::move(output_schema)), children_(std::move(children)) {}

  /** 虚析构函数。 */
  virtual ~AbstractPlanNode() = default;

  /** @return 此计划节点输出的模式 */
  auto OutputSchema() const -> const Schema & { return *output_schema_; }

  /** @return 此计划节点在索引 child_idx 处的子节点 */
  auto GetChildAt(uint32_t child_idx) const -> AbstractPlanNodeRef { return children_.at(child_idx); }

  /** @return 此计划节点的子节点 */
  auto GetChildren() const -> const std::vector<AbstractPlanNodeRef> & { return children_; }

  /** @return 此计划节点的类型 */
  virtual auto GetType() const -> PlanType = 0;

  /** @return 计划节点及其子节点的字符串表示 */
  auto ToString(bool with_schema = true) const -> std::string {
	if (with_schema) {
	  return fmt::format("{} | {}{}", PlanNodeToString(), output_schema_, ChildrenToString(2, with_schema));
	}
	return fmt::format("{}{}", PlanNodeToString(), ChildrenToString(2, with_schema));
  }

  /** @return 具有新子节点的克隆计划节点 */
  virtual auto CloneWithChildren(std::vector<AbstractPlanNodeRef> children) const
  -> std::unique_ptr<AbstractPlanNode> = 0;

  /**
   * 此计划节点的输出模式。在 Volcano 模型中，每个计划节点都会输出元组，
   * 这告诉您此计划节点的元组将具有哪个模式。
   */
  SchemaRef output_schema_;

  /** 此计划节点的子节点。 */
  std::vector<AbstractPlanNodeRef> children_;

 protected:
  /** @return 计划节点本身的字符串表示 */
  virtual auto PlanNodeToString() const -> std::string { return "<unknown>"; }

  /** @return 计划节点子节点的字符串表示 */
  auto ChildrenToString(int indent, bool with_schema = true) const -> std::string;

 private:
};

}  // namespace vdbms

template<typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<vdbms::AbstractPlanNode, T>::value, char>>
	: fmt::formatter<std::string> {
  template<typename FormatCtx>
  auto format(const T &x, FormatCtx &ctx) const {
	return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template<typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<vdbms::AbstractPlanNode, T>::value, char>>
	: fmt::formatter<std::string> {
  template<typename FormatCtx>
  auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
	return fmt::formatter<std::string>::format(x->ToString(), ctx);
  }
};
