#include "optimizer/optimizer.h"

namespace vdbms {

auto Optimizer::OptimizeSeqScanAsIndexScan(const vdbms::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  return plan;
}

}  // namespace vdbms
