#include "optimizer/optimizer.h"

namespace vdbms {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  return plan;
}

}  // namespace vdbms
