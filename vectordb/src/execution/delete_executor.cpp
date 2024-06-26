#include <memory>

#include "execution/executors/delete_executor.h"

namespace vdbms {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
							   std::unique_ptr<AbstractExecutor> &&child_executor)
	: AbstractExecutor(exec_ctx) {}

void DeleteExecutor::Init() { throw NotImplementedException("DeleteExecutor is not implemented"); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace vdbms
