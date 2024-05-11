
#include "recovery/checkpoint_manager.h"

namespace vdbms {

void CheckpointManager::BeginCheckpoint() {
  // Block all the transactions and ensure that both the WAL and all dirty buffer pool pages are persisted to disk,
  // creating a consistent checkpoint. Do NOT allow transactions to resume at the end of this method, resume them
  // in CheckpointManager::EndCheckpoint() instead. This is for grading purposes.
}

void CheckpointManager::EndCheckpoint() {
  // Allow transactions to resume, completing the checkpoint.
}

}  // namespace vdbms
