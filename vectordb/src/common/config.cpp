//===----------------------------------------------------------------------===//
//
//                         vdbms
//
// config.cpp
//
// Identification: src/common/config.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/config.h"

namespace vdbms {

std::atomic<bool> enable_logging(false);

std::chrono::duration<int64_t> log_timeout = std::chrono::seconds(1);

std::chrono::milliseconds cycle_detection_interval = std::chrono::milliseconds(50);

std::atomic<bool> global_disable_execution_exception_print{false};

bool SIMD_ENABLED = false;

}  // namespace vdbms
