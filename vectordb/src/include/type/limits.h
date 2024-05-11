#pragma once

#include <cfloat>
#include <climits>
#include <cstdint>
#include <limits>

namespace vdbms {

static constexpr double DBL_LOWEST = std::numeric_limits<double>::lowest();
static constexpr double FLT_LOWEST = std::numeric_limits<float>::lowest();

static constexpr int8_t vdbms_INT8_MIN = (SCHAR_MIN + 1);
static constexpr int16_t vdbms_INT16_MIN = (SHRT_MIN + 1);
static constexpr int32_t vdbms_INT32_MIN = (INT_MIN + 1);
static constexpr int64_t vdbms_INT64_MIN = (LLONG_MIN + 1);
static constexpr double vdbms_DECIMAL_MIN = FLT_LOWEST;
static constexpr uint64_t vdbms_TIMESTAMP_MIN = 0;
static constexpr uint32_t vdbms_DATE_MIN = 0;
static constexpr int8_t vdbms_BOOLEAN_MIN = 0;

static constexpr int8_t vdbms_INT8_MAX = SCHAR_MAX;
static constexpr int16_t vdbms_INT16_MAX = SHRT_MAX;
static constexpr int32_t vdbms_INT32_MAX = INT_MAX;
static constexpr int64_t vdbms_INT64_MAX = LLONG_MAX;
static constexpr uint64_t vdbms_UINT64_MAX = ULLONG_MAX - 1;
static constexpr double vdbms_DECIMAL_MAX = DBL_MAX;
static constexpr uint64_t vdbms_TIMESTAMP_MAX = 11231999986399999999U;
static constexpr uint64_t vdbms_DATE_MAX = INT_MAX;
static constexpr int8_t vdbms_BOOLEAN_MAX = 1;

static constexpr uint32_t vdbms_VALUE_NULL = UINT_MAX;
static constexpr int8_t vdbms_INT8_NULL = SCHAR_MIN;
static constexpr int16_t vdbms_INT16_NULL = SHRT_MIN;
static constexpr int32_t vdbms_INT32_NULL = INT_MIN;
static constexpr int64_t vdbms_INT64_NULL = LLONG_MIN;
static constexpr uint64_t vdbms_DATE_NULL = 0;
static constexpr uint64_t vdbms_TIMESTAMP_NULL = ULLONG_MAX;
static constexpr double vdbms_DECIMAL_NULL = DBL_LOWEST;
static constexpr int8_t vdbms_BOOLEAN_NULL = SCHAR_MIN;

static constexpr uint32_t vdbms_VARCHAR_MAX_LEN = UINT_MAX;

// Use to make TEXT type as the alias of VARCHAR(TEXT_MAX_LENGTH)
static constexpr uint32_t vdbms_TEXT_MAX_LEN = 1000000000;

// Objects (i.e., VARCHAR) with length prefix of -1 are NULL
static constexpr int OBJECTLENGTH_NULL = -1;
}  // namespace vdbms
