#pragma once

#include <cmath>
#include <string>
#include <utility>
#include <vector>
#include <immintrin.h>
#include <avxintrin.h>

#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "fmt/format.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace vdbms {

/** ComparisonType represents the type of comparison that we want to perform. */
enum class VectorExpressionType { L2Dist, InnerProduct, CosineSimilarity };

inline auto ComputeDistance(const std::vector<double> &left, const std::vector<double> &right,
							VectorExpressionType dist_fn) {
  if (CACHE_ENABLED) {
	auto key = std::make_pair(left, right);
	auto iter = distance_cache.find(key);
	if (iter != distance_cache.end()) {
	  return iter->second;
	}
  }

  auto sz = left.size();
  vdbms_ASSERT(sz == right.size(), "vector length mismatched!");
  switch (dist_fn) {
	case VectorExpressionType::L2Dist: {
	  double dist = 0.0;
	  if (SIMD_ENABLED) {
#pragma omp simd reduction(+ : dist)
		for (size_t i = 0; i < sz; i++) {
		  double diff = left[i] - right[i];
		  dist += diff * diff;
		}
//		size_t i = 0;
//		__m256d sum = _mm256_setzero_pd();
//		for (; i <= sz - 4; i += 4) {
//		  __m256d vec_left = _mm256_load_pd(left.data() + i); // 改用对齐加载
//		  __m256d vec_right = _mm256_load_pd(right.data() + i);
//		  __m256d diff = _mm256_sub_pd(vec_left, vec_right);
//		  __m256d squared_diff = _mm256_mul_pd(diff, diff);
//		  sum = _mm256_add_pd(sum, squared_diff); // 直接在寄存器中累加
//		}
//		double result[4];
//		_mm256_store_pd(result, sum); // 只在最后一次写回内存
//		dist = result[0] + result[1] + result[2] + result[3];
//
//		for (; i < sz; ++i) {
//		  double diff = left[i] - right[i];
//		  dist += diff * diff;
//		}

	  } else {
		for (size_t i = 0; i < sz; i++) {
		  double diff = left[i] - right[i];
		  dist += diff * diff;
		}
	  }
	  if (CACHE_ENABLED) {
		distance_cache[{left, right}] = std::sqrt(dist);
		distance_cache[{right, left}] = std::sqrt(dist);
	  }
	  return std::sqrt(dist);
	}
	case VectorExpressionType::InnerProduct: {
	  double dist = 0.0;
	  if (SIMD_ENABLED) {
		__m256d sum_vec = _mm256_setzero_pd(); // 初始化为0的向量
		for (size_t i = 0; i < sz; i += 4) {
		  __m256d vec_left = _mm256_loadu_pd(&left[i]);
		  __m256d vec_right = _mm256_loadu_pd(&right[i]);
		  __m256d prod = _mm256_mul_pd(vec_left, vec_right);
		  sum_vec = _mm256_add_pd(sum_vec, prod);
		}
		double sum_array[4];
		_mm256_storeu_pd(sum_array, sum_vec);
		dist = sum_array[0] + sum_array[1] + sum_array[2] + sum_array[3];
		// 处理剩余的元素
		for (size_t i = sz - sz % 4; i < sz; ++i) {
		  dist += left[i] * right[i];
		}
	  } else {
		for (size_t i = 0; i < sz; i++) {
		  dist += left[i] * right[i];
		}
	  }

	  if (CACHE_ENABLED) {
		distance_cache[{left, right}] = -dist;
		distance_cache[{right, left}] = -dist;
	  }
	  return -dist;
	}
	case VectorExpressionType::CosineSimilarity: {
	  double dist = 0.0;
	  double norma = 0.0;
	  double normb = 0.0;
	  if (SIMD_ENABLED) {
		__m256d sum_vec = _mm256_setzero_pd();
		__m256d norma_vec = _mm256_setzero_pd();
		__m256d normb_vec = _mm256_setzero_pd();
		for (size_t i = 0; i < sz; i += 4) {
		  __m256d vec_left = _mm256_loadu_pd(&left[i]);
		  __m256d vec_right = _mm256_loadu_pd(&right[i]);
		  __m256d prod = _mm256_mul_pd(vec_left, vec_right);
		  sum_vec = _mm256_add_pd(sum_vec, prod);
		  norma_vec = _mm256_add_pd(norma_vec, _mm256_mul_pd(vec_left, vec_left));
		  normb_vec = _mm256_add_pd(normb_vec, _mm256_mul_pd(vec_right, vec_right));
		}
		double sum_array[4], norma_array[4], normb_array[4];
		_mm256_storeu_pd(sum_array, sum_vec);
		_mm256_storeu_pd(norma_array, norma_vec);
		_mm256_storeu_pd(normb_array, normb_vec);
		dist = sum_array[0] + sum_array[1] + sum_array[2] + sum_array[3];
		norma = norma_array[0] + norma_array[1] + norma_array[2] + norma_array[3];
		normb = normb_array[0] + normb_array[1] + normb_array[2] + normb_array[3];
		// 处理剩余的元素
		for (size_t i = sz - sz % 4; i < sz; ++i) {
		  dist += left[i] * right[i];
		  norma += left[i] * left[i];
		  normb += right[i] * right[i];
		}
	  } else {
		for (size_t i = 0; i < sz; i++) {
		  dist += left[i] * right[i];
		  norma += left[i] * left[i];
		  normb += right[i] * right[i];
		}
	  }
	  auto similarity = dist / std::sqrt(norma * normb);

	  if (CACHE_ENABLED) {
		// Store computed distance in cache
		distance_cache[{left, right}] = 1.0 - similarity;
		distance_cache[{right, left}] = 1.0 - similarity;
	  }
	  return 1.0 - similarity;
	}
	default:vdbms_ASSERT(false, "Unsupported vector expr type.");
  }
}

class VectorExpression : public AbstractExpression {
 public:
  VectorExpression(VectorExpressionType expr_type, AbstractExpressionRef left, AbstractExpressionRef right)
	  : AbstractExpression({std::move(left), std::move(right)}, Column{"<val>", TypeId::DECIMAL}),
		expr_type_{expr_type} {}

  auto Evaluate(const Tuple *tuple, const Schema &schema) const -> Value override {
	Value lhs = GetChildAt(0)->Evaluate(tuple, schema);
	Value rhs = GetChildAt(1)->Evaluate(tuple, schema);
	return ValueFactory::GetDecimalValue(PerformComputation(lhs, rhs));
  }

  auto EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
					const Schema &right_schema) const -> Value override {
	Value lhs = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
	Value rhs = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
	return ValueFactory::GetDecimalValue(PerformComputation(lhs, rhs));
  }

  /** @return the string representation of the expression node and its children */
  auto ToString() const -> std::string override {
	return fmt::format("{}({}, {})", expr_type_, *GetChildAt(0), *GetChildAt(1));
  }

  vdbms_EXPR_CLONE_WITH_CHILDREN(VectorExpression);

  VectorExpressionType expr_type_;

 private:
  auto PerformComputation(const Value &lhs, const Value &rhs) const -> double {
	auto left_vec = lhs.GetVector();
	auto right_vec = rhs.GetVector();
	return ComputeDistance(left_vec, right_vec, expr_type_);
  }
};

}  // namespace vdbms

template<>
struct fmt::formatter<vdbms::VectorExpressionType> : formatter<string_view> {
  template<typename FormatContext>
  auto format(vdbms::VectorExpressionType c, FormatContext &ctx) const {
	string_view name;
	switch (c) {
	  case vdbms::VectorExpressionType::L2Dist:name = "l2_dist";
		break;
	  case vdbms::VectorExpressionType::CosineSimilarity:name = "cosine_similarity";
		break;
	  case vdbms::VectorExpressionType::InnerProduct:name = "inner_product";
		break;
	  default:name = "Unknown";
		break;
	}
	return formatter<string_view>::format(name, ctx);
  }
};
