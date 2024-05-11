//===----------------------------------------------------------------------===//
//
//                         vdbms
//
// statement_type.h
//
// Identification: src/include/enums/statement_type.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/config.h"
#include "fmt/format.h"

namespace vdbms {

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//
enum class StatementType : uint8_t {
  INVALID_STATEMENT,        // invalid statement type
  SELECT_STATEMENT,         // select statement type
  INSERT_STATEMENT,         // insert statement type
  UPDATE_STATEMENT,         // update statement type
  CREATE_STATEMENT,         // create statement type
  DELETE_STATEMENT,         // delete statement type
  EXPLAIN_STATEMENT,        // explain statement type
  DROP_STATEMENT,           // drop statement type
  INDEX_STATEMENT,          // index statement type
  VARIABLE_SET_STATEMENT,   // set variable statement type
  VARIABLE_SHOW_STATEMENT,  // show variable statement type
  TRANSACTION_STATEMENT,    // txn statement type
};

}  // namespace vdbms

template <>
struct fmt::formatter<vdbms::StatementType> : formatter<string_view> {
  template <typename FormatContext>
  auto format(vdbms::StatementType c, FormatContext &ctx) const {
    string_view name;
    switch (c) {
      case vdbms::StatementType::INVALID_STATEMENT:
        name = "Invalid";
        break;
      case vdbms::StatementType::SELECT_STATEMENT:
        name = "Select";
        break;
      case vdbms::StatementType::INSERT_STATEMENT:
        name = "Insert";
        break;
      case vdbms::StatementType::UPDATE_STATEMENT:
        name = "Update";
        break;
      case vdbms::StatementType::CREATE_STATEMENT:
        name = "Create";
        break;
      case vdbms::StatementType::DELETE_STATEMENT:
        name = "Delete";
        break;
      case vdbms::StatementType::EXPLAIN_STATEMENT:
        name = "Explain";
        break;
      case vdbms::StatementType::DROP_STATEMENT:
        name = "Drop";
        break;
      case vdbms::StatementType::INDEX_STATEMENT:
        name = "Index";
        break;
      case vdbms::StatementType::VARIABLE_SHOW_STATEMENT:
        name = "VariableShow";
        break;
      case vdbms::StatementType::VARIABLE_SET_STATEMENT:
        name = "VariableSet";
        break;
      case vdbms::StatementType::TRANSACTION_STATEMENT:
        name = "Transaction";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
