#include <cmath>

#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include "binder/binder.h"
#include "common/vdbms_instance.h"
#include "common/exception.h"
#include "common/util/string_util.h"
#include "fmt/core.h"
#include "linenoise/linenoise.h"
#include "utf8proc/utf8proc.h"

static std::unique_ptr<vdbms::vdbmsInstance> instance = nullptr;

extern "C" {

auto vdbmsInit() -> int {
  std::cout << "Initialize vdbms..." << std::endl;
  auto vdbms = std::make_unique<vdbms::vdbmsInstance>();
  vdbms->GenerateMockTable();

  if (vdbms->buffer_pool_manager_ != nullptr) {
    vdbms->GenerateTestTable();
  }

  vdbms->EnableManagedTxn();

  instance = std::move(vdbms);
  return 0;
}

auto vdbmsExecuteQuery(const char *input, char *prompt, char *output, uint16_t len) -> int {
  std::string input_string(input);
  std::cout << input_string << std::endl;
  std::string output_string;
  std::string output_prompt;
  try {
    auto writer = vdbms::HtmlWriter();
    instance->ExecuteSql(input_string, writer);
    output_string = writer.ss_.str();
  } catch (vdbms::Exception &ex) {
    output_string = ex.what();
  }
  auto txn = instance->CurrentManagedTxn();
  if (txn != nullptr) {
    output_prompt = fmt::format("txn{}", txn->GetTransactionIdHumanReadable());
  }
  strncpy(output, output_string.c_str(), len);
  strncpy(prompt, output_prompt.c_str(), len);
  if (output_string.length() >= len) {
    return 1;
  }
  return 0;
}
}
