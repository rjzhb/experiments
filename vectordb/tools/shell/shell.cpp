#include <iostream>
#include <string>
#include "binder/binder.h"
#include "common/vdbms_instance.h"
#include "common/exception.h"
#include "common/util/string_util.h"
#include "concurrency/transaction.h"
#include "fmt/core.h"
#include "libfort/lib/fort.hpp"
#include "linenoise/linenoise.h"
#include "utf8proc/utf8proc.h"

#include "utf8proc/utf8proc.h"

// 计算UTF-8字符串的显示宽度
auto GetWidthOfUtf8(const void *beg, const void *end, size_t *width) -> int {
  size_t computed_width = 0; // 计算得到的宽度
  utf8proc_ssize_t n;
  // 计算字符串的大小
  utf8proc_ssize_t size = static_cast<const char *>(end) - static_cast<const char *>(beg);
  auto pstring = static_cast<utf8proc_uint8_t const *>(beg); // 指向字符串起始位置的指针
  utf8proc_int32_t data; // 存储当前字符的Unicode码点
  // 遍历字符串
  while ((n = utf8proc_iterate(pstring, size, &data)) > 0) {
	computed_width += utf8proc_charwidth(data); // 计算并累加字符的显示宽度
	pstring += n; // 移动指针，指向下一个字符
	size -= n; // 减少剩余大小
  }
  *width = computed_width; // 返回计算得到的宽度
  return 0;
}

// 主函数
auto main(int argc, char **argv) -> int {
  ft_set_u8strwid_func(&GetWidthOfUtf8); // 设置计算字符串宽度的函数

  auto vdbms = std::make_unique<vdbms::vdbmsInstance>("test.db"); // 创建数据库实例

  auto default_prompt = "vdbms> "; // 默认提示符
  auto emoji_prompt = "\U0001f6c1> ";  // 浴缸emoji作为提示符
  bool use_emoji_prompt = false; // 是否使用emoji提示符
  bool disable_tty = false; // 是否禁用TTY

  // 处理命令行参数
  for (int i = 1; i < argc; i++) {
	if (strcmp(argv[i], "--emoji-prompt") == 0) {
	  use_emoji_prompt = true;
	  break;
	}
	if (strcmp(argv[i], "--disable-tty") == 0) {
	  disable_tty = true;
	  break;
	}
  }

  vdbms->GenerateMockTable(); // 生成模拟表

  if (vdbms->buffer_pool_manager_ != nullptr) {
	vdbms->GenerateTestTable(); // 生成测试表
  }

  vdbms->EnableManagedTxn(); // 启用事务管理

  std::cout << "Welcome to the vdbms shell! Type \\help to learn more." << std::endl << std::endl;

  linenoiseHistorySetMaxLen(1024); // 设置历史记录最大长度
  linenoiseSetMultiLine(1); // 启用多行输入

  auto prompt = use_emoji_prompt ? emoji_prompt : default_prompt; // 根据参数选择提示符

  // 循环读取和处理用户输入
  while (true) {
	std::string query;
	bool first_line = true;
	while (true) {
	  std::string context_prompt = prompt; // 设置当前提示符
	  auto *txn = vdbms->CurrentManagedTxn(); // 获取当前事务
	  if (txn != nullptr) {
		// 根据事务状态设置提示符
		if (txn->GetTransactionState() != vdbms::TransactionState::RUNNING) {
		  context_prompt =
			  fmt::format("txn{} ({})> ", txn->GetTransactionIdHumanReadable(), txn->GetTransactionState());
		} else {
		  context_prompt = fmt::format("txn{}> ", txn->GetTransactionIdHumanReadable());
		}
	  }
	  std::string line_prompt = first_line ? context_prompt : "... "; // 设置行提示符
	  if (!disable_tty) {
		char *query_c_str = linenoise(line_prompt.c_str()); // 读取用户输入
		if (query_c_str == nullptr) {
		  return 0; // 如果用户输入为空，则退出
		}
		query += query_c_str; // 添加到查询字符串
		linenoiseFree(query_c_str); // 释放内存
		// 检查是否结束输入
		if (vdbms::StringUtil::EndsWith(query, ";") || vdbms::StringUtil::StartsWith(query, "\\")) {
		  break;
		}
		query += " "; // 添加空格，准备下一行输入
	  } else {
		// 如果禁用了TTY，直接从标准输入读取
		std::string query_line;
		std::cout << line_prompt;
		std::getline(std::cin, query_line);
		if (!std::cin) {
		  return 0; // 如果读取失败，则退出
		}
		query += query_line; // 添加到查询字符串
		if (vdbms::StringUtil::EndsWith(query, ";") || vdbms::StringUtil::StartsWith(query, "\\")) {
		  break;
		}
		query += "\n"; // 添加换行符，准备下一行输入
	  }
	  first_line = false; // 更新标志，表示已经处理过第一行
	}

	if (!disable_tty) {
	  linenoiseHistoryAdd(query.c_str()); // 添加到历史记录
	}

	// 尝试执行SQL查询
	try {
	  auto writer = vdbms::FortTableWriter(); // 创建表格写入器
	  vdbms->ExecuteSql(query, writer); // 执行SQL查询
	  // 打印查询结果
	  for (const auto &table : writer.tables_) {
		std::cout << table << std::flush;
	  }
	} catch (vdbms::Exception &ex) {
	  std::cerr << ex.what() << std::endl; // 打印异常信息
	}
  }

  return 0;
}
