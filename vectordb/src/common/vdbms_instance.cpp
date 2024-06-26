#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <tuple>

#include "binder/binder.h"
#include "binder/bound_expression.h"
#include "binder/bound_statement.h"
#include "binder/statement/create_statement.h"
#include "binder/statement/explain_statement.h"
#include "binder/statement/index_statement.h"
#include "binder/statement/select_statement.h"
#include "binder/statement/set_show_statement.h"
#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "catalog/table_generator.h"
#include "common/vdbms_instance.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/util/string_util.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/check_options.h"
#include "execution/execution_common.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/mock_scan_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "optimizer/optimizer.h"
#include "planner/planner.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "type/value_factory.h"

namespace vdbms {

auto vdbmsInstance::MakeExecutorContext(Transaction *txn, bool is_modify) -> std::unique_ptr<ExecutorContext> {
  return std::make_unique<ExecutorContext>(txn, catalog_.get(), buffer_pool_manager_.get(), txn_manager_.get(),
										   lock_manager_.get(), is_modify);
}

vdbmsInstance::vdbmsInstance(const std::string &db_file_name, size_t bpm_size) {
  enable_logging = false;

  // Storage related.
  disk_manager_ = std::make_unique<DiskManager>(db_file_name);

#ifndef DISABLE_CHECKPOINT_MANAGER
  // Log related.
  log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
#endif

  // We need more frames for GenerateTestTable to work. Therefore, we use 128 instead of the default
  // buffer pool size specified in `config.h`.
  try {
	buffer_pool_manager_ =
		std::make_unique<BufferPoolManager>(bpm_size, disk_manager_.get(), LRUK_REPLACER_K, log_manager_.get());
  } catch (NotImplementedException &e) {
	std::cerr << "BufferPoolManager is not implemented, only mock tables are supported." << std::endl;
	buffer_pool_manager_ = nullptr;
  }

// Transaction (txn) related.
#ifndef DISABLE_LOCK_MANAGER
  lock_manager_ = std::make_unique<LockManager>();
  txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get());
#else
  txn_manager_ = std::make_unique<TransactionManager>();
#endif

#ifndef DISABLE_LOCK_MANAGER
  lock_manager_->txn_manager_ = txn_manager_.get();

#ifndef __EMSCRIPTEN__
  lock_manager_->StartDeadlockDetection();
#endif

#endif

#ifndef DISABLE_CHECKPOINT_MANAGER
  // Checkpoint related.
  checkpoint_manager_ =
	  std::make_unique<CheckpointManager>(txn_manager_.get(), log_manager_.get(), buffer_pool_manager_.get());
#endif

  // Catalog related.
  catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get(), lock_manager_.get(), log_manager_.get());

  txn_manager_->catalog_ = catalog_.get();

  // Execution engine related.
  execution_engine_ = std::make_unique<ExecutionEngine>(buffer_pool_manager_.get(), txn_manager_.get(), catalog_.get());
}

vdbmsInstance::vdbmsInstance(size_t bpm_size) {
  enable_logging = false;

  // Storage related.
  disk_manager_ = std::make_unique<DiskManagerUnlimitedMemory>();

#ifndef DISABLE_CHECKPOINT_MANAGER
  // Log related.
  log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
#endif

  // We need more frames for GenerateTestTable to work. Therefore, we use 128 instead of the default
  // buffer pool size specified in `config.h`.
  try {
	buffer_pool_manager_ =
		std::make_unique<BufferPoolManager>(bpm_size, disk_manager_.get(), LRUK_REPLACER_K, log_manager_.get());
  } catch (NotImplementedException &e) {
	std::cerr << "BufferPoolManager is not implemented, only mock tables are supported." << std::endl;
	buffer_pool_manager_ = nullptr;
  }

#ifndef DISABLE_LOCK_MANAGER
  lock_manager_ = std::make_unique<LockManager>();
  txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get());
#else
  txn_manager_ = std::make_unique<TransactionManager>();
#endif

#ifndef DISABLE_LOCK_MANAGER
  lock_manager_->txn_manager_ = txn_manager_.get();

#ifndef __EMSCRIPTEN__
  lock_manager_->StartDeadlockDetection();
#endif
#endif

#ifndef DISABLE_CHECKPOINT_MANAGER
  // Checkpoint related.
  checkpoint_manager_ =
	  std::make_unique<CheckpointManager>(txn_manager_.get(), log_manager_.get(), buffer_pool_manager_.get());
#endif

  // Catalog related.
  catalog_ = std::make_unique<Catalog>(buffer_pool_manager_.get(), lock_manager_.get(), log_manager_.get());

  txn_manager_->catalog_ = catalog_.get();

  // Execution engine related.
  execution_engine_ = std::make_unique<ExecutionEngine>(buffer_pool_manager_.get(), txn_manager_.get(), catalog_.get());
}

void vdbmsInstance::CmdDbgMvcc(const std::vector<std::string> &params, ResultWriter &writer) {
  if (params.size() != 2) {
	writer.OneCell("please provide a table name");
	return;
  }
  const auto &table = params[1];
  writer.OneCell("please view the result in the vdbms console (or Chrome DevTools console), table=" + table);
  std::shared_lock<std::shared_mutex> lck(catalog_lock_);
  auto table_info = catalog_->GetTable(table);
  if (table_info == nullptr) {
	writer.OneCell("table " + table + " not found");
	return;
  }
  TxnMgrDbg("\\dbgmvcc", txn_manager_.get(), table_info, table_info->table_.get());
}

void vdbmsInstance::CmdDisplayTables(ResultWriter &writer) {
  auto table_names = catalog_->GetTableNames();
  writer.BeginTable(false);
  writer.BeginHeader();
  writer.WriteHeaderCell("oid");
  writer.WriteHeaderCell("name");
  writer.WriteHeaderCell("cols");
  writer.EndHeader();
  for (const auto &name : table_names) {
	writer.BeginRow();
	const auto *table_info = catalog_->GetTable(name);
	writer.WriteCell(fmt::format("{}", table_info->oid_));
	writer.WriteCell(table_info->name_);
	writer.WriteCell(table_info->schema_.ToString());
	writer.EndRow();
  }
  writer.EndTable();
}

void vdbmsInstance::CmdDisplayIndices(ResultWriter &writer) {
  auto table_names = catalog_->GetTableNames();
  writer.BeginTable(false);
  writer.BeginHeader();
  writer.WriteHeaderCell("table_name");
  writer.WriteHeaderCell("index_oid");
  writer.WriteHeaderCell("index_name");
  writer.WriteHeaderCell("index_cols");
  writer.EndHeader();
  for (const auto &table_name : table_names) {
	for (const auto *index_info : catalog_->GetTableIndexes(table_name)) {
	  writer.BeginRow();
	  writer.WriteCell(table_name);
	  writer.WriteCell(fmt::format("{}", index_info->index_oid_));
	  writer.WriteCell(index_info->name_);
	  writer.WriteCell(index_info->key_schema_.ToString());
	  writer.EndRow();
	}
  }
  writer.EndTable();
}

void vdbmsInstance::WriteOneCell(const std::string &cell, ResultWriter &writer) { writer.OneCell(cell); }

void vdbmsInstance::CmdDisplayHelp(ResultWriter &writer) {
  std::string help = R"(Welcome to the vdbms shell!

\dt: show all tables
\di: show all indices
\dbgmvcc <table>: show version chain of a table
\help: show this message again
\txn: show current txn information
\txn <txn_id>: switch to txn
\txn gc: run garbage collection
\txn -1: exit txn mode

vdbms shell currently only supports a small set of Postgres queries. We'll set
up a doc describing the current status later. It will silently ignore some parts
of the query, so it's normal that you'll get a wrong result when executing
unsupported SQL queries. This shell will be able to run `create table` only
after you have completed the buffer pool manager. It will be able to execute SQL
queries after you have implemented necessary query executors. Use `explain` to
see the execution plan of your query.
)";
  WriteOneCell(help, writer);
}

// 在数据库实例上执行给定的SQL语句。
// 这个方法负责管理事务的生命周期（如果需要），并处理SQL执行过程中可能出现的异常。
auto vdbmsInstance::ExecuteSql(const std::string &sql,
							   ResultWriter &writer,
							   std::shared_ptr<CheckOptions> check_options) -> bool {
  // 检查当前是否有活动的事务。
  bool is_local_txn = current_txn_ != nullptr;
  // 如果有活动的事务，则使用它；否则，从事务管理器开始一个新的事务。
  auto *txn = is_local_txn ? current_txn_ : txn_manager_->Begin();

  try {
	// 执行SQL语句。如果成功，根据事务的类型（本地或全局）来提交事务。
	auto result = ExecuteSqlTxn(sql, writer, txn, std::move(check_options));
	// 如果这不是一个本地事务（即，这个事务是为这次SQL执行新创建的），则尝试提交事务。
	if (!is_local_txn) {
	  auto res = txn_manager_->Commit(txn);
	  // 如果提交失败，抛出异常。
	  if (!res) {
		throw Exception("failed to commit txn");
	  }
	}
	// 返回SQL执行的结果。
	return result;
  } catch (vdbms::Exception &ex) {
	// 如果在SQL执行过程中发生异常，中止事务，并清理当前事务指针，然后重新抛出异常。
	txn_manager_->Abort(txn);
	current_txn_ = nullptr;
	throw ex;
  }
}

// 在特定事务环境下执行SQL语句，处理内部命令和标准SQL查询。
auto vdbmsInstance::ExecuteSqlTxn(const std::string &sql,
								  ResultWriter &writer,
								  Transaction *txn,
								  std::shared_ptr<CheckOptions> check_options) -> bool {
  // 检查是否是内部命令（以'\'开头的命令，类似于psql的内部命令）
  if (!sql.empty() && sql[0] == '\\') {
	// 处理不同的内部命令
	if (sql == "\\dt") {
	  CmdDisplayTables(writer); // 显示数据库中的所有表
	  return true;
	}
	if (sql == "\\di") {
	  CmdDisplayIndices(writer); // 显示数据库中的所有索引
	  return true;
	}
	if (sql == "\\help") {
	  CmdDisplayHelp(writer); // 显示帮助信息
	  return true;
	}
	if (StringUtil::StartsWith(sql, "\\dbgmvcc")) {
	  auto split = StringUtil::Split(sql, " ");
	  CmdDbgMvcc(split, writer); // 调试MVCC信息
	  return true;
	}
	if (StringUtil::StartsWith(sql, "\\txn")) {
	  auto split = StringUtil::Split(sql, " ");
	  CmdTxn(split, writer); // 处理事务相关的命令
	  return true;
	}
	// 抛出异常，未知的内部命令
	throw Exception(fmt::format("unsupported internal command: {}", sql));
  }

  bool is_successful = true; // 记录执行是否成功

  // 获取对目录的共享锁
  std::shared_lock<std::shared_mutex> l(catalog_lock_);
  // 绑定和解析SQL语句
  vdbms::Binder binder(*catalog_);
  binder.ParseAndSave(sql);
  l.unlock(); // 解锁目录

  // 遍历所有解析后的语句节点
  for (auto *stmt : binder.statement_nodes_) {
	auto statement = binder.BindStatement(stmt); // 绑定语句

	bool is_delete = false;

	// 根据语句类型进行不同的处理
	switch (statement->type_) {
	  case StatementType::CREATE_STATEMENT: {
		const auto &create_stmt = dynamic_cast<const CreateStatement &>(*statement);
		HandleCreateStatement(txn, create_stmt, writer); // 处理创建表语句
		continue;
	  }
	  case StatementType::INDEX_STATEMENT: {
		const auto &index_stmt = dynamic_cast<const IndexStatement &>(*statement);
		HandleIndexStatement(txn, index_stmt, writer); // 处理创建索引语句
		continue;
	  }
	  case StatementType::VARIABLE_SHOW_STATEMENT: {
		const auto &show_stmt = dynamic_cast<const VariableShowStatement &>(*statement);
		HandleVariableShowStatement(txn, show_stmt, writer); // 处理显示变量语句
		continue;
	  }
	  case StatementType::VARIABLE_SET_STATEMENT: {
		const auto &set_stmt = dynamic_cast<const VariableSetStatement &>(*statement);
		HandleVariableSetStatement(txn, set_stmt, writer); // 处理设置变量语句
		continue;
	  }
	  case StatementType::EXPLAIN_STATEMENT: {
		const auto &explain_stmt = dynamic_cast<const ExplainStatement &>(*statement);
		HandleExplainStatement(txn, explain_stmt, writer); // 处理解释计划语句
		continue;
	  }
	  case StatementType::TRANSACTION_STATEMENT: {
		const auto &txn_stmt = dynamic_cast<const TransactionStatement &>(*statement);
		HandleTxnStatement(txn, txn_stmt, writer); // 处理事务语句
		continue;
	  }
	  case StatementType::DELETE_STATEMENT:
	  case StatementType::UPDATE_STATEMENT: is_delete = true; // 标记为删除或更新语句
	  default: break;
	}

	// 再次获取目录的共享锁
	std::shared_lock<std::shared_mutex> l(catalog_lock_);

	// 规划查询
	vdbms::Planner planner(*catalog_);
	planner.PlanQuery(*statement);

	// 优化查询
	vdbms::Optimizer optimizer(*catalog_, session_variables_);
	auto optimized_plan = optimizer.Optimize(planner.plan_);

	l.unlock(); // 解锁目录

	// 执行查询
	auto exec_ctx = MakeExecutorContext(txn, is_delete);
	if (check_options != nullptr) {
	  exec_ctx->InitCheckOptions(std::move(check_options)); // 初始化检查选项
	}
	std::vector<Tuple> result_set{}; // 结果集
	is_successful &= execution_engine_->Execute(optimized_plan, &result_set, txn, exec_ctx.get()); // 执行优化后的计划

	// 将结果集转换为字符串并写入writer
	auto schema = planner.plan_->OutputSchema(); // 获取输出模式

	// 生成结果集的表头
	writer.BeginTable(false);
	writer.BeginHeader();
	for (const auto &column : schema.GetColumns()) {
	  writer.WriteHeaderCell(column.GetName()); // 写入列名
	}
	writer.EndHeader();

	// 将结果集的每一行转换为字符串
	for (const auto &tuple : result_set) {
	  writer.BeginRow();
	  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
		writer.WriteCell(tuple.GetValue(&schema, i).ToString()); // 写入每个单元格的值
	  }
	  writer.EndRow();
	}
	writer.EndTable();
  }

  return is_successful; // 返回执行结果
}

/**
 * FOR TEST ONLY. Generate test tables in this vdbms instance.
 * It's used in the shell to predefine some tables, as we don't support
 * create / drop table and insert for now. Should remove it in the future.
 */
void vdbmsInstance::GenerateTestTable() {
  auto *txn = txn_manager_->Begin();
  auto exec_ctx = MakeExecutorContext(txn, false);
  TableGenerator gen{exec_ctx.get()};

  std::shared_lock<std::shared_mutex> l(catalog_lock_);
  gen.GenerateTestTables();
  l.unlock();

  txn_manager_->Commit(txn);
}

void vdbmsInstance::GenerateHighDTestTable(int D, int N, vdbms::vdbmsInstance* vdbms) {
  std::string createTableQuery = "CREATE TABLE t1(v1 VECTOR(" + std::to_string(D) + "), v2 integer);"; // 创建表的SQL

  std::mt19937 rng(std::random_device{}()); // 随机数生成器
  std::uniform_real_distribution<double> dist(-10, 10); // 设定随机数范围

  auto writer = vdbms::FortTableWriter(); // 创建表格写入器
  vdbms->ExecuteSql(createTableQuery, writer); // 执行创建表格的SQL查询

  for (int i = 0; i < N; ++i) {
	std::ostringstream insertQueryStream;
	insertQueryStream << "INSERT INTO t1 VALUES (ARRAY [";
	for (int j = 0; j < D; ++j) { // 每条数据为D维
	  insertQueryStream << dist(rng); // 使用ceil函数向上取整
	  if (j < D - 1) insertQueryStream << ", ";
	}
	insertQueryStream << "], " << static_cast<int>(std::ceil(dist(rng))) << ");"; // 每条数据后跟一个向上取整的随机整数
	auto insertQuery = insertQueryStream.str();
	vdbms->ExecuteSql(insertQuery, writer); // 执行插入数据的SQL查询
  }
}

/**
 * FOR TEST ONLY. Generate test tables in this vdbms instance.
 * It's used in the shell to predefine some tables, as we don't support
 * create / drop table and insert for now. Should remove it in the future.
 */
void vdbmsInstance::GenerateMockTable() {
  // The actual content generated by mock scan executors are described in `mock_scan_executor.cpp`.
  auto txn = txn_manager_->Begin();

  std::shared_lock<std::shared_mutex> l(catalog_lock_);
  for (auto table_name = &mock_table_list[0]; *table_name != nullptr; table_name++) {
	catalog_->CreateTable(txn, *table_name, GetMockTableSchemaOf(*table_name), false);
  }
  l.unlock();

  txn_manager_->Commit(txn);
}

vdbmsInstance::~vdbmsInstance() {
  if (enable_logging) {
	log_manager_->StopFlushThread();
  }
}

/** Enable managed txn mode on this vdbms instance, allowing statements like `BEGIN`. */
void vdbmsInstance::EnableManagedTxn() { managed_txn_mode_ = true; }

/** Get the current transaction. */
auto vdbmsInstance::CurrentManagedTxn() -> Transaction * { return current_txn_; }

void vdbmsInstance::CmdTxn(const std::vector<std::string> &params,
						   ResultWriter &writer) {
  if (!managed_txn_mode_) {
	writer.OneCell("only supported in managed mode, please use vdbms-shell");
	return;
  }
  auto dump_current_txn = [&](const std::string &prefix) {
	writer.OneCell(fmt::format("{}txn_id={} txn_real_id={} read_ts={} commit_ts={} status={} iso_lvl={}", prefix,
							   current_txn_->GetTransactionIdHumanReadable(), current_txn_->GetTransactionId(),
							   current_txn_->GetReadTs(), current_txn_->GetCommitTs(),
							   current_txn_->GetTransactionState(), current_txn_->GetIsolationLevel()));
  };
  if (params.size() == 1) {
	if (current_txn_ != nullptr) {
	  dump_current_txn("");
	} else {
	  writer.OneCell("no active txn, each statement starts a new txn.");
	}
	return;
  }
  if (params.size() == 2) {
	const std::string &param1 = params[1];
	if (param1 == "gc") {
	  txn_manager_->GarbageCollection();
	  writer.OneCell("GC complete");
	  return;
	}
	auto txn_id = std::stoi(param1);
	if (txn_id == -1) {
	  dump_current_txn("pause current txn ");
	  current_txn_ = nullptr;
	  return;
	}
	auto iter = txn_manager_->txn_map_.find(txn_id);
	if (iter == txn_manager_->txn_map_.end()) {
	  iter = txn_manager_->txn_map_.find(txn_id + TXN_START_ID);
	  if (iter == txn_manager_->txn_map_.end()) {
		writer.OneCell("cannot find txn.");
		return;
	  }
	}
	current_txn_ = iter->second.get();
	dump_current_txn("switch to new txn ");
	return;
  }
  writer.OneCell("unsupported txn cmd.");
}

}  // namespace vdbms
