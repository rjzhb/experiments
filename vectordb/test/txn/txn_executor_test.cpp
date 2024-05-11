#include "execution/execution_common.h"
#include "txn_common.h"  // NOLINT

namespace vdbms {

// NOLINTBEGIN(bugprone-unchecked-optional-access)

TEST(TxnExecutorTest, DISABLED_InsertTest) {  // NOLINT
  auto vdbms = std::make_unique<vdbmsInstance>();
  auto empty_table = IntResult{};
  Execute(*vdbms, "CREATE TABLE maintable(a int)");
  auto table_info = vdbms->catalog_->GetTable("maintable");
  auto txn1 = BeginTxn(*vdbms, "txn1");
  auto txn2 = BeginTxn(*vdbms, "txn2");
  auto txn_ref = BeginTxn(*vdbms, "txn_ref");

  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (1)"));
  WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (2)"));

  TxnMgrDbg("after insertion", vdbms->txn_manager_.get(), table_info, table_info->table_.get());

  const std::string query = "SELECT a FROM maintable";
  fmt::println(stderr, "A: check scan txn1");
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
  fmt::println(stderr, "B: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2}}));

  auto txn3 = BeginTxn(*vdbms, "txn3");
  fmt::println(stderr, "C: check scan txn3");
  WithTxn(txn3, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
}

TEST(TxnExecutorTest, DISABLED_InsertCommitTest) {  // NOLINT
  auto vdbms = std::make_unique<vdbmsInstance>();
  Execute(*vdbms, "CREATE TABLE maintable(a int)");
  auto table_info = vdbms->catalog_->GetTable("maintable");
  auto txn1 = BeginTxn(*vdbms, "txn1");
  auto txn2 = BeginTxn(*vdbms, "txn2");

  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (1)"));
  WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (2)"));
  TxnMgrDbg("after insertion", vdbms->txn_manager_.get(), table_info, table_info->table_.get());

  const std::string query = "SELECT a FROM maintable";
  fmt::println(stderr, "A: check scan txn1");
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
  fmt::println(stderr, "B: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2}}));
  WithTxn(txn1, CommitTxn(*vdbms, _var, _txn));
  TxnMgrDbg("after commit txn1", vdbms->txn_manager_.get(), table_info, table_info->table_.get());

  auto txn_ref = BeginTxn(*vdbms, "txn_ref");

  auto txn3 = BeginTxn(*vdbms, "txn3");
  fmt::println(stderr, "C: check scan txn3");
  WithTxn(txn3, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
  fmt::println(stderr, "D: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2}}));
  WithTxn(txn3, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (3)"));
  TxnMgrDbg("after insert into txn3", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  fmt::println(stderr, "E: check scan txn3");
  WithTxn(txn3, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {3}}));
  fmt::println(stderr, "F: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2}}));
  WithTxn(txn3, CommitTxn(*vdbms, _var, _txn));
  WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
  TxnMgrDbg("after commit txn2", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  auto txn4 = BeginTxn(*vdbms, "txn4");
  fmt::println(stderr, "G: check scan txn4");
  WithTxn(txn4, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}, {3}}));
  WithTxn(txn4, CommitTxn(*vdbms, _var, _txn));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
}

TEST(TxnExecutorTest, DISABLED_InsertDeleteTest) {  // NOLINT
  auto vdbms = std::make_unique<vdbmsInstance>();
  auto empty_table = IntResult{};
  Execute(*vdbms, "CREATE TABLE maintable(a int)");
  auto table_info = vdbms->catalog_->GetTable("maintable");
  auto txn1 = BeginTxn(*vdbms, "txn1");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (1)"));
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (2)"));
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (3)"));
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 3"));
  TxnMgrDbg("after 3 insert + 1 delete", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  fmt::println(stderr, "A: check scan txn1");
  const auto query = "SELECT a FROM maintable";
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}}));
  WithTxn(txn1, CommitTxn(*vdbms, _var, _txn));
  TxnMgrDbg("after commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  auto txn_ref = BeginTxn(*vdbms, "txn_ref");
  auto txn2 = BeginTxn(*vdbms, "txn2");
  fmt::println(stderr, "B: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}}));
  WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 2"));
  TxnMgrDbg("after txn2 delete", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
  auto txn4 = BeginTxn(*vdbms, "txn4");
  fmt::println(stderr, "C: check scan txn4");
  WithTxn(txn4, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}}));
  WithTxn(txn4, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (4)"));
  WithTxn(txn4, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (5)"));
  WithTxn(txn4, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (6)"));
  WithTxn(txn4, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 6"));
  TxnMgrDbg("after txn4 modification", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  fmt::println(stderr, "D: check scan txn4");
  WithTxn(txn4, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}, {4}, {5}}));
  fmt::println(stderr, "E: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
  WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 5"));
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
  WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
  WithTxn(txn4, CommitTxn(*vdbms, _var, _txn));
  TxnMgrDbg("after commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  auto txn5 = BeginTxn(*vdbms, "txn5");
  fmt::println(stderr, "F: check scan txn5");
  WithTxn(txn5, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {4}, {5}}));
  WithTxn(txn5, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable"));
  WithTxn(txn5, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn5, CommitTxn(*vdbms, _var, _txn));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}}));
}

TEST(TxnExecutorTest, DISABLED_InsertDeleteConflictTest) {  // NOLINT
  auto vdbms = std::make_unique<vdbmsInstance>();
  auto empty_table = IntResult{};
  Execute(*vdbms, "CREATE TABLE maintable(a int)");
  auto table_info = vdbms->catalog_->GetTable("maintable");
  auto txn1 = BeginTxn(*vdbms, "txn1");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (1)"));
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (2)"));
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (3)"));
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 3"));
  TxnMgrDbg("after 3 insert + 1 delete", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  fmt::println(stderr, "A: check scan txn1");
  const auto query = "SELECT a FROM maintable";
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}}));
  WithTxn(txn1, CommitTxn(*vdbms, _var, _txn));
  TxnMgrDbg("after commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  auto txn2 = BeginTxn(*vdbms, "txn2");
  fmt::println(stderr, "B: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}}));
  WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 2"));
  TxnMgrDbg("after txn2 delete", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
  auto txn3 = BeginTxn(*vdbms, "txn3");
  fmt::println(stderr, "C: check scan txn3");
  WithTxn(txn3, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}}));
  fmt::println(stderr, "D: taint txn3");
  WithTxn(txn3, ExecuteTxnTainted(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 2"));
  TxnMgrDbg("after txn3 tainted", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  auto txn4 = BeginTxn(*vdbms, "txn4");
  fmt::println(stderr, "E: check scan txn4");
  WithTxn(txn4, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}}));
  WithTxn(txn4, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (4)"));
  WithTxn(txn4, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (5)"));
  WithTxn(txn4, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (6)"));
  WithTxn(txn4, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 6"));
  TxnMgrDbg("after txn4 modification", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  fmt::println(stderr, "F: check scan txn4");
  WithTxn(txn4, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {2}, {4}, {5}}));
  fmt::println(stderr, "G: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
  WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 5"));
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}}));
  WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
  WithTxn(txn4, CommitTxn(*vdbms, _var, _txn));
  TxnMgrDbg("after commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  auto txn5 = BeginTxn(*vdbms, "txn5");
  fmt::println(stderr, "H: check scan txn5");
  WithTxn(txn5, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {4}, {5}}));
  fmt::println(stderr, "I: commit txn 6");
  auto txn6 = BeginTxn(*vdbms, "txn6");
  WithTxn(txn6, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 5"));
  TxnMgrDbg("after txn6 deletes", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn6, CommitTxn(*vdbms, _var, _txn));
  TxnMgrDbg("after txn6 commits", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  fmt::println(stderr, "J: taint txn5");
  WithTxn(txn5, ExecuteTxnTainted(*vdbms, _var, _txn, "DELETE FROM maintable WHERE a = 5"));
  auto txn7 = BeginTxn(*vdbms, "txn7");
  fmt::println(stderr, "I: check scan txn7");
  WithTxn(txn7, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1}, {4}}));
  WithTxn(txn7, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM maintable"));
  WithTxn(txn7, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn7, CommitTxn(*vdbms, _var, _txn));
}

TEST(TxnExecutorTest, DISABLED_UpdateTest1) {  // NOLINT
  fmt::println(stderr, "--- UpdateTest1: no undo log ---");
  auto vdbms = std::make_unique<vdbmsInstance>();
  auto empty_table = IntResult{};
  Execute(*vdbms, "CREATE TABLE table1(a int, b int, c int)");
  auto table_info = vdbms->catalog_->GetTable("table1");
  auto txn_ref = BeginTxn(*vdbms, "txn_ref");
  auto txn1 = BeginTxn(*vdbms, "txn1");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO table1 VALUES (1, 1, 1)"));
  TxnMgrDbg("after insert", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  const std::string query = "SELECT * FROM table1";
  fmt::println(stderr, "A: 1st update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET b = 2"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 2, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn1, CheckUndoLogNum(*vdbms, _var, _txn, 0));
  fmt::println(stderr, "B: 2nd update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET b = 3"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 3, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn1, CheckUndoLogNum(*vdbms, _var, _txn, 0));
  fmt::println(stderr, "C1: 3rd update, not real update...");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET a = 1"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 3, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn1, CheckUndoLogNum(*vdbms, _var, _txn, 0));
  fmt::println(stderr, "C2: the real 3rd update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET a = 2"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2, 3, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn1, CheckUndoLogNum(*vdbms, _var, _txn, 0));
  fmt::println(stderr, "D: 4th update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET b = 1"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2, 1, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn1, CheckUndoLogNum(*vdbms, _var, _txn, 0));
  fmt::println(stderr, "E: 5th update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET a = 3"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{3, 1, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn1, CheckUndoLogNum(*vdbms, _var, _txn, 0));
  fmt::println(stderr, "F: 6th update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET a = 4, b = 4, c = 4"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{4, 4, 4}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn1, CheckUndoLogNum(*vdbms, _var, _txn, 0));
  fmt::println(stderr, "G: delete");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "DELETE from table1"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn1, CheckUndoLogNum(*vdbms, _var, _txn, 0));
  WithTxn(txn1, CommitTxn(*vdbms, _var, _txn));
  auto txn2 = BeginTxn(*vdbms, "txn2");
  fmt::println(stderr, "H: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
  TableHeapEntryNoMoreThan(*vdbms, table_info, 1);
}

TEST(TxnExecutorTest, DISABLED_UpdateTest2) {  // NOLINT
  fmt::println(stderr, "--- UpdateTest2: update applied on insert ---");
  auto vdbms = std::make_unique<vdbmsInstance>();
  auto empty_table = IntResult{};
  Execute(*vdbms, "CREATE TABLE table2(a int, b int, c int)");
  auto table_info = vdbms->catalog_->GetTable("table2");
  auto txn0 = BeginTxn(*vdbms, "txn0");
  WithTxn(txn0, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO table2 VALUES (1, 1, 1)"));
  WithTxn(txn0, CommitTxn(*vdbms, _var, _txn));
  TxnMgrDbg("after insert and commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  auto txn1 = BeginTxn(*vdbms, "txn1");
  auto txn_ref = BeginTxn(*vdbms, "txn_ref");
  const std::string query = "SELECT * FROM table2";
  fmt::println(stderr, "A: 1st update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET b = 2"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 2, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 1));
  fmt::println(stderr, "B: 2nd update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET b = 3"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 3, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 1));
  fmt::println(stderr, "C1: 3rd update, not real update...");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET a = 1"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 3, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 1));
  fmt::println(stderr, "C2: the real 3rd update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET a = 2"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2, 3, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 2));
  fmt::println(stderr, "D: 4th update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET b = 1"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2, 1, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 2));
  fmt::println(stderr, "E: 5th update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET a = 3"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{3, 1, 1}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 2));
  fmt::println(stderr, "F: 6th update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET a = 4, b = 4, c = 4"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{4, 4, 4}}));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 3));
  fmt::println(stderr, "G: delete");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "DELETE from table2"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 3));
  WithTxn(txn1, CommitTxn(*vdbms, _var, _txn));
  auto txn2 = BeginTxn(*vdbms, "txn2");
  fmt::println(stderr, "H: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn_ref, CommitTxn(*vdbms, _var, _txn));
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
  TableHeapEntryNoMoreThan(*vdbms, table_info, 1);
}

TEST(TxnExecutorTest, DISABLED_UpdateTestWithUndoLog) {  // NOLINT
  fmt::println(stderr, "--- UpdateTestWithUndoLog: update applied on a version chain with undo log ---");
  auto vdbms = std::make_unique<vdbmsInstance>();
  auto empty_table = IntResult{};
  Execute(*vdbms, "CREATE TABLE table2(a int, b int, c int)");
  auto table_info = vdbms->catalog_->GetTable("table2");
  auto txn00 = BeginTxn(*vdbms, "txn00");
  WithTxn(txn00, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO table2 VALUES (0, 0, 0)"));
  WithTxn(txn00, CommitTxn(*vdbms, _var, _txn));
  auto txn_ref_0 = BeginTxn(*vdbms, "txn_ref_0");
  auto txn01 = BeginTxn(*vdbms, "txn01");
  WithTxn(txn01, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET a = 1, b = 1, c = 1"));
  WithTxn(txn01, CommitTxn(*vdbms, _var, _txn));
  TxnMgrDbg("after insert, update, and commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  auto txn1 = BeginTxn(*vdbms, "txn1");
  auto txn_ref_1 = BeginTxn(*vdbms, "txn_ref_1");
  const std::string query = "SELECT * FROM table2";
  fmt::println(stderr, "A: 1st update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET b = 2"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 2, 1}}));
  WithTxn(txn_ref_0, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}}));
  WithTxn(txn_ref_1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 1));
  fmt::println(stderr, "B: 2nd update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET b = 3"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 3, 1}}));
  WithTxn(txn_ref_0, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}}));
  WithTxn(txn_ref_1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 1));
  fmt::println(stderr, "C1: 3rd update, not real update...");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET a = 1"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 3, 1}}));
  WithTxn(txn_ref_0, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}}));
  WithTxn(txn_ref_1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 1));
  fmt::println(stderr, "C2: the real 3rd update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET a = 2"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2, 3, 1}}));
  WithTxn(txn_ref_0, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}}));
  WithTxn(txn_ref_1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 2));
  fmt::println(stderr, "D: 4th update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET b = 1"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{2, 1, 1}}));
  WithTxn(txn_ref_0, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}}));
  WithTxn(txn_ref_1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 2));
  fmt::println(stderr, "E: 5th update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET a = 3"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{3, 1, 1}}));
  WithTxn(txn_ref_0, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}}));
  WithTxn(txn_ref_1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 2));
  fmt::println(stderr, "F: 6th update");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table2 SET a = 4, b = 4, c = 4"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{4, 4, 4}}));
  WithTxn(txn_ref_0, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}}));
  WithTxn(txn_ref_1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 3));
  fmt::println(stderr, "G: delete");
  WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "DELETE from table2"));
  TxnMgrDbg("after update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn1, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_ref_0, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}}));
  WithTxn(txn_ref_1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn1, CheckUndoLogColumn(*vdbms, _var, _txn, 3));
  WithTxn(txn1, CommitTxn(*vdbms, _var, _txn));
  auto txn2 = BeginTxn(*vdbms, "txn2");
  fmt::println(stderr, "H: check scan txn2");
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_ref_0, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}}));
  WithTxn(txn_ref_1, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{1, 1, 1}}));
  WithTxn(txn_ref_0, CommitTxn(*vdbms, _var, _txn));
  WithTxn(txn_ref_1, CommitTxn(*vdbms, _var, _txn));
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
  TableHeapEntryNoMoreThan(*vdbms, table_info, 1);
}

TEST(TxnExecutorTest, DISABLED_UpdateConflict) {  // NOLINT
  {
    fmt::println(stderr, "--- UpdateConflict1: simple case, insert and two txn update it ---");
    auto vdbms = std::make_unique<vdbmsInstance>();
    Execute(*vdbms, "CREATE TABLE table1(a int, b int, c int)");
    auto table_info = vdbms->catalog_->GetTable("table1");
    auto txn0 = BeginTxn(*vdbms, "txn0");
    WithTxn(txn0, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO table1 VALUES (0, 0, 0)"));
    WithTxn(txn0, CommitTxn(*vdbms, _var, _txn));
    auto txn_ref = BeginTxn(*vdbms, "txn_ref");
    TxnMgrDbg("after initialize", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn1 = BeginTxn(*vdbms, "txn1");
    auto txn2 = BeginTxn(*vdbms, "txn2");
    WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET a = 1"));
    TxnMgrDbg("after 1st update", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn2, ExecuteTxnTainted(*vdbms, _var, _txn, "UPDATE table1 SET b = 2"));
    TxnMgrDbg("after txn tainted", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn1, CommitTxn(*vdbms, _var, _txn));
    TxnMgrDbg("after commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, "SELECT * FROM table1", IntResult{{0, 0, 0}}));
    TableHeapEntryNoMoreThan(*vdbms, table_info, 1);
  }
  {
    fmt::println(stderr, "--- UpdateConflict2: complex case with version chain ---");
    auto vdbms = std::make_unique<vdbmsInstance>();
    Execute(*vdbms, "CREATE TABLE table1(a int, b int, c int)");
    auto table_info = vdbms->catalog_->GetTable("table1");
    auto txn0 = BeginTxn(*vdbms, "txn0");
    WithTxn(txn0, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO table1 VALUES (0, 0, 0), (1, 1, 1)"));
    WithTxn(txn0, CommitTxn(*vdbms, _var, _txn));
    TxnMgrDbg("after initialize", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn1 = BeginTxn(*vdbms, "txn1");
    auto txn2 = BeginTxn(*vdbms, "txn2");
    auto txn3 = BeginTxn(*vdbms, "txn3");
    auto txn4 = BeginTxn(*vdbms, "txn4");
    auto txn_ref = BeginTxn(*vdbms, "txn_ref");
    WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET b = 233 WHERE a = 0"));
    WithTxn(txn1, CommitTxn(*vdbms, _var, _txn));
    WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET b = 2333 WHERE a = 1"));
    TxnMgrDbg("after updates", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn3, ExecuteTxnTainted(*vdbms, _var, _txn, "UPDATE table1 SET b = 2 WHERE a = 0"));
    TxnMgrDbg("after txn3 tainted", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn4, ExecuteTxnTainted(*vdbms, _var, _txn, "UPDATE table1 SET b = 2 WHERE a = 1"));
    TxnMgrDbg("after txn4 tainted", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
    TxnMgrDbg("after commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn_ref, QueryShowResult(*vdbms, _var, _txn, "SELECT * FROM table1", IntResult{{0, 0, 0}, {1, 1, 1}}));
    auto txn5 = BeginTxn(*vdbms, "txn5");
    WithTxn(txn5, QueryShowResult(*vdbms, _var, _txn, "SELECT * FROM table1", IntResult{{0, 233, 0}, {1, 2333, 1}}));
    TableHeapEntryNoMoreThan(*vdbms, table_info, 2);
  }
}

TEST(TxnExecutorTest, DISABLED_GarbageCollection) {  // NOLINT
  auto vdbms = std::make_unique<vdbmsInstance>();
  auto empty_table = IntResult{};
  Execute(*vdbms, "CREATE TABLE table1(a int, b int, c int)");
  auto table_info = vdbms->catalog_->GetTable("table1");
  const std::string query = "SELECT * FROM table1";
  auto txn_watermark_at_0 = BeginTxn(*vdbms, "txn_watermark_at_0");
  auto txn_watermark_at_0_id = txn_watermark_at_0->GetTransactionId();
  BumpCommitTs(*vdbms, 2);
  auto txn_a = BeginTxn(*vdbms, "txn_a");
  auto txn_a_id = txn_a->GetTransactionId();
  WithTxn(txn_a, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO table1 VALUES (0, 0, 0), (1, 1, 1)"));
  WithTxn(txn_a, CommitTxn(*vdbms, _var, _txn));
  auto txn_b = BeginTxn(*vdbms, "txn_b");
  auto txn_b_id = txn_b->GetTransactionId();
  WithTxn(txn_b, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO table1 VALUES (2, 2, 2), (3, 3, 3)"));
  WithTxn(txn_b, CommitTxn(*vdbms, _var, _txn));
  BumpCommitTs(*vdbms, 2);
  auto txn_watermark_at_1 = BeginTxn(*vdbms, "txn_watermark_at_1");
  auto txn_watermark_at_1_id = txn_watermark_at_1->GetTransactionId();
  BumpCommitTs(*vdbms, 2);
  auto txn2 = BeginTxn(*vdbms, "txn2");
  auto txn2_id = txn2->GetTransactionId();
  WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET a = a + 10"));
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
  BumpCommitTs(*vdbms, 2);
  auto txn_watermark_at_2 = BeginTxn(*vdbms, "txn_watermark_at_2");
  auto txn_watermark_at_2_id = txn_watermark_at_2->GetTransactionId();
  BumpCommitTs(*vdbms, 2);
  auto txn3 = BeginTxn(*vdbms, "txn3");
  auto txn3_id = txn3->GetTransactionId();
  WithTxn(txn3, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET a = a + 10 WHERE a < 12"));
  WithTxn(txn3, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM table1 WHERE a = 21"));
  WithTxn(txn3, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn3, CommitTxn(*vdbms, _var, _txn));
  BumpCommitTs(*vdbms, 2);
  auto txn_watermark_at_3 = BeginTxn(*vdbms, "txn_watermark_at_3");
  auto txn_watermark_at_3_id = txn_watermark_at_3->GetTransactionId();
  BumpCommitTs(*vdbms, 2);
  TxnMgrDbg("after commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());

  WithTxn(txn_watermark_at_0, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_watermark_at_1,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}));
  WithTxn(txn_watermark_at_2,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "A: first GC");
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  fmt::println(stderr, "B: second GC");
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection (yes, we call it twice without doing anything...)", vdbms->txn_manager_.get(),
            table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnExists(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnExists(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnExists(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnExists(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnExists(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnExists(*vdbms, _var, txn3_id));
  WithTxn(txn_watermark_at_0, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_watermark_at_1,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}));
  WithTxn(txn_watermark_at_2,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "C: 3rd GC");
  WithTxn(txn_watermark_at_0, CommitTxn(*vdbms, _var, _txn));
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnExists(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnExists(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnExists(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnExists(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnExists(*vdbms, _var, txn3_id));
  WithTxn(txn_watermark_at_1,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}));
  WithTxn(txn_watermark_at_2,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "D: 4th GC");
  WithTxn(txn_watermark_at_1, CommitTxn(*vdbms, _var, _txn));
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnExists(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnExists(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnGCed(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnExists(*vdbms, _var, txn3_id));
  WithTxn(txn_watermark_at_2,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "E: 5th GC");
  WithTxn(txn_watermark_at_2, CommitTxn(*vdbms, _var, _txn));
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnExists(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnGCed(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnGCed(*vdbms, _var, txn3_id));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "F: 6th GC");
  WithTxn(txn_watermark_at_3, CommitTxn(*vdbms, _var, _txn));
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnGCed(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnGCed(*vdbms, _var, txn3_id));
}

TEST(TxnExecutorTest, DISABLED_GarbageCollectionWithTainted) {  // NOLINT
  auto empty_table = IntResult{};
  auto vdbms = std::make_unique<vdbmsInstance>();
  Execute(*vdbms, "CREATE TABLE table1(a int, b int, c int)");
  auto table_info = vdbms->catalog_->GetTable("table1");
  const std::string query = "SELECT * FROM table1";
  auto txn_watermark_at_0 = BeginTxn(*vdbms, "txn_watermark_at_0");
  auto txn_watermark_at_0_id = txn_watermark_at_0->GetTransactionId();
  BumpCommitTs(*vdbms, 2);
  auto txn_a = BeginTxn(*vdbms, "txn_a");
  auto txn_a_id = txn_a->GetTransactionId();
  WithTxn(txn_a, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO table1 VALUES (0, 0, 0), (1, 1, 1)"));
  WithTxn(txn_a, CommitTxn(*vdbms, _var, _txn));
  auto txn_b = BeginTxn(*vdbms, "txn_b");
  auto txn_b_id = txn_b->GetTransactionId();
  WithTxn(txn_b, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO table1 VALUES (2, 2, 2), (3, 3, 3)"));
  WithTxn(txn_b, CommitTxn(*vdbms, _var, _txn));
  BumpCommitTs(*vdbms, 2);
  auto txn_watermark_at_1 = BeginTxn(*vdbms, "txn_watermark_at_1");
  auto txn_watermark_at_1_id = txn_watermark_at_1->GetTransactionId();
  BumpCommitTs(*vdbms, 2);
  auto txn2 = BeginTxn(*vdbms, "txn2");
  auto txn2_id = txn2->GetTransactionId();
  WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET a = a + 10"));
  WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
  BumpCommitTs(*vdbms, 2);
  auto txn_watermark_at_2 = BeginTxn(*vdbms, "txn_watermark_at_2");
  auto txn_watermark_at_2_id = txn_watermark_at_2->GetTransactionId();
  BumpCommitTs(*vdbms, 2);
  auto txn3 = BeginTxn(*vdbms, "txn3");
  auto txn3_id = txn3->GetTransactionId();
  WithTxn(txn3, ExecuteTxn(*vdbms, _var, _txn, "UPDATE table1 SET a = a + 10 WHERE a < 12"));
  WithTxn(txn3, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM table1 WHERE a = 21"));
  WithTxn(txn3, QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));
  auto txn5 = BeginTxn(*vdbms, "txn5");
  auto txn5_id = txn5->GetTransactionId();
  auto txn6 = BeginTxn(*vdbms, "txn6");
  auto txn6_id = txn6->GetTransactionId();
  WithTxn(txn3, CommitTxn(*vdbms, _var, _txn));
  BumpCommitTs(*vdbms, 2);
  auto txn_watermark_at_3 = BeginTxn(*vdbms, "txn_watermark_at_3");
  auto txn_watermark_at_3_id = txn_watermark_at_3->GetTransactionId();
  BumpCommitTs(*vdbms, 2);
  TxnMgrDbg("after commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());

  WithTxn(txn_watermark_at_0, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_watermark_at_1,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}));
  WithTxn(txn_watermark_at_2,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "A: first GC");
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  fmt::println(stderr, "B: second GC");
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection (yes, we call it twice without doing anything...)", vdbms->txn_manager_.get(),
            table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnExists(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnExists(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnExists(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnExists(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnExists(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnExists(*vdbms, _var, txn3_id));
  WithTxn(txn5, EnsureTxnExists(*vdbms, _var, txn5_id));
  WithTxn(txn_watermark_at_0, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_watermark_at_1,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}));
  WithTxn(txn_watermark_at_2,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "C: taint txn5 + txn6 + third GC");
  WithTxn(txn5, ExecuteTxn(*vdbms, _var, _txn, "DELETE FROM table1 WHERE a = 12"));
  WithTxn(txn5, ExecuteTxnTainted(*vdbms, _var, _txn, "DELETE FROM table1 WHERE a = 11"));
  WithTxn(txn6, ExecuteTxnTainted(*vdbms, _var, _txn, "DELETE FROM table1 WHERE a = 11"));
  TxnMgrDbg("after txn5 + txn6 tainted", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnExists(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnExists(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnExists(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnExists(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnExists(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnExists(*vdbms, _var, txn3_id));
  WithTxn(txn5, EnsureTxnExists(*vdbms, _var, txn5_id));
  WithTxn(txn6, EnsureTxnExists(*vdbms, _var, txn6_id));
  WithTxn(txn_watermark_at_0, QueryShowResult(*vdbms, _var, _txn, query, empty_table));
  WithTxn(txn_watermark_at_1,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}));
  WithTxn(txn_watermark_at_2,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "D: 4th GC");
  WithTxn(txn_watermark_at_0, CommitTxn(*vdbms, _var, _txn));
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnExists(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnExists(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnExists(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnExists(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnExists(*vdbms, _var, txn3_id));
  WithTxn(txn5, EnsureTxnExists(*vdbms, _var, txn5_id));
  WithTxn(txn6, EnsureTxnExists(*vdbms, _var, txn6_id));
  WithTxn(txn_watermark_at_1,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}));
  WithTxn(txn_watermark_at_2,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "E: 5th GC");
  WithTxn(txn_watermark_at_1, CommitTxn(*vdbms, _var, _txn));
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnExists(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnExists(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnGCed(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnExists(*vdbms, _var, txn3_id));
  WithTxn(txn5, EnsureTxnExists(*vdbms, _var, txn5_id));
  WithTxn(txn6, EnsureTxnExists(*vdbms, _var, txn6_id));
  WithTxn(txn_watermark_at_2,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{10, 0, 0}, {11, 1, 1}, {12, 2, 2}, {13, 3, 3}}));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "F: 6th GC");
  WithTxn(txn_watermark_at_2, CommitTxn(*vdbms, _var, _txn));
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnExists(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnGCed(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnExists(*vdbms, _var, txn3_id));
  WithTxn(txn5, EnsureTxnExists(*vdbms, _var, txn5_id));
  WithTxn(txn6, EnsureTxnExists(*vdbms, _var, txn6_id));
  WithTxn(txn_watermark_at_3,
          QueryShowResult(*vdbms, _var, _txn, query, IntResult{{20, 0, 0}, {12, 2, 2}, {13, 3, 3}}));

  fmt::println(stderr, "G: 7th GC");
  WithTxn(txn_watermark_at_3, CommitTxn(*vdbms, _var, _txn));
  GarbageCollection(*vdbms);
  TxnMgrDbg("after garbage collection", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
  WithTxn(txn_watermark_at_0, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_0_id));
  WithTxn(txn_watermark_at_1, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_1_id));
  WithTxn(txn_watermark_at_2, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_2_id));
  WithTxn(txn_watermark_at_3, EnsureTxnGCed(*vdbms, _var, txn_watermark_at_3_id));
  WithTxn(txn_a, EnsureTxnGCed(*vdbms, _var, txn_a_id));
  WithTxn(txn_b, EnsureTxnGCed(*vdbms, _var, txn_b_id));
  WithTxn(txn2, EnsureTxnGCed(*vdbms, _var, txn2_id));
  WithTxn(txn3, EnsureTxnExists(*vdbms, _var, txn3_id));
  WithTxn(txn5, EnsureTxnExists(*vdbms, _var, txn5_id));
  WithTxn(txn6, EnsureTxnExists(*vdbms, _var, txn6_id));
}

// NOLINTEND(bugprone-unchecked-optional-access))

}  // namespace vdbms
