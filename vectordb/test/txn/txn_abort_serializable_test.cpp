#include "common/vdbms_instance.h"
#include "concurrency/transaction.h"
#include "fmt/core.h"
#include "txn_common.h"  // NOLINT

namespace vdbms {

// NOLINTBEGIN(bugprone-unchecked-optional-access)

TEST(TxnBonusTest, DISABLED_SerializableTest) {  // NOLINT
  fmt::println(stderr, "--- SerializableTest2: Serializable ---");
  {
    auto vdbms = std::make_unique<vdbmsInstance>();
    EnsureIndexScan(*vdbms);
    Execute(*vdbms, "CREATE TABLE maintable(a int, b int primary key)");
    auto table_info = vdbms->catalog_->GetTable("maintable");
    auto txn1 = BeginTxnSerializable(*vdbms, "txn1");
    WithTxn(txn1,
            ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (1, 100), (1, 101), (0, 102), (0, 103)"));
    WithTxn(txn1, CommitTxn(*vdbms, _var, _txn));

    auto txn2 = BeginTxnSerializable(*vdbms, "txn2");
    auto txn3 = BeginTxnSerializable(*vdbms, "txn3");
    auto txn_read = BeginTxnSerializable(*vdbms, "txn_read");
    WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "UPDATE maintable SET a = 0 WHERE a = 1"));
    WithTxn(txn3, ExecuteTxn(*vdbms, _var, _txn, "UPDATE maintable SET a = 1 WHERE a = 0"));
    TxnMgrDbg("after two updates", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn_read, ExecuteTxn(*vdbms, _var, _txn, "SELECT * FROM maintable WHERE a = 0"));
    WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
    WithTxn(txn3, CommitTxn(*vdbms, _var, _txn, EXPECT_FAIL));
    WithTxn(txn_read, CommitTxn(*vdbms, _var, _txn));
    // test continues on Gradescope...
  }
}

TEST(TxnBonusTest, DISABLED_AbortTest) {  // NOLINT
  fmt::println(stderr, "--- AbortTest1: Simple Abort ---");
  {
    auto vdbms = std::make_unique<vdbmsInstance>();
    EnsureIndexScan(*vdbms);
    Execute(*vdbms, "CREATE TABLE maintable(a int primary key, b int)");
    auto table_info = vdbms->catalog_->GetTable("maintable");
    auto txn1 = BeginTxn(*vdbms, "txn1");
    WithTxn(txn1, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (1, 233), (2, 2333)"));
    WithTxn(txn1, AbortTxn(*vdbms, _var, _txn));
    TxnMgrDbg("after abort", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn2 = BeginTxn(*vdbms, "txn2");
    WithTxn(txn2, ExecuteTxn(*vdbms, _var, _txn, "INSERT INTO maintable VALUES (1, 2333), (2, 23333), (3, 233)"));
    WithTxn(txn2, QueryShowResult(*vdbms, _var, _txn, "SELECT * FROM maintable",
                                  IntResult{
                                      {1, 2333},
                                      {2, 23333},
                                      {3, 233},
                                  }));
    TxnMgrDbg("after insert", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn2, CommitTxn(*vdbms, _var, _txn));
    TxnMgrDbg("after commit", vdbms->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn3 = BeginTxn(*vdbms, "txn3");
    WithTxn(txn3, QueryShowResult(*vdbms, _var, _txn, "SELECT * FROM maintable",
                                  IntResult{
                                      {1, 2333},
                                      {2, 23333},
                                      {3, 233},
                                  }));
    TableHeapEntryNoMoreThan(*vdbms, table_info, 3);
    // test continues on Gradescope...
  }
}
// NOLINTEND(bugprone-unchecked-optional-access))

}  // namespace vdbms
