#include <chrono>  // NOLINT
#include <cstdio>
#include <exception>
#include <functional>
#include <future>  // NOLINT
#include <memory>
#include <stdexcept>
#include <thread>  // NOLINT

#include "common/vdbms_instance.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/watermark.h"
#include "fmt/core.h"
#include "gtest/gtest.h"

namespace vdbms {

TEST(TxnTsTest, DISABLED_WatermarkPerformance) {  // NOLINT
  const int txn_n = 1000000;
  {
    auto watermark = Watermark(0);
    for (int i = 0; i < txn_n; i++) {
      watermark.AddTxn(i);
      ASSERT_EQ(watermark.GetWatermark(), 0);
    }
    for (int i = 0; i < txn_n; i++) {
      watermark.UpdateCommitTs(i + 1);
      watermark.RemoveTxn(i);
      ASSERT_EQ(watermark.GetWatermark(), i + 1);
    }
  }
  {
    auto watermark = Watermark(0);
    for (int i = 0; i < txn_n; i++) {
      watermark.AddTxn(i);
      ASSERT_EQ(watermark.GetWatermark(), 0);
    }
    for (int i = 0; i < txn_n; i++) {
      watermark.UpdateCommitTs(i + 1);
      watermark.RemoveTxn(txn_n - i - 1);
      if (i == txn_n - 1) {
        ASSERT_EQ(watermark.GetWatermark(), txn_n);
      } else {
        ASSERT_EQ(watermark.GetWatermark(), 0);
      }
    }
  }
}

TEST(TxnTsTest, DISABLED_TimestampTracking) {  // NOLINT
  auto vdbms = std::make_unique<vdbmsInstance>();

  auto txn0 = vdbms->txn_manager_->Begin();
  ASSERT_EQ(txn0->GetReadTs(), 0);
  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 0);

  {
    auto txn_store_1 = vdbms->txn_manager_->Begin();
    ASSERT_EQ(txn_store_1->GetReadTs(), 0);
    vdbms->txn_manager_->Commit(txn_store_1);
    ASSERT_EQ(txn_store_1->GetCommitTs(), 1);
  }

  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 0);

  auto txn1 = vdbms->txn_manager_->Begin();
  ASSERT_EQ(txn1->GetReadTs(), 1);

  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 0);

  {
    auto txn_store_2 = vdbms->txn_manager_->Begin();
    ASSERT_EQ(txn_store_2->GetReadTs(), 1);
    vdbms->txn_manager_->Commit(txn_store_2);
    ASSERT_EQ(txn_store_2->GetCommitTs(), 2);
  }

  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 0);

  auto txn2 = vdbms->txn_manager_->Begin();
  ASSERT_EQ(txn2->GetReadTs(), 2);

  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 0);

  vdbms->txn_manager_->Abort(txn0);
  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 1);

  {
    auto txn_store_3 = vdbms->txn_manager_->Begin();
    ASSERT_EQ(txn_store_3->GetReadTs(), 2);
    vdbms->txn_manager_->Commit(txn_store_3);
    ASSERT_EQ(txn_store_3->GetCommitTs(), 3);
  }

  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 1);

  auto txn3 = vdbms->txn_manager_->Begin();
  ASSERT_EQ(txn3->GetReadTs(), 3);

  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 1);

  vdbms->txn_manager_->Abort(txn1);
  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 2);
  vdbms->txn_manager_->Abort(txn2);
  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 3);

  {
    auto txn_store_4 = vdbms->txn_manager_->Begin();
    ASSERT_EQ(txn_store_4->GetReadTs(), 3);
    vdbms->txn_manager_->Commit(txn_store_4);
    ASSERT_EQ(txn_store_4->GetCommitTs(), 4);
  }

  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 3);

  auto txn4 = vdbms->txn_manager_->Begin();
  ASSERT_EQ(txn4->GetReadTs(), 4);

  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 3);
  vdbms->txn_manager_->Abort(txn3);
  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 4);
  vdbms->txn_manager_->Abort(txn4);
  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 4);

  {
    auto txn_store_5 = vdbms->txn_manager_->Begin();
    ASSERT_EQ(txn_store_5->GetTransactionState(), TransactionState::RUNNING);
    ASSERT_EQ(txn_store_5->GetReadTs(), 4);
    vdbms->txn_manager_->Commit(txn_store_5);
    ASSERT_EQ(txn_store_5->GetTransactionState(), TransactionState::COMMITTED);
  }

  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 5);

  auto txn5 = vdbms->txn_manager_->Begin();
  ASSERT_EQ(txn5->GetTransactionState(), TransactionState::RUNNING);
  ASSERT_EQ(txn5->GetReadTs(), 5);
  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 5);
  vdbms->txn_manager_->Abort(txn5);
  ASSERT_EQ(txn5->GetTransactionState(), TransactionState::ABORTED);
  ASSERT_EQ(vdbms->txn_manager_->GetWatermark(), 5);
}

// NOLINTEND(bugprone-unchecked-optional-access))

}  // namespace vdbms
