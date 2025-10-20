#include <gtest/gtest.h>
#include "common/bustub_instance.h"

namespace bustub {

class SeqScanTest : public ::testing::Test {
public:
  void SetUp() override {
    bustub_instance_ = new BustubInstance("test.db");
    if (bustub_instance_->buffer_pool_manager_ != nullptr) {
      bustub_instance_->GenerateTestTable();
    }
  }

  void TearDown() override {
    free(bustub_instance_);
  }

  BustubInstance *bustub_instance_;
};


TEST_F(SeqScanTest, TestBasic) {
  std::string sql = "select * from test_2";
  NoopWriter writer;
  bustub_instance_->ExecuteSql(sql, writer);
}

}