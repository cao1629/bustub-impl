#include <gtest/gtest.h>
#include <iostream>
#include <common/bustub_instance.h>


namespace bustub {

class InsertTest : public ::testing::Test {
 protected:
  void SetUp() override {
    bustub_instance_ = std::make_unique<BustubInstance>("test.db");
    bustub_instance_->GenerateMockTable();
    if (bustub_instance_->buffer_pool_manager_ != nullptr) {
      bustub_instance_->GenerateTestTable();
    }
  }

  void TearDown() override {
    // Code here will be called immediately after each test (right before the destructor).
  }

  std::unique_ptr<BustubInstance> bustub_instance_;
};

TEST_F(InsertTest, SimpleInsertTest) {
  std::string sql = "insert into test_3 values (10, 100)";
  NoopWriter writer;
  bustub_instance_->ExecuteSql(sql, writer);
}


}