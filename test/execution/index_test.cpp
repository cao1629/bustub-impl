#include <gtest/gtest.h>
#include <common/bustub_instance.h>

namespace bustub {

class IndexTest : public ::testing::Test {
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

TEST_F(IndexTest, SimpleIndexTest) {
  // Create an index
  std::string create_index_sql = "create index test3_colA on test_3(colA)";
  NoopWriter writer;
  bustub_instance_->ExecuteSql(create_index_sql, writer);

  // IndexScanExecutor
  std::string sql = "select * from test_3 where colA = 10";
  bustub_instance_->ExecuteSql(sql, writer);
}

}