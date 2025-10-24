#include <common/bustub_instance.h>
#include <gtest/gtest.h>

namespace bustub {

class JoinTest : public ::testing::Test {
 protected:
  void SetUp() override {
    bustub_instance_ = std::make_unique<BustubInstance>("test.db");
    bustub_instance_->GenerateMockTable();
    if (bustub_instance_->buffer_pool_manager_ != nullptr) {
      bustub_instance_->GenerateTestTable();
    }
  }

  void TearDown() override {}

  // If we use BustubInstance *bustub_instance_, we need to free the memory manually.
  std::unique_ptr<BustubInstance> bustub_instance_;
};

TEST_F(JoinTest, TestNestedLoopJoin) {
  std::string sql = "select * from test_2, test_3 where test_2.colA = test_3.colA";
  NoopWriter writer;
  bustub_instance_->ExecuteSql(sql, writer);
}

TEST_F(JoinTest, TestNesteIndexJoin) {
  NoopWriter writer;
  std::string create_index_sql = "create index t3colA on test_3(colA)";
  std::string sql = "select * from test_2, test_3 where test_2.colA = test_3.colA";
  bustub_instance_->ExecuteSql(create_index_sql, writer);
  bustub_instance_->ExecuteSql(sql, writer);
}

}  // namespace bustub