#include <common/bustub_instance.h>
#include <gtest/gtest.h>

namespace bustub {

class SortTest : public ::testing::Test {
 protected:
  void SetUp() override {
    bustub_instance_ = new BustubInstance("test.db");
    bustub_instance_->GenerateMockTable();
    if (bustub_instance_->buffer_pool_manager_ != nullptr) {
      bustub_instance_->GenerateTestTable();
    }
  }

  void TearDown() override { free(bustub_instance_); }

  BustubInstance *bustub_instance_;
};

TEST_F(SortTest, Test1) {
  std::string sql = "select * from test_2 order by colA ASC, colB DESC";
  NoopWriter writer;
  bustub_instance_->ExecuteSql(sql, writer);
}

}  // namespace bustub