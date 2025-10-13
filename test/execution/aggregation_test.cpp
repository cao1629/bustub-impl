#include <gtest/gtest.h>
#include <common/bustub_instance.h>


namespace bustub {

class AggregationTest : public ::testing::Test {
protected:
  void SetUp() override {
    bustub_instance_ = std::make_unique<BustubInstance>("test.db");
    bustub_instance_->GenerateMockTable();
    if (bustub_instance_->buffer_pool_manager_ != nullptr) {
      bustub_instance_->GenerateMockTable();
    }
  }

  void TearDown() override {

  }

private:
  // If we use BustubInstance *bustub_instance_, we need to free the memory manually.
  std::unique_ptr<BustubInstance> bustub_instance_;
};


TEST_F(AggregationTest, TestCountStar) {

}

TEST_F(AggregationTest, TestCount) {

}


TEST_F(AggregationTest, TestSum) {

}

TEST_F(AggregationTest, TestMax) {

}

TEST_F(AggregationTest, TestMin) {

}

}