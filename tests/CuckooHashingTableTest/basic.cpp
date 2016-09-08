//
// Created by rui_wang on 9/8/16.
//

#include <iostream>

#include "gtest/gtest.h"
#include "CuckoohashingTable.h"

class CuckooHasingTableBasicTest : public ::testing::Test { };

TEST_F(CuckooHasingTableBasicTest, BasicTest) {
  concurrent_lib::CuckoohashingTable<int, int> table;
  std::cout << table.Size() << std::endl;
}