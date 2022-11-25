/*!
 * \file galois_field_test.cpp
 *
 */
#include <set>

#include <doctest/doctest.h>

#include "galois_field.hpp"

TEST_CASE("Galois field") {
  SUBCASE("multiply by 2") {
    REQUIRE_EQ(raid_fs::galois_field::Vector::byte_multiply_by_2(2), 4);
    REQUIRE_EQ(raid_fs::galois_field::Vector::byte_multiply_by_2(4), 8);
    REQUIRE_EQ(raid_fs::galois_field::Vector::byte_multiply_by_2(8), 16);
    REQUIRE_EQ(raid_fs::galois_field::Vector::byte_multiply_by_2(0x80), 0x1d);
    REQUIRE_EQ(raid_fs::galois_field::Vector::byte_multiply_by_2(0x80), 0x1d);
  }
  SUBCASE("generator power table") {
    std::set<uint8_t> power_table;
    uint8_t power = 1;
    power_table.insert(power);
    for (size_t i = 1; i < 255; i++) {
      power = raid_fs::galois_field::Vector::byte_multiply_by_2(power);
      REQUIRE(!power_table.contains(power));
      power_table.insert(power);
    }
    REQUIRE_EQ(power_table.size(), 255);
    power = raid_fs::galois_field::Vector::byte_multiply_by_2(power);
    REQUIRE_EQ(power, 1);
  }
}
