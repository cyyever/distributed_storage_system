/*!
 * \file galois_field_test.cpp
 *
 */
#include <doctest/doctest.h>

#include "galois_field.hpp"

TEST_CASE("Galois field") {
  SUBCASE("multiply by 2") {
    REQUIRE_EQ(raid_fs::galois_field::Element::byte_multiply(2, 2), 4);
    REQUIRE_EQ(raid_fs::galois_field::Element::byte_multiply(4, 2), 8);
    REQUIRE_EQ(raid_fs::galois_field::Element::byte_multiply(8, 2), 16);
    REQUIRE_EQ(raid_fs::galois_field::Element::byte_multiply(0x80, 2), 0x1d);
    REQUIRE_EQ(raid_fs::galois_field::Element::byte_multiply_by_2(0x80), 0x1d);
  }
}
