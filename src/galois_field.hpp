/*!
 * \file galois_field.hpp
 *
 * \brief Implementation of Galois field for RAID 6
 */

#include "block.hpp"
namespace raid_fs::galois_field {
  class Element {
  public:
    Element(byte_stream_view_type byte_vector_) : byte_vector{byte_vector_} {}
    // Obtain additive inverse
    Element operator-() const { return *this; }
    Element &operator+=(const Element &rhs) {
      assert(byte_vector.size() == rhs.byte_vector.size());
      auto data_ptr = reinterpret_cast<std::byte *>(byte_vector.data());
      auto rhs_data_ptr =
          reinterpret_cast<const std::byte *>(rhs.byte_vector.data());
      for (size_t i = 0; i < byte_vector.size(); i++) {
        data_ptr[i] ^= rhs_data_ptr[i];
      }
    }

    static uint8_t byte_multiply_by_2(uint8_t b) {
      return (b << 1) ^ ((b & 0x80) ? 0x1d : 0);
    }
    static uint8_t byte_multiply(uint8_t a, uint8_t b) {
      uint8_t res = 0;
      for (size_t i = 0; i < 7; i++) {
        if (b & 0x80) {
          res += a;
        }
        res = byte_multiply_by_2(res);
        b <<= 1;
      }
      if (b & 0x80) {
        res += a;
      }
      return res;
    }

    /* static void multiply_table() { */

    /* } */

  private:
    byte_stream_view_type byte_vector;
  };
} // namespace raid_fs::galois_field
