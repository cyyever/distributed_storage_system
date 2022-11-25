/*!
 * \file galois_field.hpp
 *
 * \brief Implementation of Galois field GF(2^8) for RAID 6
 */

#include <cyy/naive_lib/log/log.hpp>
#include <spdlog/fmt/fmt.h>

#include "block.hpp"
namespace raid_fs::galois_field {
  class Vector {

  public:
    explicit Vector(size_t byte_count) {
      if (byte_count == 0) {
        LOG_ERROR("can't support empty byte vector");
        throw std::runtime_error("can't support empty byte vector");
      }
      byte_vector.resize(byte_count, 0);
    }
    explicit Vector(byte_stream_view_type byte_vector_)
        : byte_vector(byte_vector_.data(), byte_vector_.size()) {
      if (byte_vector.empty()) {
        LOG_ERROR("can't support empty byte vector");
        throw std::runtime_error("can't support empty byte vector");
      }
    }
    byte_stream_type &get_byte_vector() { return byte_vector; }
    const byte_stream_type &get_byte_vector() const { return byte_vector; }

    // Obtain additive inverse
    Vector operator-() const { return *this; }
    Vector &operator+=(const const_byte_stream_view_type &rhs) {
      assert(rhs.size() > 0);
      if (byte_vector.size() != rhs.size()) {
        LOG_ERROR("can't add byte vectors with different sizes: {} and {}",
                  byte_vector.size(), rhs.size());

        throw std::runtime_error(fmt ::format(
            "can't add byte vectors with different sizes: {} and {}",
            byte_vector.size(), rhs.size()));
      }
      assert(byte_vector.size() == rhs.size());
      auto *data_ptr = reinterpret_cast<std::byte *>(byte_vector.data());
      const auto *rhs_data_ptr =
          reinterpret_cast<const std::byte *>(rhs.data());
      for (size_t i = 0; i < byte_vector.size(); i++) {
        data_ptr[i] ^= rhs_data_ptr[i];
      }
      return *this;
    }

    Vector operator*(uint8_t scalar) const {
      auto res = *this;
      auto *data_ptr = reinterpret_cast<uint8_t *>(res.byte_vector.data());
      for (size_t i = 0; i < res.byte_vector.size(); i++) {
        data_ptr[i] = byte_multiply(data_ptr[i], scalar);
      }
      return res;
    }

    // +=(rhs*scalar)
    Vector &multiply_add(const const_byte_stream_view_type &rhs,
                          uint8_t scalar) {
      assert(byte_vector.size() == rhs.size());
      if (byte_vector.size() != rhs.size()) {
        LOG_ERROR("can't add byte vectors with different sizes: {} and {}",
                  byte_vector.size(), rhs.size());

        throw std::runtime_error(fmt ::format(
            "can't add byte vectors with different sizes: {} and {}",
            byte_vector.size(), rhs.size()));
      }
      assert(byte_vector.size() == rhs.size());
      auto *data_ptr = reinterpret_cast<uint8_t *>(byte_vector.data());
      const auto *rhs_data_ptr = reinterpret_cast<const uint8_t *>(rhs.data());
      for (size_t i = 0; i < byte_vector.size(); i++) {
        data_ptr[i] ^= byte_multiply(rhs_data_ptr[i], scalar);
      }
      return *this;
    }

    // -=(rhs*scalar)
    Vector &multiply_subtract(const const_byte_stream_view_type &rhs,
                               uint8_t scalar) {
      return multiply_add(rhs, scalar);
    }

    // Since additive inverse is itself under XOR, subtraction is the same as
    // addition
    Vector &operator-=(const const_byte_stream_view_type &rhs) {
      return operator+=(rhs);
    }
    void multiply_by_2() {
      auto *data_ptr = reinterpret_cast<uint8_t *>(byte_vector.data());
      for (size_t i = 0; i < byte_vector.size(); i++) {
        data_ptr[i] = byte_multiply_by_2(data_ptr[i]);
      }
    }
    static inline uint8_t byte_addition(uint8_t a, uint8_t b) { return a ^ b; }

    static inline uint8_t byte_multiply_by_2(uint8_t b) {
      return (b << 1) ^ ((b & 0x80) ? 0x1d : 0);
    }
    static uint8_t byte_multiply(uint8_t a, uint8_t b) {
      uint8_t res = 0;
      for (size_t i = 0; i < 7; i++) {
        if (b & 0x80) {
          res ^= a;
        }
        res = byte_multiply_by_2(res);
        b <<= 1;
      }
      if (b & 0x80) {
        res ^= a;
      }
      return res;
    }

    class MultiplyInverseTable {
    public:
      MultiplyInverseTable() : table{} {
        for (size_t i = 1; i < 256; i++) {
          if (table[i] != 0) {
            continue;
          }
          for (size_t j = 1; j < 256; j++) {
            auto product = byte_multiply(i, j);
            if (product == 1) {
              assert(byte_multiply(j, i) == 1);
              table[i] = j;
              table[j] = i;
              break;
            }
          }
        }
      }
      uint8_t get_inverse(uint8_t a) const {
        assert(a != 0);
        return table[a];
      }

    private:
      std::array<uint8_t, 256> table{};
    };

    class GeneratorPowerTable {
    public:
      GeneratorPowerTable() {
        uint8_t power = 1;
        table[0] = power;
        for (size_t i = 1; i < 255; i++) {
          power = Vector::byte_multiply_by_2(power);
          table[i] = power;
        }
        power = Vector::byte_multiply_by_2(power);
        assert(power == 1);
      }
      uint8_t get_negative_power(int negative_exponent) const {
        assert(negative_exponent <= 0);
        assert(negative_exponent > -256);
        auto exponent = (negative_exponent + 2 * 255) % 255;
        return table[exponent];
      }

      uint8_t get_power(size_t exponent) const {
        assert(exponent < 256);
        return table[exponent];
      }

    private:
      std::array<uint8_t, 255> table{};
    };

    static inline GeneratorPowerTable generator_power_table;
    static inline MultiplyInverseTable multiply_inverse_table;

  private:
    byte_stream_type byte_vector;
  };

  inline Vector operator+(const Vector &a, const Vector &b) {
    return Vector(a) += b.get_byte_vector();
  }

} // namespace raid_fs::galois_field
