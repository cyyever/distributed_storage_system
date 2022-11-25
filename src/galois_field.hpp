/*!
 * \file galois_field.hpp
 *
 * \brief Implementation of Galois field GF(2^8) for RAID 6
 */

#include <variant>

#include <cyy/naive_lib/log/log.hpp>
#include <spdlog/fmt/fmt.h>

#include "block.hpp"
namespace raid_fs::galois_field {

  class Element {

  public:
    Element(uint8_t element_ = 0) : element(element_) {}

    uint8_t value() const { return element; }
    bool operator==(const Element &rhs) const = default;

    // Obtain additive inverse
    Element operator-() const { return *this; }
    Element &operator+=(Element rhs) {
      element ^= rhs.element;
      return *this;
    }

    Element &operator*=(Element rhs) {
      uint8_t res = 0;
      auto rhs_value = rhs.element;
      for (size_t i = 0; i < 7; i++) {
        if (rhs_value & 0x80) {
          res ^= element;
        }
        res = multiply_by_2(res);
        rhs_value <<= 1;
      }
      if (rhs_value & 0x80) {
        res ^= element;
      }
      element = res;
      return *this;
    }

    // Since additive inverse is itself under XOR, subtraction is the same as
    // addition
    Element &operator-=(Element rhs) { return operator+=(rhs); }

    static inline constexpr uint8_t multiply_by_2(uint8_t b) {
      return (b << 1) ^ ((b & 0x80) ? 0x1d : 0);
    }

  private:
    uint8_t element;
  };
  static inline Element operator+(const Element &a, const Element &b) {
    return Element(a) += b;
  }
  static inline Element operator*(const Element &a, const Element &b) {
    return Element(a) *= b;
  }

  class MultiplyInverseTable {
  public:
    MultiplyInverseTable() : table{} {
      for (size_t i = 1; i < 256; i++) {
        if (table[i] != 0) {
          continue;
        }
        for (size_t j = 1; j < 256; j++) {
          auto product = Element(i) * Element(j);
          if (product == 1) {
            assert(Element(j) * Element(i) == 1);
            table[i] = j;
            table[j] = i;
            break;
          }
        }
      }
    }
    Element get_inverse(Element a) const {
      assert(a.value() != 0);
      return table[a.value()];
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
        power = Element::multiply_by_2(power);
        table[i] = power;
      }
      power = Element::multiply_by_2(power);
      assert(power == 1);
    }
    Element get_negative_power(int negative_exponent) const {
      assert(negative_exponent <= 0);
      assert(negative_exponent > -256);
      auto exponent = (negative_exponent + 2 * 255) % 255;
      return table[exponent];
    }

    Element get_power(size_t exponent) const {
      assert(exponent < 256);
      return table[exponent];
    }

  private:
    std::array<uint8_t, 255> table{};
  };

  static inline GeneratorPowerTable generator_power_table;
  static inline MultiplyInverseTable multiply_inverse_table;

  class Vector {

  public:
    Vector(size_t byte_count) {
      if (byte_count == 0) {
        LOG_ERROR("can't support empty byte vector");
        throw std::runtime_error("can't support empty byte vector");
      }
      byte_stream_type stream(byte_count, 0);
      byte_vector = stream;
    }
    Vector(byte_stream_view_type byte_vector_) : byte_vector(byte_vector_) {
      if (byte_vector_.empty()) {
        LOG_ERROR("can't support empty byte vector");
        throw std::runtime_error("can't support empty byte vector");
      }
    }
    Vector(const_byte_stream_view_type byte_vector_)
        : byte_vector(byte_vector_) {
      if (byte_vector_.empty()) {
        LOG_ERROR("can't support empty byte vector");
        throw std::runtime_error("can't support empty byte vector");
      }
    }
    byte_stream_type &get_byte_vector() {
      return std::get<byte_stream_type>(byte_vector);
    }

    Vector &operator+=(const Vector &rhs) {
      check_rhs(rhs);
      for (size_t i = 0; i < size(); i++) {
        data()[i] += rhs.data()[i];
      }
      return *this;
    }

    Vector operator*(Element scalar) const {
      auto res = *this;
      for (size_t i = 0; i < size(); i++) {
        res.data()[i] *= scalar;
      }
      return res;
    }

    // +=(rhs*scalar)
    Vector &add_multiple(const Vector &rhs, Element scalar) {
      for (size_t i = 0; i < size(); i++) {
        data()[i] += rhs.data()[i] * scalar;
      }
      return *this;
    }

    // -=(rhs*scalar)
    Vector &subtract_multiple(const Vector &rhs, Element scalar) {
      return add_multiple(rhs, scalar);
    }

    // Since additive inverse is itself under XOR, subtraction is the same as
    // addition
    Vector &operator-=(const Vector &rhs) { return operator+=(rhs); }

  private:
    void check_rhs(const Vector &rhs) {

      assert(size() == rhs.size());
      if (size() != rhs.size()) {
        LOG_ERROR("can't add byte vectors with different sizes: {} and {}",
                  size(), rhs.size());

        throw std::runtime_error(fmt ::format(
            "can't add byte vectors with different sizes: {} and {}", size(),
            rhs.size()));
      }
    }
    Element *data() {
      if (auto *ptr = std::get_if<byte_stream_type>(&byte_vector))
        return reinterpret_cast<Element *>(ptr->data());
      else if (auto *ptr = std::get_if<byte_stream_view_type>(&byte_vector))
        return reinterpret_cast<Element *>(ptr->data());
      else {
        throw std::runtime_error("bad access");
      }
    }
    const Element *data() const {
      if (const auto *ptr = std::get_if<byte_stream_type>(&byte_vector))
        return reinterpret_cast<const Element *>(ptr->size());
      else if (auto *ptr = std::get_if<byte_stream_view_type>(&byte_vector))
        return reinterpret_cast<const Element *>(ptr->data());
      else
        return reinterpret_cast<const Element *>(
            std::get<const_byte_stream_view_type>(byte_vector).data());
    }
    size_t size() const {
      if (const auto *ptr = std::get_if<byte_stream_type>(&byte_vector))
        return ptr->size();
      else if (auto *ptr = std::get_if<byte_stream_view_type>(&byte_vector))
        return ptr->size();
      else
        return std::get<byte_stream_view_type>(byte_vector).size();
    }

  private:
    std::variant<byte_stream_view_type, const_byte_stream_view_type,
                 byte_stream_type>
        byte_vector;
  };

  inline Vector operator+(Vector a, const Vector &b) { return a += b; }

} // namespace raid_fs::galois_field
