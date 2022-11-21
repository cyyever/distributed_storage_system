/*!
 * \file block.hpp
 *
 * \brief Contains definitions of various blocks
 */
#pragma once
#include <ranges>
#include <span>
#include <string>

namespace raid_fs {
  using byte_stream_type = std::string;
  using block_data_type = std::string;
  using block_data_view_type = std::span<char>;
  using const_block_data_view_type = std::span<const char>;

  struct LogicalAddressRange {
    uint64_t offset;
    uint64_t length;
    auto operator<=>(const LogicalAddressRange &rhs) const = default;
    bool operator==(const LogicalAddressRange &rhs) const = default;

    auto split(uint64_t block_size) const {
      auto block_count = (offset + length) / block_size - offset / block_size;
      return std::views::iota(uint64_t(0), block_count) |
             std::views::transform([=, this](auto idx) {
               if (idx == 0) {
                 return offset;
               }
               auto second_offset = offset + (block_size - offset % block_size);
               return second_offset + (idx - 1) * block_size;
             });
    }
  };

} // namespace raid_fs
