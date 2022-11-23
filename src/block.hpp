/*!
 * \file block.hpp
 *
 * \brief Contains definitions of various blocks
 */
#pragma once
#include <cassert>
#include <ranges>
#include <span>
#include <string>
#include <vector>

namespace raid_fs {
  using byte_stream_type = std::string;
  using byte_stream_view_type = std::span<char>;
  using const_byte_stream_view_type = std::span<const char>;
  using block_data_type = byte_stream_type;
  using block_data_view_type = byte_stream_view_type;
  using const_block_data_view_type = const_byte_stream_view_type;

  // data offset and length
  struct LogicalAddressRange {
    LogicalAddressRange(uint64_t offset_, uint64_t length_)
        : offset{offset_}, length{length_} {}
    auto operator<=>(const LogicalAddressRange &rhs) const = default;
    bool operator==(const LogicalAddressRange &rhs) const = default;

    auto split(uint64_t block_size) const {
      auto block_count =
          (offset + length - 1) / block_size - offset / block_size + 1;
      auto tmp_offset = offset;
      auto tmp_length = length;
      return std::views::iota(uint64_t(0), block_count) |
             std::views::transform([tmp_offset, tmp_length,
                                    block_size](auto idx) {
               auto first_piece_length =
                   std::min(tmp_length, block_size - tmp_offset % block_size);
               if (idx == 0) {
                 return LogicalAddressRange(tmp_offset, first_piece_length);
               }
               auto second_piece_offset = tmp_offset + first_piece_length;
               auto piece_offset = second_piece_offset + (idx - 1) * block_size;
               auto piece_length = std::min(
                   block_size, (tmp_offset + tmp_length) - piece_offset);
               return LogicalAddressRange(piece_offset, piece_length);
             });
    }
    uint64_t offset{};
    uint64_t length{};
  };

} // namespace raid_fs
