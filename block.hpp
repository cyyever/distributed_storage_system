/*!
 * \file block.hpp
 *
 * \brief Contains definitions of various blocks
 */
#pragma once
#include <span>
#include <string>

namespace raid_fs {
  using block_data_type = std::string;
  using block_data_view_type = std::span<char>;
  using const_block_data_view_type = std::span<const char>;
} // namespace raid_fs
