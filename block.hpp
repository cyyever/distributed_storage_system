/*!
 * \file block.hpp
 *
 * \brief Contains definitions of various blocks
 */
#pragma once
#include <cstdint>
namespace raid_fs {
  struct alignas(128) SuperBlock {
    char FS_type[8];
    uint16_t FS_version;
    uint64_t bitmap_block_offset;
    uint64_t inode_bitmap_size;
    uint64_t data_bitmap_size;
    uint64_t inode_table_offset;
    uint64_t inode_number;
    uint64_t data_table_offset;
    uint64_t data_block_number;
  };
  static_assert(sizeof(SuperBlock) == 128);

  struct alignas(128) INode {
    uint64_t size;
    time_t atime; // time of last access
    time_t mtime; // time of last modification
    uint64_t block_number;
    uint64_t block_ptrs[12];
  };
  static_assert(sizeof(INode) == 128);

} // namespace raid_fs
