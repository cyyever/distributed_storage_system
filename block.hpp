/*!
 * \file block.hpp
 *
 * \brief Contains definitions of various blocks
 */
#pragma once
#include <cassert>
#include <cstdint>
#include <string>

#include <fmt/format.h>

#include "type.hpp"
namespace raid_fs {
  struct alignas(128) SuperBlock {
    char fs_type[8];
    uint16_t raid_version;
    uint16_t fs_version;
    uint64_t bitmap_byte_offset;
    uint64_t inode_table_offset;
    uint64_t inode_number;
    uint64_t data_table_offset;
    uint64_t data_block_number;
  };
  static_assert(sizeof(SuperBlock) == 128);
  enum class file_type : uint8_t {
    free_dir_entry_head = 0,
    directory = 1,
    file = 2,
    free_dir_entry = 3,
  };

  struct alignas(128) INode {
    file_type type{};
    uint64_t size{}; // file size or the number of files in the directory
    uint64_t block_ptrs[14]{};

    uint64_t get_max_file_size(uint64_t block_size) {
      return block_size * std::size(block_ptrs);
    }
  };
  static_assert(sizeof(INode) == 128);

  struct Block {
    Block() : data(block_size, '\0') { assert(block_size != 0); }
    explicit Block(block_data_type data_) : data(std::move(data_)) {
      assert(block_size != 0 && data.size() == block_size);
    }

    Block(const Block &rhs) = default;
    Block &operator=(const Block &rhs) = default;
    Block(Block &&) noexcept = default;
    Block &operator=(Block &&) noexcept = default;

    SuperBlock &as_super_block() {
      assert(data.size() >= sizeof(SuperBlock));
      return *reinterpret_cast<SuperBlock *>(data.data());
    }

    static inline size_t block_size = 0;
    block_data_type data;
  };
  using block_ptr_type = std::shared_ptr<Block>;

  struct alignas(256) DirEntry {
    file_type type{};
    uint64_t inode_no{};
    char name[128]{};
  };
  static_assert(sizeof(DirEntry) == 256);

} // namespace raid_fs
