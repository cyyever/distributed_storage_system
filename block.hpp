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
  struct SuperBlock {
    char fs_type[8];
    uint16_t fs_version;
    uint64_t block_size;
    uint64_t bitmap_byte_offset;
    uint64_t inode_table_offset;
    uint64_t inode_number;
    uint64_t next_inode_offset;
    uint64_t data_table_offset;
    uint64_t data_block_number;

  public:
    uint64_t get_data_bitmap_byte_offset() const {
      return bitmap_byte_offset + inode_number / 8;
    }
  };
  static_assert(sizeof(SuperBlock) == 72);

  enum class file_type : uint8_t {
    free_dir_entry_head = 0,
    directory = 1,
    file = 2,
    free_dir_entry = 3,
  };

  struct INode {
    file_type type{};
    uint64_t size{}; // file size
    // 12 direct pointers, 1 indirect pointer, 1 double indirect pointer
    uint64_t block_ptrs[14]{};

  public:
    static constexpr uint64_t get_direct_block_pointer_number() { return 12; }
    static constexpr uint64_t get_indirect_block_pointer_number() { return 1; }
    static constexpr uint64_t get_double_indirect_block_pointer_number() {
      return 1;
    }
    uint64_t get_max_file_size(uint64_t block_size) const {
      auto number_of_ptr_per_block = block_size / sizeof(block_ptrs[0]);
      return block_size *
             (get_direct_block_pointer_number() +
              number_of_ptr_per_block * get_indirect_block_pointer_number() +
              number_of_ptr_per_block * number_of_ptr_per_block *
                  get_double_indirect_block_pointer_number());
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

  struct DirEntry {
    file_type type{};
    uint64_t inode_no{};
    char name[128]{};
    char pading[109]{};
  };
  static_assert(sizeof(DirEntry) == 256);

} // namespace raid_fs
