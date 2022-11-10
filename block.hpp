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

  struct alignas(128) INode {
    uint64_t size{};
    time_t mtime{}; // time of last modification
    uint64_t block_ptrs[14]{};
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
      return *reinterpret_cast<SuperBlock *>(data.data());
    }

    static inline size_t block_size = 0;
    bool dirty{false};
    block_data_type data;
  };
  using block_ptr_type = std::shared_ptr<Block>;

  struct INodeBlock : public Block {
    INodeBlock() { assert(block_size % sizeof(INode) == 0); }

    INode &get_inode(size_t idx) {
      if (idx >= block_size / sizeof(INode)) {
        throw std::range_error(fmt::format("block index {} out of range", idx));
      }
      auto *ptr = reinterpret_cast<INode *>(data.data());
      return ptr[idx];
    }
  };

} // namespace raid_fs
