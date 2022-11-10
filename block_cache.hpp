/*!
 * \file block_cache.hpp
 *
 * \brief Block cache and underlying synchronisation mechanism
 */
#pragma once
#include <cassert>
#include <cstdint>

#include <cyy/algorithm/dict/cache.hpp>

#include "block.hpp"
#include "raid_controller.hpp"
namespace raid_fs {
  class BlockCacheBackend
      : public ::cyy::algorithm::storage_backend<uint64_t, block_ptr_type> {
  public:
    explicit BlockCacheBackend(
        std::shared_ptr<RAIDController> raid_controller_ptr_,
        size_t block_size_)
        : raid_controller_ptr(raid_controller_ptr_), block_size(block_size_) {
      raid_block_size = raid_controller_ptr->get_block_size();
      if (block_size % raid_block_size != 0) {
        throw std::invalid_argument(
            "file system block size must be a multiple of RAID block size");
      }
    }
    ~BlockCacheBackend() override = default;

    std::vector<key_type> get_keys() override {
      throw std::runtime_error("shouldn't be called");
    }

    bool contains(const key_type &block_no) override {
      auto block_number = raid_controller_ptr->get_capacity() / block_size;
      return block_no < block_number;
    }
    mapped_type load_data(const key_type &block_no) override {
      block_data_type fs_block;
      auto ratio = block_size / raid_block_size;
      std::set<key_type> raid_block_no_set;
      for (size_t i = block_no * ratio; i < (block_no + 1) * ratio; i++) {
        raid_block_no_set.insert(i);
      }
      auto res = raid_controller_ptr->read_blocks(raid_block_no_set);
      if (!res.has_value()) {
        throw std::runtime_error(
            fmt::format("failed to read block {}", block_no));
      }
      for (auto &[_, block] : res.value()) {
        fs_block.append(std::move(block));
      }
      return std::make_shared<Block>(std::move(fs_block));
    }
    void clear_data() override {}
    void erase_data(const key_type &) override {
      throw std::runtime_error("shouldn't be called");
    }
    void save_data(const key_type &block_no, mapped_type block) override {
      assert(block->dirty);
      auto ratio = block_size / raid_block_size;
      std::map<key_type, std::string> raid_blocks;
      for (size_t i = block_no * ratio; i < (block_no + 1) * ratio; i++) {
        raid_blocks[i] =
            block->data.substr(i * raid_block_size, raid_block_size);
      }

      auto err_res = raid_controller_ptr->write_blocks(raid_blocks);
      if (err_res.has_value()) {
        throw std::runtime_error("failed to write block");
      }
    }

  private:
    std::shared_ptr<RAIDController> raid_controller_ptr;
    size_t block_size;
    size_t raid_block_size{};
  };

  class BlockCache : public ::cyy::algorithm::cache<uint64_t, block_ptr_type> {
  public:
    explicit BlockCache(
        size_t capacity, size_t block_size,
        const std::shared_ptr<RAIDController> &raid_controller_ptr)
        : cyy::algorithm::cache<uint64_t, block_ptr_type>(
              std::make_unique<BlockCacheBackend>(raid_controller_ptr,
                                                  block_size)) {
      this->set_in_memory_number(capacity);
      this->set_fetch_thread_number(1);
      this->set_saving_thread_number(1);
    }
    ~BlockCache() override = default;
  };
} // namespace raid_fs
