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

    std::vector<key_type> load_keys() override {
      auto block_number = raid_controller_ptr->get_capacity() / block_size;
      std::vector<key_type> keys;
      keys.reserve(block_number);
      for (size_t i = 0; i < block_number; i++) {
        keys.push_back(i);
      }
      return keys;
    }
    mapped_type load_data(const key_type &block_no) override {
      block_data_type fs_block;
      auto ratio = block_size / raid_block_size;
      for (size_t i = block_no * ratio; i < (block_no + 1) * ratio; i++) {
        auto res = raid_controller_ptr->read_block(i);
        if (!res.has_value()) {
          throw std::runtime_error(fmt::format("failed to read block {}", i));
        }
        fs_block.append(std::move(res.value()));
      }
      return std::make_shared<Block>(std::move(fs_block));
    }
    void clear_data() override {}
    void erase_data(const key_type &) override {}
    void save_data(const key_type &block_no, mapped_type block) override {
      assert(block->dirty);
      auto err_res =
          raid_controller_ptr->write_block(block_no, std::move(block->data));
      if (err_res.has_value()) {
        throw std::runtime_error("failed to read block");
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
