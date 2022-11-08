/*!
 * \file block_cache.hpp
 *
 * \brief Block cache and underlying synchronisation mechanism
 */
#pragma once
#include <cassert>
#include <cstdint>
#include <string>

#include <cyy/algorithm/dict/cache.hpp>
#include <fmt/format.h>

#include "block.hpp"
#include "raid_controller.hpp"
namespace raid_fs {
  class BlockCacheBackend
      : public ::cyy::algorithm::storage_backend<uint64_t, Block> {
  public:
    explicit BlockCacheBackend(
        std::shared_ptr<RAIDController> raid_controller_ptr_)
        : raid_controller_ptr(raid_controller_ptr_) {}
    ~BlockCacheBackend() override = default;

    std::vector<uint64_t> load_keys() override { return {}; }
    Block load_data(const uint64_t &block_no) override {
      auto res = raid_controller_ptr->read_block(block_no);
      if (!res.has_value()) {
        throw std::runtime_error("failed to read block");
      }
      return Block(std::move(res.value()));
    }
    void clear_data() override {}
    void erase_data(const uint64_t &block_no) override {}
    void save_data(const uint64_t &block_no, Block block) override {
      auto err_res =
          raid_controller_ptr->write_block(block_no, std::move(block.data));
      if (err_res.has_value()) {
        throw std::runtime_error("failed to read block");
      }
    }

  private:
    std::shared_ptr<RAIDController> raid_controller_ptr;
  };

  class BlockCache : public ::cyy::algorithm::cache<uint64_t, Block> {
  public:
    explicit BlockCache(
        size_t capacity,
        const std::shared_ptr<RAIDController> &raid_controller_ptr)
        : cyy::algorithm::cache<uint64_t, Block>(
              std::make_unique<BlockCacheBackend>(raid_controller_ptr)) {
      this->set_in_memory_number(capacity);
    }
    ~BlockCache() override = default;
  };
} // namespace raid_fs
