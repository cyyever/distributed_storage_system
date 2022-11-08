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
        std::shared_ptr<RAIDController> RAID_controller_ptr_)
        : RAID_controller_ptr(RAID_controller_ptr_) {}
    ~BlockCacheBackend() override = default;

    std::vector<uint64_t> load_keys() override { return {}; }
    Block load_data(const uint64_t &block_no) override {
      auto res = RAID_controller_ptr->read_block(block_no);
      if (!std::holds_alternative<std::string>(res)) {
        throw std::runtime_error("failed to read block");
      }
    }
    void clear_data() override {}
    void erase_data(const uint64_t &block_no) override {}
    void save_data(const uint64_t &block_no, Block block) override {
      RAID_controller_ptr->write_block(block_no, std::move(block.data));
    }

  private:
    std::shared_ptr<RAIDController> RAID_controller_ptr;
  };

  /*   class BlockCache : public ::cyy::algorithm::cache<uint64_t, Block> { */
  /*   public: */
  /*     explicit BlockCache(size_t capacity); */
  /*     ~BlockCache() override; */

  /*   private: */
  /*     std::shared_ptr<RAIDController> RAID_controller_ptr; */
  /*   }; */
} // namespace raid_fs
