/*!
 * \file block_cache.hpp
 *
 * \brief Block cache and underlying synchronisation mechanism
 */
#pragma once
#include <cassert>
#include <cstdint>

#include <cyy/algorithm/dict/lru_cache.hpp>

#include "fs_block.hpp"
#include "raid_controller.hpp"
namespace raid_fs {
  class BlockCacheBackend
      : public ::cyy::algorithm::storage_backend<uint64_t, block_ptr_type> {
  public:
    explicit BlockCacheBackend(
        std::shared_ptr<RAIDController> raid_controller_ptr_,
        size_t block_size_)
        : raid_controller_ptr(raid_controller_ptr_), block_size(block_size_) {
      if (raid_controller_ptr->get_capacity() % block_size != 0) {
        throw std::invalid_argument(
            "RAID capacity must be a multiple of file system block size");
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

    std::unordered_map<key_type, mapped_type>
    batch_load_data(const std::vector<key_type> &keys) override {
      std::set<RAIDController::LogicalRange> data_ranges;
      for (auto block_no : keys) {
        data_ranges.emplace(block_no * block_size, block_size);
      }

      auto raid_res = raid_controller_ptr->read(data_ranges);
      std::unordered_map<key_type, mapped_type> results;
      for (auto &[range, block_data] : raid_res) {
        results.emplace(range.first / block_size,
                        std::make_shared<Block>(std::move(block_data)));
      }
      return results;
    }

    std::optional<mapped_type> load_data(const key_type &) override {
      throw std::runtime_error("shouldn't be called");
    }
    void clear() override {}
    void erase_data(const key_type &) override {}
    bool save_data(const key_type &, mapped_type) override {
      throw std::runtime_error("shouldn't be called");
    }
    std::vector<std::pair<key_type, bool>> batch_save_data(
        std::vector<std::pair<key_type, mapped_type>> batch_data) override {
      std::map<uint64_t, std::string> data;
      for (auto [block_no, block_ptr] : batch_data) {
        data.emplace(block_no * block_size, block_ptr->data);
      }
      auto raid_res = raid_controller_ptr->write(data);
      std::vector<std::pair<key_type, bool>> res;
      for (auto const &[block_no, _] : batch_data) {
        if (raid_res.contains(block_no * block_size)) {
          res.emplace_back(block_no, true);
        } else {
          res.emplace_back(block_no, false);
        }
      }
      return res;
    }

  private:
    std::shared_ptr<RAIDController> raid_controller_ptr;
    size_t block_size;
  };

  class BlockCache
      : public ::cyy::algorithm::lru_cache<uint64_t, block_ptr_type> {
  public:
    explicit BlockCache(
        size_t capacity, size_t block_size,
        const std::shared_ptr<RAIDController> &raid_controller_ptr)
        : cyy::algorithm::lru_cache<uint64_t, block_ptr_type>(
              std::make_unique<BlockCacheBackend>(raid_controller_ptr,
                                                  block_size)) {
      this->set_in_memory_number(capacity / block_size);
      LOG_DEBUG("cache {} blocks", capacity / block_size);
      this->set_fetch_thread_number(1);
      this->set_saving_thread_number(1);
    }
    ~BlockCache() override = default;
  };
} // namespace raid_fs
