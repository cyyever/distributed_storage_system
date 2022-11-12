/*!
 * \file file_system.hpp
 *
 * \brief Implementation of raid_fs file system
 */

#pragma once
#include <algorithm>
#include <functional>
#include <ranges>
#include <shared_mutex>
#include <utility>

#include <cyy/naive_lib/util/runnable.hpp>
#include <fmt/format.h>

#include "block.hpp"
#include "block_cache.hpp"
#include "config.hpp"
#include "raid_controller.hpp"

namespace raid_fs {

  class RAIDFileSystem {
  public:
    RAIDFileSystem(const FileSystemConfig &fs_cfg,
                   const std::shared_ptr<RAIDController> &raid_controller_ptr)
        : block_size(fs_cfg.block_size),
          block_number(raid_controller_ptr->get_capacity() / fs_cfg.block_size),
          block_cache(fs_cfg.block_pool_size, fs_cfg.block_size,
                      raid_controller_ptr),
          sync_thread(this) {

      // file system is not initialized
      if (std::string(get_super_block().fs_type) != raid_fs_type) {
        make_filesystem();
      }
      sync_thread.start("sync thread");
    }

    ~RAIDFileSystem() { sync_thread.stop(); }

  private:
    class SyncThread final : public cyy::naive_lib::runnable {
    public:
      SyncThread(RAIDFileSystem *impl_ptr_) : impl_ptr(impl_ptr_) {}
      ~SyncThread() override { stop(); }

    private:
      void run(const std::stop_token &st) override {
        while (true) {
          std::unique_lock lk(impl_ptr->block_mu);
          if (cv.wait_for(lk, st, std::chrono::minutes(5),
                          [&st]() { return st.stop_requested(); })) {
            return;
          }
          impl_ptr->block_cache.flush();
        }
      }

    private:
      std::condition_variable_any cv;
      RAIDFileSystem *impl_ptr;
    };

  private:
    const block_ptr_type get_block(uint64_t block_no) {
      auto res = block_cache.get(block_no);
      if (!res.has_value()) {
        throw std::runtime_error(
            fmt::format("failed to read block {}", block_no));
      }
      assert(res.value().get() != nullptr);
      return res.value();
    }

    std::pair<INode *, BlockCache::value_reference>
    get_mutable_inode(uint64_t inode_no) {
      auto const super_block = get_super_block();
      assert(inode_no < super_block.inode_number);
      auto inodes_per_block = block_size / sizeof(INode);
      auto block_no =
          super_block.inode_table_offset + inode_no / inodes_per_block;
      auto block_ref = get_mutable_block(block_no);
      auto inode_ptr = reinterpret_cast<INode *>(block_ref->data.data()) +
                       (inode_no % inodes_per_block);
      return {inode_ptr, std::move(block_ref)};
    }

    BlockCache::value_reference get_mutable_data_block(uint64_t data_block_no) {
      auto const super_block = get_super_block();
      assert(data_block_no < super_block.data_block_number);
      auto block_no = super_block.data_table_offset + data_block_no;
      return get_mutable_block(block_no);
    }

    INode get_inode(uint64_t inode_no) {
      auto [inode_ptr, block_ref] = get_mutable_inode(inode_no);
      block_ref.cancel_writeback();
      return *inode_ptr;
    }

    BlockCache::value_reference get_mutable_block(uint64_t block_no) {
      auto res = block_cache.mutable_get(block_no);
      if (!res.has_value()) {
        throw std::runtime_error(
            fmt::format("failed to read block {}", block_no));
      }
      return std::move(res.value());
    }
    // initialize file system layout like the mkfs command
    void make_filesystem() {
      {
        LOG_WARN("initialize file system");
        auto block_ref = get_mutable_block(super_block_no);
        auto &blk = block_ref->as_super_block();
        strcpy(blk.fs_type, raid_fs_type);
        blk.fs_version = 0;
        blk.bitmap_byte_offset = sizeof(SuperBlock);
        blk.inode_number = block_number * 0.01;
        if (blk.inode_number == 0) {
          blk.inode_number = 1;
        }
        blk.inode_number = (blk.inode_number + 7) / 8 * 8;
        assert(blk.inode_number % 8 == 0 && blk.inode_number > 0);
        auto data_bitmap_byte_offset =
            blk.bitmap_byte_offset + blk.inode_number / 8;
        auto max_recordable_block_number =
            (block_size - data_bitmap_byte_offset % block_size) * 8;
        blk.inode_table_offset = data_bitmap_byte_offset / block_size + 1;
        while (true) {
          blk.data_table_offset = blk.inode_table_offset + blk.inode_number;
          blk.data_block_number = block_number - blk.data_table_offset;
          if (blk.data_block_number > max_recordable_block_number ||
              blk.data_block_number % 8 != 0) {
            LOG_DEBUG("{} {}", blk.data_block_number,
                      max_recordable_block_number);
            blk.inode_table_offset += 1;
            max_recordable_block_number += block_size * 8;
          } else {
            break;
          }
        }
        LOG_WARN("allocate {} inodes and {} data blocks, total {} blocks, "
                 "{} blocks for bookkeeping",
                 blk.inode_number, blk.data_block_number, block_number,
                 block_number - blk.inode_number - blk.data_block_number);
        // wrapped the above code in local scope such that the super block
        // changes are written to the cache now.
      }

      // allocate inode for the root directory '/'
      auto inode_opt =
          allocate_and_initialize_file_metadata(file_type::directory);
      if (!inode_opt.has_value()) {
        throw std::runtime_error("failed to allocate space for /");
      }
      root_inode_no = inode_opt.value();
      assert(root_inode_no == 0);
    }

    std::optional<uint64_t>
    allocate_and_initialize_file_metadata(file_type type) {
      auto inode_no_opt = allocate_inode();
      if (!inode_no_opt.has_value()) {
        return {};
      }
      auto inode_no = inode_no_opt.value();
      auto [inode_ptr, inode_block_ref] = get_mutable_inode(inode_no);

      // zero initialization
      *inode_ptr = INode{};
      inode_ptr->type = type;
      // allocate a data block in advance
      auto data_block_ref_opt = allocate_data_block(*inode_ptr);
      if (!data_block_ref_opt.has_value()) {
        auto success = release_inode(inode_no);
        assert(success);
        return {};
      }

      if (type == file_type::directory) {
        reinterpret_cast<DirEntry *>(data_block_ref_opt.value()->data.data())
            ->inode_no = 0;
      }

      return inode_no;
    }

    std::optional<uint64_t> allocate_inode() {
      const auto blk = get_super_block();
      LOG_DEBUG("inode_number is {}", blk.inode_number);
      return allocate_block(blk.bitmap_byte_offset, blk.inode_number / 8);
    }

    bool release_inode(uint64_t inode_no) {
      const auto blk = get_super_block();
      return release_block(blk.bitmap_byte_offset, inode_no);
    }

    bool release_data_block(uint64_t data_block_no) {
      const auto blk = get_super_block();
      return release_block(blk.bitmap_byte_offset + blk.inode_number / 8,
                           data_block_no);
    }

    std::optional<BlockCache::value_reference>
    allocate_data_block(INode &inode) {
      auto next_block_it = std::ranges::find(inode.block_ptrs, 0);
      if (next_block_it == std::end(inode.block_ptrs)) {
        return {};
      }

      const auto super_block = get_super_block();
      LOG_DEBUG("data block number is {}", super_block.data_table_offset);
      auto data_block_no_opt = allocate_block(
          super_block.bitmap_byte_offset + super_block.inode_number / 8,
          super_block.data_block_number / 8);
      if (!data_block_no_opt.has_value()) {
        return {};
      }
      auto data_block_ref = get_mutable_data_block(data_block_no_opt.value());
      *next_block_it = data_block_ref.get_key();
      return data_block_ref;
    }

    bool release_block(uint64_t bitmap_byte_offset,
                       uint64_t block_no_in_table) {
      bool res = false;
      iterate_bytes(
          bitmap_byte_offset + block_no_in_table / 8, 1,
          [&res, block_no_in_table](block_data_view_type view, size_t) {
            auto new_byte = std::byte(view[0]);
            std::byte mask{0b10000000};
            mask >>= (block_no_in_table % 8);
            if ((mask & new_byte) == mask) {
              new_byte |= (~mask);
              view[0] = std::to_integer<unsigned char>(new_byte);
              res = true;
              return std::pair<bool, bool>{true, true};
            }
            return std::pair<bool, bool>{false, true};
          });
      return res;
    }

    std::optional<uint64_t> allocate_block(uint64_t bitmap_byte_offset,
                                           uint64_t bitmap_length) {
      std::optional<uint64_t> res;
      iterate_bytes(
          bitmap_byte_offset, bitmap_length,
          [&res, bitmap_byte_offset](block_data_view_type view,
                                     size_t byte_offset) {
            std::byte zero_byte{0b00000000};
            for (size_t i = 0; i < view.size(); i++) {
              if ((unsigned char)(view[i]) < 255) {
                uint64_t inode_no = (byte_offset + i - bitmap_byte_offset) * 8;
                std::byte mask{0b10000000};
                auto new_byte = std::byte(view[i]);
                for (size_t j = 0; j < 8; j++, inode_no++, mask >>= 1) {
                  if ((mask & new_byte) == zero_byte) {
                    new_byte |= mask;
                    view[i] = std::to_integer<unsigned char>(new_byte);
                    res = inode_no;
                    return std::pair<bool, bool>{true, true};
                  }
                }
                throw std::runtime_error("must not be here");
              }
            }
            return std::pair<bool, bool>{false, false};
          });
      return res;
    }

    void iterate_bytes(size_t byte_offset, size_t length,
                       const std::function<std::pair<bool, bool>(
                           block_data_view_type data_view, size_t)> &callback) {
      while (length) {
        auto block_no = byte_offset / block_size;
        auto block = get_block(block_no);
        auto block_offset = byte_offset % block_size;
        auto block_length = block_size - block_offset;
        if (block_length > length) {
          block_length = length;
        }
        auto [changed, finish] = callback(
            block_data_view_type(&block->data[block_offset], block_length),
            byte_offset);
        if (changed) {
          block_cache.emplace(block_no, block);
        }
        if (finish) {
          return;
        }
        byte_offset += block_length;
        length -= block_length;
      }
    }
    SuperBlock get_super_block() {
      return get_block(super_block_no)->as_super_block();
    }

    bool is_valid_path(const std::string &path) {
      std::smatch match;
      std::regex re("^(/[0-9A-Za-z]+)+$");
      if (!std::regex_search(path, match, re)) {
        LOG_DEBUG("invalid path {}", path);
        return false;
      }
      for (const auto component : std::views::split(path, "/")) {
        if (component.size() > 127) {
          LOG_DEBUG("long component {}", component);
          return false;
        }
      }
      return true;
    }

  private:
    friend SyncThread;
    static constexpr auto raid_fs_type = "RAIDFS";
    static constexpr uint64_t super_block_no = 0;
    size_t block_size;
    size_t block_number;
    BlockCache block_cache;
    std::shared_mutex block_mu;
    SyncThread sync_thread;
    uint64_t root_inode_no{};
  };
} // namespace raid_fs
