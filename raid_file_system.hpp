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
#include <tuple>
#include <utility>

#include <cyy/naive_lib/util/runnable.hpp>
#include <fmt/format.h>

#include "block.hpp"
#include "block_cache.hpp"
#include "config.hpp"
#include "error.pb.h"
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
      if (block_size % sizeof(INode) != 0) {
        throw std::runtime_error("block size is not a multiple of inodes");
      }

      if (block_size % sizeof(DirEntry) != 0) {
        throw std::runtime_error(
            "block size is not a multiple of directory entries");
      }

      // file system is not initialized
      if (std::string(get_super_block().fs_type) != raid_fs_type) {
        make_filesystem();
      }
      sync_thread.start("sync thread");
    }

    ~RAIDFileSystem() { sync_thread.stop(); }
    std::optional<Error> create(const std::string &path) {
      if (!is_valid_path(path)) {
        return Error::ERROR_INVALID_PATH;
      }
      return {};
    }

  private:
    class SyncThread final : public cyy::naive_lib::runnable {
    public:
      SyncThread(RAIDFileSystem *impl_ptr_) : impl_ptr(impl_ptr_) {}
      ~SyncThread() override { stop(); }

    private:
      void run(const std::stop_token &st) override {
        while (true) {
          // TODO
          /* std::unique_lock lk(impl_ptr->block_mu); */
          /* if (cv.wait_for(lk, st, std::chrono::minutes(5), */
          /*                 [&st]() { return st.stop_requested(); })) { */
          /*   return; */
          /* } */
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

    BlockCache::value_reference get_mutable_block(uint64_t block_no) {
      auto res = block_cache.mutable_get(block_no);
      if (!res.has_value()) {
        throw std::runtime_error(
            fmt::format("failed to read block {}", block_no));
      }
      return std::move(res.value());
    }
    INode get_inode(uint64_t inode_no) {
      auto [inode_ptr, block_ref] = get_mutable_inode(inode_no);
      block_ref.cancel_writeback();
      return *inode_ptr;
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
      std::unique_lock lk(metadata_mutex);
      auto inode_no_opt = allocate_inode();
      if (!inode_no_opt.has_value()) {
        return {};
      }
      auto inode_no = inode_no_opt.value();
      auto [inode_ptr, inode_block_ref] = get_mutable_inode(inode_no);
      // allocate a data block in advance
      auto data_block_ref_opt = get_mutable_data_block_of_file(*inode_ptr, 0);
      if (!data_block_ref_opt.has_value()) {
        auto success = release_inode(inode_no);
        assert(success);
        return {};
      }
      lk.unlock();

      // zero initialization
      *inode_ptr = INode{};
      inode_ptr->type = type;
      LOG_DEBUG("data block no is {}", data_block_ref_opt.value().get_key());

      if (type == file_type::directory) {
        auto dir_entry_ptr = reinterpret_cast<DirEntry *>(
            data_block_ref_opt.value()->data.data());
        dir_entry_ptr->inode_no = 0;
        dir_entry_ptr->type = file_type::free_dir_entry_head;
        inode_ptr->size = sizeof(DirEntry);
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

    uint64_t write_data(INode &inode, uint64_t offset,
                        const_block_data_view_type view) {
      if (offset + view.size() > inode.get_max_file_size(block_size)) {
        return {};
      }

      while (offset > inode.size) {
        auto data_block_ref_opt =
            get_mutable_data_block_of_file(inode, inode.size / block_size);
        if (!data_block_ref_opt.has_value()) {
          return 0;
        }
        auto length =
            std::min(block_size - inode.size % block_size, offset - inode.size);
        memset(data_block_ref_opt.value()->data.data() +
                   inode.size % block_size,
               0, length);
        inode.size += length;
      }
      uint64_t written_bytes = 0;
      while (view.size()) {
        auto data_block_ref_opt =
            get_mutable_data_block_of_file(inode, offset / block_size);
        if (!data_block_ref_opt.has_value()) {
          return written_bytes;
        }
        auto length = std::min(block_size - offset % block_size, view.size());
        memcpy(data_block_ref_opt.value()->data.data() + offset % block_size,
               view.data(), length);
        inode.size = std::max(offset + length, inode.size);
        view = view.subspan(length);
        offset += length;
        written_bytes += length;
      }
      return written_bytes;
    }

    block_data_type read_data(INode &inode, uint64_t offset, uint64_t length) {
      block_data_type data;
      while (offset < inode.size) {
        auto data_block_ptr =
            get_data_block_of_file(inode, offset / block_size);
        auto partial_length =
            std::min(block_size - offset % block_size, length);
        data.append(data_block_ptr->data.data() + offset % block_size,
                    partial_length);
        offset += partial_length;
      }
      return data;
    }

    const block_ptr_type get_data_block_of_file(const INode &inode,
                                                uint64_t data_block_no_inode) {
      auto block_ptr = inode.block_ptrs[data_block_no_inode];
      if (block_ptr == 0) {
        throw std::runtime_error("invalid block");
      }
      return get_block(block_ptr);
    }

    std::optional<BlockCache::value_reference>
    get_mutable_data_block_of_file(INode &inode, uint64_t data_block_no_inode) {
      auto block_ptr = inode.block_ptrs[data_block_no_inode];
      if (block_ptr != 0) {
        return get_mutable_data_block(block_ptr);
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
      inode.block_ptrs[data_block_no_inode] = data_block_ref.get_key();
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

    using INodeLock = std::tuple<uint64_t, std::unique_lock<std::shared_mutex>,
                                 std::shared_ptr<std::shared_mutex>>;

    using INodeSharedLock =
        std::tuple<uint64_t, std::shared_lock<std::shared_mutex>,
                   std::shared_ptr<std::shared_mutex>>;
    template <bool use_shared_lock> auto lock_inode(uint64_t inode_no) {
      std::shared_lock lk(metadata_mutex);
      while (!inode_mutexes.contains(inode_no)) {
        lk.unlock();
        {
          std::unique_lock lk2(metadata_mutex);
          if (!inode_mutexes.contains(inode_no)) {
            inode_mutexes[inode_no] = std::make_shared<std::shared_mutex>();
          }
        }
        lk.lock();
      }
      auto mutex_ptr = inode_mutexes[inode_no];
      lk.unlock();
      if constexpr (use_shared_lock) {
        return std::make_tuple(inode_no,
                               std::shared_lock<std::shared_mutex>(*mutex_ptr),
                               mutex_ptr);
      } else {
        return std::make_tuple(inode_no,
                               std::unique_lock<std::shared_mutex>(*mutex_ptr),
                               mutex_ptr);
      }
    }

    std::expected<INodeLock, Error> travel_path(const std::filesystem::path &p,
                                                bool create_missing,
                                                bool is_dir = false) {
      if (p == "/") {
        return lock_inode<false>(root_inode_no);
      }

      auto parent_res = travel_path(p.parent_path(), create_missing, true);
      if (!parent_res.has_value()) {
        return parent_res;
      }
      auto [parent_inode_ptr, parant_inode_block_ref] =
          get_mutable_inode(std::get<0>(parent_res.value()));
      uint64_t p_inode_no = 0;
      Error error = ERROR_UNSPECIFIED;
      iterate_dir_entries(*parent_inode_ptr, [&](const DirEntry &dir) {
        if (strcmp(dir.name, p.filename().c_str()) == 0) {
          if (is_dir && dir.type != file_type::directory) {
            error = ERROR_PATH_COMPONENT_IS_FILE;
            return true;
          }
          if (!is_dir && dir.type != file_type::file) {
            error = ERROR_PATH_COMPONENT_IS_DIR;
            return true;
          }
          p_inode_no = dir.inode_no;
          return true;
        }
        return false;
      });
      if (p_inode_no != 0) {
        return lock_inode<false>(p_inode_no);
      }
      if (!create_missing) {
        if (error == ERROR_UNSPECIFIED) {
          error = ERROR_UNEXISTED_FILE;
        }
        return std::unexpected(error);
      }
      DirEntry new_entry;
      new_entry.type = is_dir ? file_type::directory : file_type::file;
      auto inode_opt = allocate_and_initialize_file_metadata(new_entry.type);
      if (!inode_opt.has_value()) {
        LOG_ERROR("failed to allocate space for {}", p.string());
        return std::unexpected(ERROR_FILE_SYSTEM_FULL);
      }
      strcpy(new_entry.name, p.filename().c_str());
      new_entry.inode_no = inode_opt.value();
      if (!allocate_dir_entry(*parent_inode_ptr, new_entry)) {
        LOG_ERROR("failed to allocate space for {}", p.string());
        std::unique_lock lk(metadata_mutex);
        release_inode(new_entry.inode_no);
        return std::unexpected(ERROR_FILE_SYSTEM_FULL);
      }
      return lock_inode<false>(new_entry.inode_no);
    }

    bool allocate_dir_entry(INode &inode, const DirEntry &new_entry) {
      assert(inode.type == file_type::directory);
      auto first_block = get_mutable_block(inode.block_ptrs[0]);
      DirEntry *first_dir_entry_ptr =
          reinterpret_cast<DirEntry *>(first_block->data.data());
      assert(first_dir_entry_ptr->type == file_type::free_dir_entry_head);
      auto free_dir_entry_no = first_dir_entry_ptr->inode_no;
      uint64_t written_bytes = 0;
      if (free_dir_entry_no != 0) {
        auto data = read_data(inode, free_dir_entry_no * sizeof(DirEntry),
                              sizeof(DirEntry));
        first_dir_entry_ptr->inode_no =
            reinterpret_cast<DirEntry *>(data.data())->inode_no;
      } else {
        free_dir_entry_no = inode.size / sizeof(DirEntry);
      }
      std::lock_guard lk(metadata_mutex);
      written_bytes = write_data(
          inode, free_dir_entry_no * sizeof(DirEntry),
          const_block_data_view_type(reinterpret_cast<const char *>(&new_entry),
                                     sizeof(DirEntry)));
      if (written_bytes == 0) {
        return false;
      }
      if (written_bytes != sizeof(DirEntry)) {
        throw std::runtime_error(fmt::format("invalid write operation {} {}",
                                             written_bytes, sizeof(DirEntry)));
      }
      return true;
    }

    void iterate_dir_entries(
        INode &inode,
        std::function<bool(const DirEntry &)> dir_entry_callback) {
      assert(inode.type == file_type::directory);
      uint64_t remain_size = inode.size;
      bool finish = false;
      for (auto block_ptr : inode.block_ptrs) {
        if (block_ptr == 0) {
          throw std::runtime_error("empty block in directory");
        }
        uint64_t length = std::min(block_size, remain_size);
        iterate_bytes(
            block_ptr * block_size, length,
            [&finish, &dir_entry_callback](block_data_view_type view,
                                           size_t) -> std::pair<bool, bool> {
              assert(view.size() % sizeof(DirEntry) == 0);
              DirEntry *dir_entry_ptr =
                  reinterpret_cast<DirEntry *>(view.data());
              for (size_t i = 0; i < view.size() / sizeof(DirEntry); i++) {
                if (dir_entry_ptr[i].type == file_type::free_dir_entry ||
                    dir_entry_ptr[i].type == file_type::free_dir_entry_head) {
                  continue;
                }
                if (dir_entry_callback(dir_entry_ptr[i])) {
                  finish = true;
                  return {false, true};
                }
              }
              return {false, false};
            });
        remain_size -= length;
        if (!remain_size || finish) {
          break;
        }
      }
    }

  private:
    friend SyncThread;
    static constexpr auto raid_fs_type = "RAIDFS";
    static constexpr uint64_t super_block_no = 0;
    size_t block_size;
    size_t block_number;
    BlockCache block_cache;
    std::shared_mutex metadata_mutex;
    std::unordered_map<uint64_t, std::shared_ptr<std::shared_mutex>>
        inode_mutexes;
    SyncThread sync_thread;
    uint64_t root_inode_no{};
  };
} // namespace raid_fs
