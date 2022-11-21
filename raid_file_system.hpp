/*!
 * \file file_system.hpp
 *
 * \brief Implementation of raid_fs file system
 */

#pragma once
#include <algorithm>
#include <bit>
#include <expected>
#include <functional>
#include <ranges>
#include <shared_mutex>
#include <tuple>
#include <utility>

#include <cyy/algorithm/dict/ordered_dict.hpp>
#include <cyy/naive_lib/util/runnable.hpp>
#include <spdlog/fmt/fmt.h>

#include "block_cache.hpp"
#include "config.hpp"
#include "error.pb.h"
#include "fs_block.hpp"
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
          sync_thread(this, std::chrono::seconds(fs_cfg.block_cache_seconds)) {
      if (block_size % sizeof(INode) != 0) {
        throw std::runtime_error("block size is not a multiple of inodes");
      }

      if (block_size % sizeof(DirEntry) != 0) {
        throw std::runtime_error(
            "block size is not a multiple of directory entries");
      }
      raid_fs::Block::block_size = block_size;

      // file system is not initialized
      if (std::string(get_super_block().fs_type) != raid_fs_type) {
        make_filesystem();
      } else {
        if (get_super_block().block_size != block_size) {
          throw std::runtime_error(fmt::format(
              "block size is not match with the recorded block size:{} {}",
              get_super_block().block_size, block_size));
        }
      }
      sync_thread.start("sync thread");
    }

    ~RAIDFileSystem() { sync_thread.stop(); }

    std::expected<std::pair<uint64_t, INode>, Error>
    open(const std::string &path, bool o_create, bool o_excl) {
      auto res = travel_path(path, o_create, o_excl);
      if (!res.has_value()) {
        return std::unexpected(res.error());
      }
      auto inode_no = std::get<0>(res.value());
      return std::pair<uint64_t, INode>{inode_no, get_inode(inode_no)};
    }

    std::optional<Error> remove_file(const std::filesystem::path &p) {
      if (p == "/") {
        return Error::ERROR_PATH_IS_DIR;
      }
      auto parent_res = travel_path(p.parent_path(), false, false, true);
      if (!parent_res.has_value()) {
        return parent_res.error();
      }
      auto parent_inode_no = std::get<0>(parent_res.value());
      auto [parent_inode_ptr, parent_inode_block_ref] =
          get_mutable_inode(parent_inode_no);

      auto first_block = get_mutable_block(parent_inode_ptr->block_ptrs[0]);
      DirEntry *first_dir_entry_ptr =
          reinterpret_cast<DirEntry *>(first_block->data.data());
      assert(first_dir_entry_ptr->type == file_type::free_dir_entry_head);

      std::optional<uint64_t> p_inode_no_opt;
      std::optional<Error> error_opt;
      auto basename = p.filename();
      iterate_dir_entries(*parent_inode_ptr,
                          [&](size_t entry_cnt, DirEntry &entry) {
                            if (basename.compare(entry.name) == 0) {
                              if (entry.type != file_type::file) {
                                error_opt = Error::ERROR_PATH_IS_DIR;
                              } else {
                                p_inode_no_opt = entry.inode_no;
                                entry.inode_no = first_dir_entry_ptr->inode_no;
                                first_dir_entry_ptr->inode_no = entry_cnt;
                                entry.type = file_type::free_dir_entry;
                              }
                              return std::pair<bool, bool>{true, false};
                            }
                            return std::pair<bool, bool>{false, false};
                          });
      if (error_opt) {
        return error_opt;
      }

      // no such file
      if (!p_inode_no_opt.has_value()) {
        return ERROR_NONEXISTENT_FILE;
      }
      auto file_lock = lock_inode(*p_inode_no_opt);
      release_file_metadata(*p_inode_no_opt);
      return {};
    }

    std::optional<Error> remove_dir(const std::filesystem::path &p) {
      if (p == "/") {
        return Error::ERROR_INVALID_PATH;
      }
      auto parent_res = travel_path(p.parent_path(), false, false, true);
      if (!parent_res.has_value()) {
        return parent_res.error();
      }
      auto parent_inode_no = std::get<0>(parent_res.value());
      auto [parent_inode_ptr, parent_inode_block_ref] =
          get_mutable_inode(parent_inode_no);

      auto first_block = get_mutable_block(parent_inode_ptr->block_ptrs[0]);
      DirEntry *first_dir_entry_ptr =
          reinterpret_cast<DirEntry *>(first_block->data.data());
      assert(first_dir_entry_ptr->type == file_type::free_dir_entry_head);

      std::optional<Error> error_opt;
      std::optional<DirEntry> p_entry_opt;
      auto basename = p.filename();
      iterate_dir_entries(*parent_inode_ptr,
                          [&](size_t entry_cnt, DirEntry &entry) {
                            if (basename.compare(entry.name) == 0) {
                              if (entry.type != file_type::directory) {
                                error_opt = Error::ERROR_PATH_IS_FILE;
                              } else {
                                p_entry_opt = entry;
                                entry.inode_no = first_dir_entry_ptr->inode_no;
                                first_dir_entry_ptr->inode_no = entry_cnt;
                                entry.type = file_type::free_dir_entry;
                              }
                              return std::pair<bool, bool>{true, false};
                            }
                            return std::pair<bool, bool>{false, false};
                          });
      if (error_opt) {
        return error_opt;
      }
      // no such file
      if (!p_entry_opt.has_value()) {
        return ERROR_NONEXISTENT_DIR;
      }
      auto p_lock = lock_inode(p_entry_opt.value().inode_no);
      auto [p_inode_ptr, p_inode_block_ref] =
          get_mutable_inode(std::get<0>(p_lock));

      size_t entry_number = 0;
      iterate_dir_entries(*p_inode_ptr, [&](size_t, DirEntry &) {
        entry_number++;
        return std::pair<bool, bool>{false, true};
      });
      if (entry_number > 0) {
        allocate_dir_entry(*parent_inode_ptr, p_entry_opt.value());
        return ERROR_NOT_EMPTY_DIR;
      }
      release_file_metadata(p_entry_opt.value().inode_no);
      return {};
    }

    std::expected<std::pair<block_data_type, INode>, Error>
    read(uint64_t inode_no, uint64_t offset, uint64_t length) {
      std::unique_lock metadata_lock(metadata_mutex);
      if (!is_valid_inode(inode_no)) {
        return std::unexpected(ERROR_INVALID_FD);
      }
      auto inode_lock = shared_lock_inode(inode_no);
      metadata_lock.unlock();
      auto inode = get_inode(inode_no);
      if (inode.type != file_type::file) {
        return std::unexpected(ERROR_PATH_IS_DIR);
      }
      return std::pair<block_data_type, INode>{read_data(inode, offset, length),
                                               inode};
    }

    std::expected<std::pair<uint64_t, INode>, Error>
    write(uint64_t inode_no, uint64_t offset,
          const_block_data_view_type data_view) {
      std::unique_lock metadata_lock(metadata_mutex);
      if (!is_valid_inode(inode_no)) {
        return std::unexpected(ERROR_INVALID_FD);
      }
      auto inode_lock = lock_inode(inode_no);
      metadata_lock.unlock();
      auto [inode_ptr, block_ref] = get_mutable_inode(inode_no);
      if (inode_ptr->type != file_type::file) {
        return std::unexpected(ERROR_PATH_IS_DIR);
      }
      auto written_bytes = write_data(*inode_ptr, offset, data_view);
      return std::pair<uint64_t, INode>{written_bytes, *inode_ptr};
    }

    std::tuple<SuperBlock, size_t, size_t> get_file_system_info() {
      std::unique_lock metadata_lock(metadata_mutex);
      auto const super_block = get_super_block();
      size_t used_inode_number = 0;
      iterate_bytes(super_block.bitmap_byte_offset,
                    super_block.inode_number / 8,
                    [&used_inode_number](auto view, auto) {
                      for (auto const &byte : view) {
                        used_inode_number +=
                            std::popcount(static_cast<unsigned char>(byte));
                      }
                      return std::pair<bool, bool>{false, false};
                    });
      size_t used_data_block_number = 0;
      iterate_bytes(super_block.get_data_bitmap_byte_offset(),
                    super_block.data_block_number / 8,
                    [&used_data_block_number](auto view, auto) {
                      for (auto const &byte : view) {
                        used_data_block_number +=
                            std::popcount(static_cast<unsigned char>(byte));
                      }
                      return std::pair<bool, bool>{false, false};
                    });
      return {super_block, used_inode_number, used_data_block_number};
    }

  private:
    class SyncThread final : public cyy::naive_lib::runnable {
    public:
      SyncThread(RAIDFileSystem *impl_ptr_, std::chrono::seconds cache_time_)
          : impl_ptr(impl_ptr_), cache_time(std::move(cache_time_)) {}
      ~SyncThread() override { stop(); }

    private:
      void run(const std::stop_token &st) override {
        while (!needs_stop()) {
          std::unique_lock lk(impl_ptr->metadata_mutex);
          if (cv.wait_for(lk, st, cache_time,
                          [&st]() { return st.stop_requested(); })) {
            return;
          }
          lk.unlock();
          LOG_DEBUG("flush cache");
          impl_ptr->block_cache.flush();
          lk.lock();
          // release unused mutex
          while (impl_ptr->inode_mutexes.size() > 0 && !needs_stop()) {
            auto [k, v] = impl_ptr->inode_mutexes.pop_oldest();
            // lock the mutex to ensure that we can safely release the mutex
            if (!v->try_lock()) {
              impl_ptr->inode_mutexes.emplace(k, v);
              break;
            }
            LOG_DEBUG("release mutex for {}", k);
            v->unlock();
          }
          LOG_DEBUG("allocated mutex number {}",
                    impl_ptr->inode_mutexes.size());
        }
      }

    private:
      std::condition_variable_any cv;
      RAIDFileSystem *impl_ptr;
      std::chrono::seconds cache_time;
    };

  private:
    const block_ptr_type get_block(uint64_t block_no) {
      auto res = block_cache.get(block_no);
      if (!res.has_value()) {
        throw std::runtime_error(
            fmt::format("failed to read block {}", block_no));
      }
      if (!res.value().get()) {
        LOG_ERROR("invalid block {}", block_no);
        throw std::runtime_error(fmt::format("invalid block {}", block_no));
      }
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
    bool is_valid_inode(uint64_t inode_no) {
      auto const super_block = get_super_block();
      auto offset = super_block.bitmap_byte_offset + inode_no / 8;
      bool res = false;
      iterate_bytes(offset, 1, [&res, inode_no](auto view, auto) {
        auto new_byte = std::byte(view[0]);
        std::byte mask{0b10000000};
        mask >>= (inode_no % 8);
        res = ((mask & new_byte) == mask);
        return std::pair<bool, bool>{false, false};
      });
      return res;
    }
    std::pair<INode *, BlockCache::value_reference>
    get_mutable_inode(uint64_t inode_no) {
      auto const super_block = get_super_block();
      if (inode_no >= super_block.inode_number) {
        throw std::runtime_error(
            fmt::format("invalid inode_no {}, inode_number is {}", inode_no,
                        super_block.inode_number));
      }
      auto inodes_per_block = block_size / sizeof(INode);
      auto block_no =
          super_block.inode_table_offset + inode_no / inodes_per_block;
      auto block_ref = get_mutable_block(block_no);
      auto inode_ptr = reinterpret_cast<INode *>(block_ref->data.data()) +
                       (inode_no % inodes_per_block);
      return {inode_ptr, std::move(block_ref)};
    }

    // initialize file system layout like the mkfs command
    void make_filesystem() {
      {
        LOG_WARN("initialize file system");
        auto block_ref = get_mutable_block(super_block_no);
        auto &blk = block_ref->as_super_block();
        strcpy(blk.fs_type, raid_fs_type);
        blk.fs_version = 0;
        blk.block_size = block_size;
        blk.bitmap_byte_offset = sizeof(SuperBlock);
        blk.next_inode_offset = 0;
        blk.inode_number = static_cast<uint64_t>(block_number * 0.01);
        if (blk.inode_number == 0) {
          blk.inode_number = 1;
        }
        blk.inode_number = (blk.inode_number + 7) / 8 * 8;
        assert(blk.inode_number % 8 == 0 && blk.inode_number > 0);
        auto data_bitmap_byte_offset = blk.get_data_bitmap_byte_offset();
        auto max_recordable_block_number =
            (block_size - data_bitmap_byte_offset % block_size) * 8;
        blk.inode_table_offset = data_bitmap_byte_offset / block_size + 1;
        while (true) {
          blk.data_table_offset = blk.inode_table_offset + blk.inode_number;
          blk.data_block_number = block_number - blk.data_table_offset;
          if (blk.data_block_number > max_recordable_block_number ||
              blk.data_block_number % 8 != 0) {
            /* LOG_DEBUG("{} {}", blk.data_block_number, */
            /*           max_recordable_block_number); */
            blk.inode_table_offset += 1;
            max_recordable_block_number += block_size * 8;
          } else {
            break;
          }
        }
        LOG_WARN("allocate {} inodes and {} data blocks, total {} blocks, "
                 "{} blocks for bookkeeping, effective disk capacity {} GB",
                 blk.inode_number, blk.data_block_number, block_number,
                 block_number - blk.inode_number - blk.data_block_number,
                 block_number * block_size / 1024 / 1024 / 1024);
        // wrapped the above code in local scope such that the super block
        // changes are written to the cache now.

        // preallocate cache of bitmap
        for (uint64_t block_no = super_block_no + 1;
             block_no < blk.inode_table_offset; block_no++) {
          block_cache.emplace(block_no, std::make_shared<Block>());
        }
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
      // zero initialization
      *inode_ptr = INode{};
      inode_ptr->type = type;

      if (type == file_type::directory) {
        DirEntry new_entry{};
        new_entry.type = file_type::free_dir_entry_head;

        auto written_bytes = write_data(
            *inode_ptr, 0,
            const_block_data_view_type(
                reinterpret_cast<const char *>(&new_entry), sizeof(DirEntry)),
            true);
        if (written_bytes == 0) {
          release_inode(inode_no);
          return {};
        }
      }

      return inode_no;
    }

    void release_file_metadata(uint64_t inode_no) {
      auto inode = get_inode(inode_no);
      size_t i = 0;
      while (i < INode::get_direct_block_pointer_number()) {
        auto direct_block_pointer = inode.block_ptrs[i];
        i++;
        if (direct_block_pointer == 0) {
          continue;
        }
        release_data_block(direct_block_pointer);
      }
      auto release_indirect_block = [=, this](uint64_t indirect_block_pointer) {
        auto indirect_block = get_block(indirect_block_pointer);
        for (size_t j = 0; j < block_size / sizeof(uint64_t); j++) {
          auto block_ptr =
              *(reinterpret_cast<uint64_t *>(indirect_block->data.data()) + j);
          if (block_ptr != 0) {
            release_data_block(block_ptr);
          }
        }
        release_data_block(indirect_block_pointer);
      };
      while (i < INode::get_direct_block_pointer_number() +
                     INode::get_indirect_block_pointer_number()) {
        auto indirect_block_pointer = inode.block_ptrs[i];
        i++;
        if (indirect_block_pointer == 0) {
          continue;
        }
        release_indirect_block(indirect_block_pointer);
      }
      while (i < INode::get_direct_block_pointer_number() +
                     INode::get_indirect_block_pointer_number() +
                     INode::get_double_indirect_block_pointer_number()) {
        auto double_indirect_block_pointer = inode.block_ptrs[i];
        i++;
        if (double_indirect_block_pointer == 0) {
          continue;
        }
        auto double_indirect_block = get_block(double_indirect_block_pointer);
        for (size_t j = 0; j < block_size / sizeof(uint64_t); j++) {
          auto indirect_block_pointer = *(
              reinterpret_cast<uint64_t *>(double_indirect_block->data.data()) +
              j);
          if (indirect_block_pointer == 0) {
            continue;
          }
          release_indirect_block(indirect_block_pointer);
        }
        release_data_block(double_indirect_block_pointer);
      }
      release_inode(inode_no);
    }

    std::optional<uint64_t> allocate_inode() {
      const auto blk = get_super_block();
      LOG_DEBUG("inode_number is {}", blk.inode_number);
      auto next_inode_offset = blk.next_inode_offset;
      assert(next_inode_offset < blk.inode_number / 8);
      auto res = allocate_block(blk.bitmap_byte_offset + next_inode_offset,
                                blk.inode_number / 8 - next_inode_offset);
      if (res.has_value()) {
        res.value() += next_inode_offset * 8;
        next_inode_offset++;
        next_inode_offset %= (blk.inode_number / 8);
      } else if (next_inode_offset > 0) {
        res = allocate_block(blk.bitmap_byte_offset, next_inode_offset);
        next_inode_offset = 0;
      }
      if (res.has_value()) {
        auto block_ref = get_mutable_block(super_block_no);
        block_ref->as_super_block().next_inode_offset = next_inode_offset;
      }
      return res;
    }

    void release_inode(uint64_t inode_no) {
      std::unique_lock lk(metadata_mutex);
      const auto blk = get_super_block();
      auto res = release_block(blk.bitmap_byte_offset, inode_no);
      if (!res) {
        throw std::runtime_error(
            fmt::format("failed to release inode {}", inode_no));
      }
    }

    void release_data_block(uint64_t block_no) {
      std::unique_lock lk(metadata_mutex);
      const auto blk = get_super_block();
      if (block_no < blk.data_table_offset) {
        throw std::runtime_error(
            fmt::format("invalid block to release:{}", block_no));
      }
      auto res = release_block(blk.get_data_bitmap_byte_offset(),
                               block_no - blk.data_table_offset);
      if (!res) {
        throw std::runtime_error(
            fmt::format("invalid block to release:{}", block_no));
      }
      block_cache.erase(block_no);
      return;
    }

    uint64_t write_data(INode &inode, uint64_t offset,
                        const_block_data_view_type view,
                        bool check_res = false) {
      if (offset + view.size() > inode.get_max_file_size(block_size)) {
        return 0;
      }
      auto data_length = view.size();

      while (offset > inode.size) {
        auto data_block_ref_opt =
            get_mutable_data_block_of_file(inode, inode.size);
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
        auto data_block_ref_opt = get_mutable_data_block_of_file(inode, offset);
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
      if (check_res && written_bytes != data_length) {
        throw std::runtime_error(fmt::format("invalid write operation {} {}",
                                             written_bytes, data_length));
      }
      return written_bytes;
    }

    block_data_type read_data(const INode &inode, uint64_t offset,
                              uint64_t length, bool check_res = false) {
      block_data_type data;
      LogicalAddressRange address_range(offset, std::min(length, inode.size-offset));
      for (auto [piece_offset, piece_length] :
           address_range.split(block_size)) {
        auto data_block_ptr = get_data_block_of_file(inode, piece_offset);
        data.append(data_block_ptr->data.data() + piece_offset % block_size,
                    piece_length);
      }
      if (check_res && data.size() != length) {
        throw std::runtime_error(
            fmt::format("invalid read operation {} {}", data.size(), length));
      }
      return data;
    }

    std::optional<uint64_t> get_data_block_no_of_file(INode &inode,
                                                      uint64_t offset,
                                                      bool allocate = false) {

      auto allocate_data_block = [=, this](uint64_t &block_ptr) {
        if (!allocate || block_ptr != 0) {
          return block_ptr;
        }
        std::unique_lock lk(metadata_mutex);
        const auto super_block = get_super_block();
        auto relative_data_block_no_opt =
            allocate_block(super_block.get_data_bitmap_byte_offset(),
                           super_block.data_block_number / 8);
        if (relative_data_block_no_opt.has_value()) {
          block_ptr = super_block.data_table_offset +
                      relative_data_block_no_opt.value();
          block_cache.emplace(block_ptr, std::make_shared<Block>());
        }
        return block_ptr;
      };

      const auto direct_extent =
          INode::get_direct_block_pointer_number() * block_size;
      if (offset < direct_extent) {
        auto block_ptr =
            allocate_data_block(inode.block_ptrs[offset / block_size]);
        if (block_ptr == 0) {
          if (!allocate) {
            LOG_ERROR("unallocated block");
          }
          return {};
        }
        return block_ptr;
      }
      offset -= direct_extent;
      const auto number_of_ptr_per_block =
          block_size / sizeof(inode.block_ptrs[0]);

      const auto indirect_block_extent = number_of_ptr_per_block * block_size;
      const auto indirect_extent =
          INode::get_indirect_block_pointer_number() * indirect_block_extent;
      if (offset < indirect_extent) {
        auto indirect_block_ptr = allocate_data_block(
            inode.block_ptrs[INode::get_direct_block_pointer_number() +
                             offset / indirect_block_extent]);
        if (indirect_block_ptr == 0) {
          if (!allocate) {
            LOG_ERROR("unallocated indirect block");
          }
          return {};
        }
        offset %= indirect_block_extent;
        auto indirect_block = get_mutable_block(indirect_block_ptr);
        if (!allocate) {
          indirect_block.cancel_writeback();
        }
        auto block_ptr = allocate_data_block(reinterpret_cast<uint64_t *>(
            indirect_block->data.data())[offset / block_size]);
        if (block_ptr == 0) {
          if (!allocate) {
            LOG_ERROR("unallocated block");
          }
          return {};
        }
        return block_ptr;
      }
      offset -= indirect_extent;
      const auto double_indirect_block_extent =
          number_of_ptr_per_block * number_of_ptr_per_block * block_size;
      const auto double_indirect_extent =
          INode::get_double_indirect_block_pointer_number() *
          double_indirect_block_extent;
      if (offset >= double_indirect_extent) {
        throw std::runtime_error("offset is more than file size limit");
      }
      auto double_indirect_block_ptr = allocate_data_block(
          inode.block_ptrs[INode::get_direct_block_pointer_number() +
                           INode::get_indirect_block_pointer_number() +
                           offset / double_indirect_block_extent]);
      if (double_indirect_block_ptr == 0) {
        if (!allocate) {
          LOG_ERROR("unallocated double indirect block");
        }
        return {};
      }
      offset %= double_indirect_block_extent;
      auto double_indirect_block = get_mutable_block(double_indirect_block_ptr);
      if (!allocate) {
        double_indirect_block.cancel_writeback();
      }
      auto indirect_block_ptr =
          allocate_data_block(reinterpret_cast<uint64_t *>(
              double_indirect_block->data
                  .data())[offset / indirect_block_extent]);
      if (indirect_block_ptr == 0) {
        if (!allocate) {
          LOG_ERROR("unallocated indirect block");
        }
        return {};
      }
      auto indirect_block = get_mutable_block(indirect_block_ptr);
      if (!allocate) {
        indirect_block.cancel_writeback();
      }
      offset %= indirect_block_extent;
      auto block_ptr = allocate_data_block(reinterpret_cast<uint64_t *>(
          indirect_block->data.data())[offset / block_size]);
      if (block_ptr == 0) {
        if (!allocate) {
          LOG_ERROR("unallocated block");
        }
        return {};
      }
      return block_ptr;
    }

    const block_ptr_type get_data_block_of_file(const INode &inode,
                                                uint64_t offset) {
      auto block_no_opt =
          get_data_block_no_of_file(const_cast<INode &>(inode), offset);
      if (!block_no_opt.has_value()) {
        throw std::runtime_error(
            fmt::format("unallocated block for offset {}", offset));
      }
      return get_block(block_no_opt.value());
    }

    std::optional<BlockCache::value_reference>
    get_mutable_data_block_of_file(INode &inode, uint64_t offset) {
      auto block_no_opt =
          get_data_block_no_of_file(const_cast<INode &>(inode), offset, true);
      if (!block_no_opt.value()) {
        return {};
      }
      return get_mutable_block(block_no_opt.value());
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
              new_byte &= (~mask);
              view[0] = std::to_integer<char>(new_byte);
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
              if (static_cast<unsigned char>(view[i]) < 255) {
                uint64_t block_no = (byte_offset + i - bitmap_byte_offset) * 8;
                std::byte mask{0b10000000};
                auto new_byte = std::byte(view[i]);
                for (size_t j = 0; j < 8; j++, block_no++, mask >>= 1) {
                  if ((mask & new_byte) == zero_byte) {
                    new_byte |= mask;
                    view[i] = std::to_integer<char>(new_byte);
                    res = block_no;
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
      LogicalAddressRange address_range(byte_offset, length);
      for (auto [piece_offset, piece_length] :
           address_range.split(block_size)) {
        auto block_no = piece_offset / block_size;
        auto block = get_block(block_no);
        auto [changed, finish] =
            callback(block_data_view_type(
                         &block->data[piece_offset % block_size], piece_length),
                     piece_offset);
        if (changed) {
          block_cache.emplace(block_no, block);
        }
        if (finish) {
          return;
        }
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
          LOG_DEBUG("long component {}",
                    std::string(component.data(), component.size()));
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
    INodeLock lock_inode(uint64_t inode_no) {
      std::unique_lock lk(metadata_mutex);
      if (inode_mutexes.find(inode_no) == inode_mutexes.end()) {
        inode_mutexes.emplace(inode_no, std::make_shared<std::shared_mutex>());
      }
      auto mutex_ptr = inode_mutexes.find(inode_no)->second;
      lk.unlock();
      return std::make_tuple(
          inode_no, std::unique_lock<std::shared_mutex>(*mutex_ptr), mutex_ptr);
    }
    INodeSharedLock shared_lock_inode(uint64_t inode_no) {
      std::unique_lock lk(metadata_mutex);
      if (inode_mutexes.find(inode_no) == inode_mutexes.end()) {
        inode_mutexes.emplace(inode_no, std::make_shared<std::shared_mutex>());
      }
      auto mutex_ptr = inode_mutexes.find(inode_no)->second;
      lk.unlock();
      return std::make_tuple(
          inode_no, std::shared_lock<std::shared_mutex>(*mutex_ptr), mutex_ptr);
    }

    std::expected<INodeLock, Error> travel_path(const std::filesystem::path &p,
                                                bool o_create = false,
                                                bool o_excl = false,
                                                bool path_is_dir = false) {
      if (p == "/") {
        if (!path_is_dir) {
          return std::unexpected(Error::ERROR_PATH_IS_DIR);
        }
        return lock_inode(root_inode_no);
      }
      if (!is_valid_path(p.string())) {
        return std::unexpected(Error::ERROR_INVALID_PATH);
      }

      auto parent_res = travel_path(p.parent_path(), o_create, false, true);
      if (!parent_res.has_value()) {
        return parent_res;
      }
      auto [parent_inode_ptr, parent_inode_block_ref] =
          get_mutable_inode(std::get<0>(parent_res.value()));
      uint64_t p_inode_no = 0;
      Error error = ERROR_UNSPECIFIED;
      auto basename = p.filename();
      iterate_dir_entries(
          *parent_inode_ptr, [&](size_t, const DirEntry &entry) {
            if (basename.compare(entry.name) == 0) {
              if (path_is_dir && entry.type != file_type::directory) {
                error = ERROR_PATH_COMPONENT_IS_FILE;
                return std::pair<bool, bool>{false, true};
              }
              if (!path_is_dir && entry.type != file_type::file) {
                error = ERROR_PATH_IS_DIR;
                return std::pair<bool, bool>{false, true};
              }
              p_inode_no = entry.inode_no;
              return std::pair<bool, bool>{false, true};
            }
            return std::pair<bool, bool>{false, false};
          });
      if (p_inode_no != 0) {
        if (o_create && o_excl && !path_is_dir) {
          return std::unexpected(ERROR_EXISTED_FILE);
        }
        return lock_inode(p_inode_no);
      }
      if (!o_create) {
        if (error == ERROR_UNSPECIFIED) {
          error = ERROR_NONEXISTENT_FILE;
        }
        return std::unexpected(error);
      }
      DirEntry new_entry;
      new_entry.type = path_is_dir ? file_type::directory : file_type::file;
      auto inode_opt = allocate_and_initialize_file_metadata(new_entry.type);
      if (!inode_opt.has_value()) {
        LOG_ERROR("failed to allocate space for {}", p.string());
        return std::unexpected(ERROR_FILE_SYSTEM_FULL);
      }
      strcpy(new_entry.name, p.filename().string().c_str());
      new_entry.inode_no = inode_opt.value();
      std::unique_lock lk(metadata_mutex);
      if (!allocate_dir_entry(*parent_inode_ptr, new_entry)) {
        LOG_DEBUG("failed to allocate space for {}", p.string());
        release_inode(new_entry.inode_no);
        return std::unexpected(ERROR_FILE_SYSTEM_FULL);
      }

      return lock_inode(new_entry.inode_no);
    }

    bool allocate_dir_entry(INode &inode, const DirEntry &new_entry) {
      assert(inode.type == file_type::directory);
      auto first_block = get_mutable_block(inode.block_ptrs[0]);
      DirEntry *first_dir_entry_ptr =
          reinterpret_cast<DirEntry *>(first_block->data.data());
      assert(first_dir_entry_ptr->type == file_type::free_dir_entry_head);
      auto free_dir_entry_no = first_dir_entry_ptr->inode_no;
      if (free_dir_entry_no != 0) {
        assert(inode.size >= free_dir_entry_no * sizeof(DirEntry));
        auto data = read_data(inode, free_dir_entry_no * sizeof(DirEntry),
                              sizeof(DirEntry), true);
        first_dir_entry_ptr->inode_no =
            reinterpret_cast<DirEntry *>(data.data())->inode_no;
      } else {
        free_dir_entry_no = inode.size / sizeof(DirEntry);
      }
      assert(free_dir_entry_no != 0);
      assert(free_dir_entry_no * sizeof(DirEntry) != 0);
      auto written_bytes = write_data(
          inode, free_dir_entry_no * sizeof(DirEntry),
          const_block_data_view_type(reinterpret_cast<const char *>(&new_entry),
                                     sizeof(DirEntry)),
          true);
      if (written_bytes == 0) {
        return false;
      }
      return true;
    }

    void
    iterate_dir_entries(INode &inode,
                        std::function<std::pair<bool, bool>(size_t, DirEntry &)>
                            dir_entry_callback) {
      assert(inode.type == file_type::directory);
      assert(inode.size != 0);
      assert(inode.size % sizeof(DirEntry) == 0);
      uint64_t remain_size = inode.size;
      bool finish = false;
      size_t entry_cnt = 0;
      for (auto block_ptr : inode.block_ptrs) {
        assert(block_ptr != 0);
        uint64_t length = std::min(block_size, remain_size);
        iterate_bytes(
            block_ptr * block_size, length,
            [&finish, &dir_entry_callback, &entry_cnt](
                block_data_view_type view, size_t) -> std::pair<bool, bool> {
              assert(view.size() % sizeof(DirEntry) == 0);
              DirEntry *dir_entry_ptr =
                  reinterpret_cast<DirEntry *>(view.data());
              bool save = false;
              for (size_t i = 0; i < view.size() / sizeof(DirEntry);
                   i++, entry_cnt++) {
                if (dir_entry_ptr[i].type == file_type::free_dir_entry ||
                    dir_entry_ptr[i].type == file_type::free_dir_entry_head) {
                  continue;
                }
                auto res = dir_entry_callback(entry_cnt, dir_entry_ptr[i]);
                if (res.first) {
                  save = true;
                }
                if (res.second) {
                  finish = true;
                }
                if (finish) {
                  return {save, true};
                }
              }
              return {save, false};
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
    std::recursive_mutex metadata_mutex;
    cyy::algorithm::ordered_dict<uint64_t, std::shared_ptr<std::shared_mutex>>
        inode_mutexes;
    SyncThread sync_thread;
    uint64_t root_inode_no{};
  };
} // namespace raid_fs
