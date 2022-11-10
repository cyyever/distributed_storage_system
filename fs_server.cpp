/*!
 * \file fs_server.cpp
 *
 * \brief Implementation of a file system
 */

#include <shared_mutex>

#include <cyy/naive_lib/util/runnable.hpp>
#include <fmt/format.h>
#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "block.hpp"
#include "block_cache.hpp"
#include "config.hpp"
#include "fs.grpc.pb.h"
#include "raid_controller.hpp"

namespace raid_fs {

  class FileSystemServiceImpl final : public raid_fs::FileSystem::Service {
  public:
    FileSystemServiceImpl(
        const FileSystemConfig &fs_cfg,
        const std::shared_ptr<RAIDController> &raid_controller_ptr)
        : block_size(fs_cfg.block_size),
          block_number(raid_controller_ptr->get_capacity() / fs_cfg.block_size),
          block_cache(fs_cfg.block_pool_size, fs_cfg.block_size,
                      raid_controller_ptr),
          sync_thread(this) {

      // file system is not initialized
      if (std::string(super_block().fs_type) != raid_fs_type) {
        make_filesystem();
      }
      sync_thread.start("sync thread");
    }

    ~FileSystemServiceImpl() { sync_thread.stop(); }

  private:
    class SyncThread final : public cyy::naive_lib::runnable {
    public:
      SyncThread(FileSystemServiceImpl *impl_ptr_) : impl_ptr(impl_ptr_) {}
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
      FileSystemServiceImpl *impl_ptr;
    };
    class BlockReference final {
    public:
      BlockReference(uint64_t block_no_, block_ptr_type block_ptr_,
                     BlockCache &cache_)
          : block_no(block_no_), block_ptr{block_ptr_}, cache{cache_} {}

      BlockReference(const BlockReference &) = delete;
      BlockReference &operator=(const BlockReference &) = delete;

      BlockReference(BlockReference &&) noexcept = delete;
      BlockReference &operator=(BlockReference &&) noexcept = delete;

      ~BlockReference() {
        block_ptr->dirty = true;
        cache.emplace(block_no, block_ptr);
      }
      Block *operator->() const { return block_ptr.get(); }

    private:
      uint64_t block_no;
      block_ptr_type block_ptr;
      BlockCache &cache;
    };

  private:
    const block_ptr_type get_block(uint64_t block_no) {
      auto res = block_cache.get(block_no);
      if (!res.has_value()) {
        throw std::runtime_error(
            fmt::format("failed to read block {}", block_no));
      }
      return res.value();
    }
    BlockReference get_modifiable_block(uint64_t block_no) {
      auto res = block_cache.get(block_no);
      if (!res.has_value()) {
        throw std::runtime_error(
            fmt::format("failed to read block {}", block_no));
      }
      return {block_no, res.value(), block_cache};
    }
    // initialize file system layout like the mkfs command
    void make_filesystem() {
      LOG_WARN("initialize file system");
      auto block_ref = get_modifiable_block(super_block_no);
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
        if (blk.data_block_number > max_recordable_block_number) {
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
    }
    const SuperBlock &super_block() {
      return get_block(super_block_no)->as_super_block();
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
  };
} // namespace raid_fs

int main(int argc, char **argv) {
  if (argc <= 1) {
    std::cerr << "Usage:" << argv[0] << " config.yaml" << std::endl;
    return -1;
  }
  raid_fs::FileSystemConfig cfg(argv[1]);
  raid_fs::RAIDConfig raid_cfg(argv[1]);
  raid_fs::Block::block_size = cfg.block_size;
  std::string server_address(fmt::format("0.0.0.0:{}", cfg.port));

  raid_fs::FileSystemServiceImpl service(cfg, get_RAID_controller(raid_cfg));

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
  return 0;
}
