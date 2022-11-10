/*!
 * \file fs_server.cpp
 *
 * \brief Implementation of a file system
 */

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
    explicit FileSystemServiceImpl(const FileSystemConfig &fs_cfg,
                                   const RAIDConfig &raid_cfg)
        : raid_controller_ptr(get_RAID_controller(raid_cfg)),
          block_size(fs_cfg.block_size),
          block_number(raid_controller_ptr->get_capacity() / fs_cfg.block_size),
          block_cache(fs_cfg.block_pool_size, fs_cfg.block_size,
                      raid_controller_ptr) {

      // file system is not initialized
      if (std::string(super_block().fs_type) != raid_fs_type) {
        make_filesystem();
      }
    }

  public:
    static constexpr auto raid_fs_type = "RAIDFS";

  private:
    block_ptr_type read_block(size_t block_no) {
      auto it = dirty_blocks.find(block_no);
      if (it != dirty_blocks.end()) {
        return it->second;
      }
      auto res = block_cache.get(block_no);
      if (!res.has_value()) {
        throw std::runtime_error(
            fmt::format("failed to read block {}", block_no));
      }
      return res.value();
    }
    // initialize file system layout like the mkfs command
    void make_filesystem() {
      LOG_WARN("initialize file system");
      auto &blk = modificable_super_block();
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
    SuperBlock &modificable_super_block() {
      auto ptr = read_block(super_block_no);
      mark_dirty(super_block_no, ptr);
      return ptr->as_super_block();
    }
    const SuperBlock &super_block() {
      return read_block(super_block_no)->as_super_block();
    }
    void mark_dirty(uint64_t block_no, const block_ptr_type &block_ptr) {
      block_ptr->dirty = true;
      dirty_blocks[block_no] = block_ptr;
    }

  private:
    std::shared_ptr<RAIDController> raid_controller_ptr;
    std::unordered_map<uint64_t, block_ptr_type> dirty_blocks;
    static constexpr uint64_t super_block_no = 0;
    size_t block_size;
    size_t block_number;
    BlockCache block_cache;
    block_ptr_type super_block_ptr{};
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

  raid_fs::FileSystemServiceImpl service(cfg, raid_cfg);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
  return 0;
}
