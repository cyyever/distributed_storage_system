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
      if (super_block().FS_type != raid_fs_type) {
        make_filesystem();
      }
    }

  public:
    static constexpr auto raid_fs_type = "RAIDFS";

  private:
    block_ptr_type read_block(size_t block_no) {
      auto res = block_cache.get(block_no);
      if (!res.has_value()) {
        throw std::runtime_error(
            fmt::format("failed to read block {}", block_no));
      }
      return res.value();
    }
    // initialize file system layout like the mkfs command
    void make_filesystem() { LOG_WARN("initialize file system"); }
    SuperBlock &super_block() {
      constexpr uint64_t super_block_no = 0;
      if (!super_block_ptr) {
        super_block_ptr = read_block(super_block_no);
      }
      return super_block_ptr->as_super_block();

      /* super_block = {.FS_type = "RAIDFS", */
      /*              .FS_version = 0, */
      /*              .bitmap_offset = 0, */
      /*              .inode_bitmap_size = 0, */
      /*              .data_bitmap_size = 0, */
      /*              .inode_table_offset = 0, */
      /*              .inode_number = 0, */
      /*              .data_table_offset = 0, */
      /*              .data_block_number = 0}; */
    }

  private:
    std::shared_ptr<RAIDController> raid_controller_ptr;
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
