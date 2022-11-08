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
#include "config.hpp"
#include "raid_controller.hpp"
#include "fs.grpc.pb.h"

namespace raid_fs {

  class FileSystemServiceImpl final : public raid_fs::FileSystem::Service {
  public:
    explicit FileSystemServiceImpl(size_t disk_capacity_, size_t block_size_)
        : disk_capacity(disk_capacity_), block_size(block_size_) {
      if (!read_super_block()) {
        throw std::runtime_error("failed to read super block");
      }
      // file system is not initialized
      if (super_blk.FS_type != raid_fs_type) {
      }
    }

  public:
    static constexpr auto raid_fs_type = "RAIDFS";

  private:
    bool read_super_block() {
      super_blk = {.FS_type = "RAIDFS",
                   .FS_version = 0,
                   .bitmap_offset = 0,
                   .inode_bitmap_size = 0,
                   .data_bitmap_size = 0,
                   .inode_table_offset = 0,
                   .inode_number = 0,
                   .data_table_offset = 0,
                   .data_block_number = 0};
      return true;
    }

  private:
    size_t disk_capacity;
    size_t block_size;
    SuperBlock super_blk{};
  };
} // namespace raid_fs
int main(int argc, char **argv) {
  if (argc <= 1) {
    std::cerr << "Usage:" << argv[0] << " config.yaml" << std::endl;
    return -1;
  }
  raid_fs::FileSystemConfig cfg(argv[1]);
  raid_fs::Block::block_size = cfg.block_size;
  std::string server_address(fmt::format("0.0.0.0:{}", cfg.port));

  raid_fs::FileSystemServiceImpl service(cfg.disk_capacity, cfg.block_size);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
  return 0;
}
