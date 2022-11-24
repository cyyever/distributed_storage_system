/*!
 * \file fs_server.cpp
 *
 * \brief Implementation of a file system
 */

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "config.hpp"
#include "fs.grpc.pb.h"
#include "raid_file_system.hpp"

namespace raid_fs {

  class FileSystemServiceImpl final : public raid_fs::FileSystem::Service {
  public:
    FileSystemServiceImpl(
        const FileSystemConfig &fs_cfg,
        const std::shared_ptr<RAIDController> &raid_controller_ptr)
        : raid_fs(fs_cfg, raid_controller_ptr) {}

    ~FileSystemServiceImpl() override = default;
    ::grpc::Status Open(::grpc::ServerContext *,
                        const ::raid_fs::OpenRequest *request,
                        ::raid_fs::OpenReply *response) override {
      auto inode_or_error =
          raid_fs.open(request->path(), request->o_create(), request->o_excl());
      if (!inode_or_error.has_value()) {
        response->set_error(inode_or_error.error());
      } else {
        auto [inode_no, inode] = inode_or_error.value();
        response->mutable_ok()->set_fd(inode_no);
        response->mutable_ok()->set_file_size(inode.size);
      }
      return ::grpc::Status::OK;
    }
    ::grpc::Status Read(::grpc::ServerContext *,
                        const ::raid_fs::ReadRequest *request,
                        ::raid_fs::ReadReply *response) override {
      auto res_or_error =
          raid_fs.read(request->fd(), VirtualAddressRange(request->offset(),
                                                          request->count()));
      if (!res_or_error.has_value()) {
        response->set_error(res_or_error.error());
      } else {
        response->mutable_ok()->set_data(std::move(res_or_error.value().first));
        response->mutable_ok()->set_file_size(res_or_error.value().second.size);
      }
      return ::grpc::Status::OK;
    }
    ::grpc::Status Write(::grpc::ServerContext *,
                         const ::raid_fs::WriteRequest *request,
                         ::raid_fs::WriteReply *response) override {
      auto res_or_error =
          raid_fs.write(request->fd(), request->offset(), request->data());
      if (!res_or_error.has_value()) {
        response->set_error(res_or_error.error());
      } else {
        response->mutable_ok()->set_written_size(res_or_error.value().first);
        response->mutable_ok()->set_file_size(res_or_error.value().second.size);
      }
      return ::grpc::Status::OK;
    }
    ::grpc::Status Remove(::grpc::ServerContext *,
                          const ::raid_fs::RemoveRequest *request,
                          ::raid_fs::RemoveReply *response) override {
      auto error_opt = raid_fs.remove_file(request->path());
      if (error_opt.has_value()) {
        response->set_error(error_opt.value());
      }
      return ::grpc::Status::OK;
    }
    ::grpc::Status RemoveDir(::grpc::ServerContext *,
                             const ::raid_fs::RemoveDirRequest *request,
                             ::raid_fs::RemoveDirReply *response) override {
      auto error_opt = raid_fs.remove_dir(request->path());
      if (error_opt.has_value()) {
        response->set_error(error_opt.value());
      }
      return ::grpc::Status::OK;
    }
    ::grpc::Status
    GetFileSystemInfo(::grpc::ServerContext *context,
                      const ::google::protobuf::Empty *request,
                      ::raid_fs::FileSystemInfoReply *response) override {
      auto [super_block, used_inode_number, used_data_block_number] =
          raid_fs.get_file_system_info();
      response->mutable_ok()->set_file_system_type(super_block.fs_type);
      response->mutable_ok()->set_file_system_version(super_block.fs_version);
      response->mutable_ok()->set_block_size(super_block.block_size);
      response->mutable_ok()->set_file_number(used_inode_number);
      response->mutable_ok()->set_used_data_block_number(
          used_data_block_number);
      response->mutable_ok()->set_free_data_block_number(
          super_block.data_block_number - used_data_block_number);
      return ::grpc::Status::OK;
    }

  private:
    RAIDFileSystem raid_fs;
  };
} // namespace raid_fs

int main(int argc, char **argv) {
  if (argc <= 1) {
    std::cerr << "Usage:" << argv[0] << " config.yaml" << std::endl;
    return -1;
  }
  raid_fs::FileSystemConfig cfg(argv[1]);
  raid_fs::RAIDConfig raid_cfg(argv[1]);
  if (cfg.block_size % raid_cfg.block_size != 0) {
    LOG_ERROR(
        "file system block size {} must be a multiple of RAID block size {}",
        cfg.block_size, raid_cfg.block_size);
    return -1;
  }
  if (cfg.debug_log) {
    cyy::naive_lib::log::set_level(spdlog::level::level_enum::debug);
  }
  std::string server_address(fmt::format("0.0.0.0:{}", cfg.port));

  raid_fs::FileSystemServiceImpl service(cfg, get_RAID_controller(raid_cfg));

  grpc::ServerBuilder builder;
  builder.SetMaxMessageSize(std::numeric_limits<int>::max());
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LOG_INFO("Server listening on {}", server_address);
  server->Wait();
  return 0;
}
