

#include <cyy/naive_lib/log/log.hpp>
#include <cyy/naive_lib/util/error.hpp>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <spdlog/fmt/fmt.h>

#include "config.hpp"
#include "disk.hpp"
#include "raid.grpc.pb.h"

namespace raid_fs {

  class RAIDNodeServiceImpl final : public raid_fs::RAIDNode::Service {
  public:
    explicit RAIDNodeServiceImpl(const RAIDConfig &config, size_t service_id)
        : disk_ptr(get_disk(config, service_id)) {}

    ~RAIDNodeServiceImpl() override = default;
    ::grpc::Status Read(::grpc::ServerContext *context,
                        const ::raid_fs::BlockReadRequest *request,
                        ::raid_fs::BlockReadReply *response) override {
      if (request->block_no() >= disk_ptr->get_block_number()) {
        LOG_ERROR("invalid block no {} {}", request->block_no(),
                  disk_ptr->get_block_number());
        response->set_error(Error::ERROR_FS_INTERNAL_ERROR);
        return ::grpc::Status::OK;
      }
      auto res = disk_ptr->read(request->block_no());
      if (res.has_value()) {
        response->mutable_ok()->set_block(std::move(res.value()));
        return ::grpc::Status::OK;
      }
      LOG_ERROR("read block failed:{}",
                ::cyy::naive_lib::util::errno_to_str(res.error()));
      response->set_error(Error::ERROR_FS_INTERNAL_ERROR);
      return ::grpc::Status::OK;
    }
    ::grpc::Status Write(::grpc::ServerContext *context,
                         const ::raid_fs::BlockWriteRequest *request,
                         ::raid_fs::BlockWriteReply *response) override {
      if (request->block_no() >= disk_ptr->get_block_number()) {
        LOG_ERROR("invalid block no {} {}", request->block_no(),
                  disk_ptr->get_block_number());
        response->set_error(Error::ERROR_FS_INTERNAL_ERROR);
        return ::grpc::Status::OK;
      }
      if (request->block().size() != disk_ptr->get_block_size()) {
        LOG_ERROR("block size mismatch:{} {}", request->block().size(),
                  disk_ptr->get_block_size());
        response->set_error(Error::ERROR_FS_INTERNAL_ERROR);
        return ::grpc::Status::OK;
      }
      auto res = disk_ptr->write(request->block_no(), request->block());
      if (res.has_value()) {
        LOG_ERROR("write block failed:{}",
                  ::cyy::naive_lib::util::errno_to_str(res.value()));
        response->set_error(Error::ERROR_FS_INTERNAL_ERROR);
      }
      return ::grpc::Status::OK;
    }

  private:
    std::shared_ptr<VirtualDisk> disk_ptr;
  };
} // namespace raid_fs
int main(int argc, char **argv) {
  if (argc <= 1) {
    std::cerr << "Usage:" << argv[0] << " config.yaml" << std::endl;
    return -1;
  }
  raid_fs::RAIDConfig cfg(argv[1]);
  std::vector<std::unique_ptr<grpc::Server>> servers;
  std::vector<std::unique_ptr<raid_fs::RAIDNodeServiceImpl>> services;

  size_t service_id = 0;
  auto ports = cfg.data_ports;
  ports.insert(ports.end(), cfg.parity_ports.begin(), cfg.parity_ports.end());
  for (auto port : ports) {
    std::string server_address(fmt::format("0.0.0.0:{}", port));
    grpc::ServerBuilder builder;
    services.emplace_back(
        std::make_unique<raid_fs::RAIDNodeServiceImpl>(cfg, service_id));
    service_id++;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(services.back().get());
    servers.emplace_back(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
  }
  for (const auto &server : servers) {
    server->Wait();
  }
  std::cout << "wait end" << std::endl;
  return 0;
}
