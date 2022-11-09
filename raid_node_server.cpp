
#include <fmt/format.h>
#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "config.hpp"
#include "raid.grpc.pb.h"

namespace raid_fs {

  class RAIDNodeServiceImpl final : public raid_fs::RAIDNode::Service {
  public:
    explicit RAIDNodeServiceImpl(size_t disk_capacity_, size_t block_size_)
        : block_size(block_size_) {
      block_number = disk_capacity_ / block_size;
      disk.resize(block_number);
    }

    ~RAIDNodeServiceImpl() override = default;
    ::grpc::Status Read(::grpc::ServerContext *context,
                        const ::raid_fs::BlockReadRequest *request,
                        ::raid_fs::BlockReadReply *response) override {
      if (request->block_no() >= disk.size()) {
        response->set_error(Error::ERROR_OUT_OF_RANGE);
        return ::grpc::Status::OK;
      }
      auto &block = disk[request->block_no()];
      if (block.empty()) {
        std::cout << "use block size" << block_size << std::endl;
        block.resize(block_size, '\0');
      }
      response->mutable_ok()->set_block(block);
      return ::grpc::Status::OK;
    }
    ::grpc::Status Write(::grpc::ServerContext *context,
                         const ::raid_fs::BlockWriteRequest *request,
                         ::raid_fs::BlockWriteReply *response) override {
      if (request->block_no() >= disk.size()) {
        response->set_error(Error::ERROR_OUT_OF_RANGE);
        return ::grpc::Status::OK;
      }
      if (request->block().size() != block_size) {
        response->set_error(Error::ERROR_INVALID_BLOCK);
        return ::grpc::Status::OK;
      }
      disk[request->block_no()] = request->block();
      return ::grpc::Status::OK;
    }

  private:
    std::vector<std::string> disk;
    size_t block_number{};
    size_t block_size;
  }; // namespace raid_fs
} // namespace raid_fs
int main(int argc, char **argv) {
  if (argc <= 1) {
    std::cerr << "Usage:" << argv[0] << " config.yaml" << std::endl;
    return -1;
  }
  raid_fs::RAIDConfig cfg(argv[1]);
  std::vector<std::unique_ptr<grpc::Server>> servers;
  std::vector<std::unique_ptr<raid_fs::RAIDNodeServiceImpl>> services;

  std::cout << cfg.ports.size() << std::endl;
  for (auto port : cfg.ports) {
    std::string server_address(fmt::format("0.0.0.0:{}", port));
    grpc::ServerBuilder builder;
    services.emplace_back(std::make_unique<raid_fs::RAIDNodeServiceImpl>(
        cfg.disk_capacity, cfg.block_size));
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
