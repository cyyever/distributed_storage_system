/*!
 * \file raid_controller.hpp
 *
 * \brief
 */
#pragma once

#include <optional>
#include <variant>

#include <fmt/format.h>
#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "config.hpp"
#include "error.pb.h"
#include "raid.grpc.pb.h"
namespace raid_fs {
  class RAIDController {
  public:
    virtual ~RAIDController() = default;
    virtual std::variant<Error, std::string> read_block(size_t block_no) = 0;
    virtual std::optional<Error> write_block(size_t block_no,
                                             std::string &&block) = 0;
  };
  class RAID6Controller : public RAIDController {
  public:
    RAID6Controller(const RAIDConfig &raid_config) {
      for (auto port : raid_config.ports) {
        auto channel =
            grpc::CreateChannel(fmt::format("localhost:{}", port),
                                ::grpc::InsecureChannelCredentials());
        stubs.emplace_back(RAIDNode::NewStub(channel));
      }
    }
    ~RAID6Controller() override = default;
    /* virtual std::variant<Error, std::string> read_block(size_t block_no) = 0;
     */
    /* virtual std::optional<Error> write_block(size_t block_no, */
    /*                                          std::string &&block) = 0; */
    std::vector<std::unique_ptr<RAIDNode::Stub>> stubs;
  };
} // namespace raid_fs
