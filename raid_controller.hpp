/*!
 * \file raid_controller.hpp
 *
 * \brief
 */
#pragma once

#include <cassert>
#include <expected>
#include <map>
#include <optional>
#include <set>

#include <cyy/naive_lib/log/log.hpp>
#include <fmt/format.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include "config.hpp"
#include "error.pb.h"
#include "raid.grpc.pb.h"
namespace raid_fs {
  class RAIDController {
  public:
    virtual ~RAIDController() = default;
    virtual size_t get_block_size() = 0;
    virtual size_t get_capacity() = 0;
    virtual std::expected<std::map<uint64_t, std::string>, Error>
    read_blocks(const std::set<uint64_t> &block_no_set) = 0;
    virtual std::optional<Error>
    write_blocks(std::map<uint64_t, std::string> blocks) = 0;
  };
  class RAID6Controller : public RAIDController {
  public:
    RAID6Controller(const RAIDConfig &raid_config) {
      block_number = raid_config.disk_capacity / raid_config.block_size;
      block_size = raid_config.block_size;
      for (auto port : raid_config.ports) {
        auto channel =
            grpc::CreateChannel(fmt::format("localhost:{}", port),
                                ::grpc::InsecureChannelCredentials());
        stubs.emplace_back(RAIDNode::NewStub(channel));
      }
    }
    ~RAID6Controller() override = default;
    size_t get_capacity() override { return block_size * block_number; }
    size_t get_block_size() override { return block_size; }

    std::expected<std::map<uint64_t, std::string>, Error>
    read_blocks(const std::set<uint64_t> &block_no_set) override {
      std::map<uint64_t, std::string> blocks;
      for (auto block_no : block_no_set) {

        ::grpc::ClientContext context;
        BlockReadRequest request;
        request.set_block_no(block_no);
        BlockReadReply reply;

        auto grpc_status = stubs[0]->Read(&context, request, &reply);
        if (!grpc_status.ok()) {
          LOG_ERROR("read block {} failed:{}", block_no,
                    grpc_status.error_message());
          return std::unexpected(Error::ERROR_FS_INTERNAL_ERROR);
        }
        if (reply.has_error()) {
          return std::unexpected(reply.error());
        }
        assert(reply.has_ok());
        blocks[block_no] = reply.ok().block();
      }
      return blocks;
    }
    std::optional<Error>
    write_blocks(std::map<uint64_t, std::string> blocks) override {
      for (auto &[block_no, block] : blocks) {
        ::grpc::ClientContext context;
        BlockWriteRequest request;
        request.set_block_no(block_no);
        request.set_block(std::move(block));
        BlockWriteReply reply;

        auto grpc_status = stubs[0]->Write(&context, request, &reply);
        if (!grpc_status.ok()) {
          LOG_ERROR("write block {} failed:{}", block_no,
                    grpc_status.error_message());
          return {Error::ERROR_FS_INTERNAL_ERROR};
        }
        if (reply.has_error()) {
          LOG_ERROR("write block {} failed:{}", block_no, reply.error());
          return {reply.error()};
        }
      }
      return {};
    }

  private:
    std::vector<std::unique_ptr<RAIDNode::Stub>> stubs;
    size_t block_number{};
    size_t block_size{};
  };

  inline std::shared_ptr<RAIDController>
  get_RAID_controller(const RAIDConfig &raid_config) {
    return std::make_shared<RAID6Controller>(raid_config);
  }
} // namespace raid_fs
