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
    // data offset and length
    using LogicalRange = std::pair<uint64_t, uint64_t>;
    virtual ~RAIDController() = default;
    virtual size_t get_capacity() = 0;
    virtual std::expected<std::map<LogicalRange, std::string>, Error>
    read(const std::set<LogicalRange> &data_ranges) = 0;
    virtual std::optional<Error>
    write(std::map<uint64_t, std::string> blocks) = 0;
  };
  class RAID6Controller : public RAIDController {
  public:
    RAID6Controller(const RAIDConfig &raid_config) {
      data_node_number = raid_config.data_ports.size();
      capacity = raid_config.disk_capacity * data_node_number;
      block_size = raid_config.block_size;
      for (auto port : raid_config.data_ports) {
        auto channel =
            grpc::CreateChannel(fmt::format("localhost:{}", port),
                                ::grpc::InsecureChannelCredentials());
        data_stubs.emplace_back(RAIDNode::NewStub(channel));
      }
      auto channel = grpc::CreateChannel(
          fmt::format("localhost:{}", raid_config.parity_ports[0]),
          ::grpc::InsecureChannelCredentials());
      P_stub = RAIDNode::NewStub(channel);
      P_stub = RAIDNode::NewStub(channel);
    }
    ~RAID6Controller() override = default;
    size_t get_capacity() override { return capacity; }

    std::expected<std::map<LogicalRange, std::string>, Error>
    read(const std::set<LogicalRange> &data_ranges) override {
      auto block_no_set = convert_logical_range_to_raid_blocks(data_ranges);
      std::map<uint64_t, std::string> raid_blocks;

      for (auto block_no : block_no_set) {
        auto physical_node_no = block_no % data_node_number;
        auto physical_block_no = block_no / data_node_number;
        ::grpc::ClientContext context;
        BlockReadRequest request;
        request.set_block_no(physical_block_no);
        BlockReadReply reply;

        auto grpc_status =
            data_stubs[physical_node_no]->Read(&context, request, &reply);
        if (!grpc_status.ok()) {
          LOG_ERROR("read block {} failed:{}", block_no,
                    grpc_status.error_message());
          return std::unexpected(Error::ERROR_FS_INTERNAL_ERROR);
        }
        if (reply.has_error()) {
          return std::unexpected(reply.error());
        }
        assert(reply.has_ok());
        raid_blocks[block_no] = reply.ok().block();
      }
      std::map<LogicalRange, std::string> results;
      for (auto const &range : data_ranges) {
        auto [offset, length] = range;
        uint64_t p = offset;
        auto end_pos = offset + length;
        auto partial_length = std::min(block_size, block_size - p % block_size);
        std::string result(raid_blocks[p / block_size], partial_length);
        p += partial_length;
        length -= partial_length;
        while (p < end_pos) {
          partial_length = std::min(length, block_size - p % block_size);
          result.append(raid_blocks[p / block_size], partial_length);
          p += block_size;
          length -= partial_length;
        }
        results.emplace(range, std::move(result));
      }
      return results;
    }
    std::optional<Error> write(std::map<uint64_t, std::string> blocks) override {
#if 0
      for (auto &[block_no, block] : blocks) {
        ::grpc::ClientContext context;
        BlockWriteRequest request;
        request.set_block_no(block_no);
        request.set_block(std::move(block));
        BlockWriteReply reply;

        auto grpc_status = data_stubs[0]->Write(&context, request, &reply);
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
#endif
      return {};
    }

  private:
    std::set<uint64_t> convert_logical_range_to_raid_blocks(
        const std::set<LogicalRange> &data_ranges) {
      std::set<uint64_t> raid_block_no_set;
      for (auto const &[offset, length] : data_ranges) {
        for (uint64_t p = offset; p < offset + length; p += block_size) {
          raid_block_no_set.emplace(p / block_size);
        }
      }
      return raid_block_no_set;
    }

  private:
    std::vector<std::unique_ptr<RAIDNode::Stub>> data_stubs;
    std::unique_ptr<RAIDNode::Stub> P_stub;
    std::unique_ptr<RAIDNode::Stub> Q_stub;
    size_t data_node_number{};
    size_t capacity{};
    size_t block_size{};
  };

  inline std::shared_ptr<RAIDController>
  get_RAID_controller(const RAIDConfig &raid_config) {
    return std::make_shared<RAID6Controller>(raid_config);
  }
} // namespace raid_fs
