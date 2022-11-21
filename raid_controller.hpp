/*!
 * \file raid_controller.hpp
 *
 * \brief
 */
#pragma once

#include <algorithm>
#include <cassert>
#include <expected>
#include <map>
#include <optional>
#include <ranges>
#include <set>

#include <cyy/naive_lib/log/log.hpp>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <spdlog/fmt/fmt.h>

#include "block.hpp"
#include "config.hpp"
#include "raid.grpc.pb.h"
namespace raid_fs {
  class RAIDController {
  public:
    // data offset and length
    virtual ~RAIDController() = default;
    virtual size_t get_capacity() = 0;
    virtual std::map<LogicalAddressRange, byte_stream_type>
    read(const std::set<LogicalAddressRange> &data_ranges) = 0;
    virtual std::set<uint64_t>
    write(std::map<uint64_t,byte_stream_type> blocks) = 0;
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
      Q_stub = RAIDNode::NewStub(channel);
    }
    ~RAID6Controller() override = default;
    size_t get_capacity() override { return capacity; }

    std::map<LogicalAddressRange, byte_stream_type>
    read(const std::set<LogicalAddressRange> &data_ranges) override {
      auto raid_blocks = concurrent_read(get_raid_block_no(data_ranges));
      std::map<LogicalAddressRange,byte_stream_type> results;
      for (auto const &range : data_ranges) {
        bool has_raid_block = true;
        for (auto block_no : get_raid_block_no({range})) {
          if (!raid_blocks.contains(block_no)) {
            has_raid_block = false;
            break;
          }
        }

        if (!has_raid_block) {
          continue;
        }
          byte_stream_type result;
        result.reserve(range.length);
        for (auto [offset, length] : range.split(block_size)) {
          result.append(raid_blocks[offset / block_size].data() +
                            offset % block_size,
                        length);
        }
        results.emplace(range, std::move(result));
      }
      return results;
    }
    std::set<uint64_t> write(std::map<uint64_t,byte_stream_type> blocks) override {
      std::map<uint64_t, std::string_view> raid_blocks;
      for (auto const &[offset, block] : blocks) {
        assert(offset % block_size == 0);
        assert(!block.empty());
        assert(block.size() % block_size == 0);
        for (size_t p = offset; p < offset + block.size(); p += block_size) {
          raid_blocks.emplace(
              p / block_size,
              std::string_view{block.data() + p - offset, block_size});
        }
      }
      auto raid_res = concurrent_write(raid_blocks);
      std::set<uint64_t> block_result;
      for (auto const &[data_offset, block] : blocks) {
        bool write_succ = std::ranges::all_of(
            LogicalAddressRange(data_offset, block.size()).split(block_size),
            [&](const auto &range) {
              return raid_res.contains(range.offset / block_size);
            });
        if (write_succ) {
          block_result.insert(data_offset);
        }
      }
      return block_result;
    }

  private:
    std::set<uint64_t>
    get_raid_block_no(const std::set<LogicalAddressRange> &data_ranges) {
      std::set<uint64_t> raid_block_no_set;
      for (auto const &range : data_ranges) {
        for (auto sub_range : range.split(block_size)) {
          raid_block_no_set.emplace(sub_range.offset / block_size);
        }
      }
      return raid_block_no_set;
    }
    std::map<uint64_t,byte_stream_type>
    concurrent_read(std::set<uint64_t> block_no_set) {
      std::map<uint64_t,byte_stream_type> raid_blocks;
      grpc::CompletionQueue cq;
      while (!block_no_set.empty()) {
        std::set<uint64_t> new_block_no_set;
        std::map<uint64_t,
                 std::tuple<std::unique_ptr<grpc::ClientAsyncResponseReader<
                                BlockReadReply>>,
                            BlockReadReply, ::grpc::Status, uint64_t>>
            reply_map;
        std::map<uint64_t, ::grpc::ClientContext> contexts;

        for (auto block_no : block_no_set) {
          auto physical_node_no = block_no % data_node_number;
          if (reply_map.contains(physical_node_no)) {
            new_block_no_set.insert(block_no);
            continue;
          }
          auto physical_block_no = block_no / data_node_number;
          BlockReadRequest request;
          request.set_block_no(physical_block_no);

          std::get<0>(reply_map[physical_node_no]) =
              data_stubs[physical_node_no]->AsyncRead(
                  &contexts[physical_node_no], request, &cq);
          std::get<3>(reply_map[physical_node_no]) = block_no;
          std::get<0>(reply_map[physical_node_no])
              ->Finish(&std::get<1>(reply_map[physical_node_no]),
                       &std::get<2>(reply_map[physical_node_no]),
                       (void *)physical_node_no);
        }
        while (!reply_map.empty()) {
          void *got_tag = nullptr;
          bool ok = false;
          if (!cq.Next(&got_tag, &ok) || !ok) {
            throw std::runtime_error("cg next failed");
          }
          auto physical_node_no = reinterpret_cast<uint64_t>(got_tag);

          auto node = reply_map.extract(physical_node_no);
          if (node.empty()) {
            throw std::runtime_error(
                fmt::format("invalid grpc tag {}", got_tag));
          }
          auto &[_, reply, grpc_status, block_no] = node.mapped();

          if (!grpc_status.ok()) {
            LOG_ERROR("read block {} failed:{}", block_no,
                      grpc_status.error_message());
            continue;
          }
          if (reply.has_error()) {
            LOG_ERROR("read block {} failed:{}", block_no, reply.error());
            continue;
          }
          raid_blocks[block_no] = reply.ok().block();
        }
        block_no_set = std::move(new_block_no_set);
      }

      return raid_blocks;
    }

    std::set<uint64_t>
    concurrent_write(std::map<uint64_t, std::string_view> raid_blocks) {
      std::set<uint64_t> raid_results;
      grpc::CompletionQueue cq;
      while (!raid_blocks.empty()) {
        decltype(raid_blocks) new_raid_blocks;
        std::map<uint64_t,
                 std::tuple<std::unique_ptr<grpc::ClientAsyncResponseReader<
                                BlockWriteReply>>,
                            BlockWriteReply, ::grpc::Status, uint64_t>>
            reply_map;
        std::map<uint64_t, ::grpc::ClientContext> contexts;

        for (auto &[block_no, raid_block] : raid_blocks) {
          auto physical_node_no = block_no % data_node_number;
          if (reply_map.contains(physical_node_no)) {
            new_raid_blocks.emplace(block_no, raid_block);
            continue;
          }
          auto physical_block_no = block_no / data_node_number;
          BlockWriteRequest request;
          request.set_block_no(physical_block_no);
          request.set_block(byte_stream_type(raid_block));

          std::get<0>(reply_map[physical_node_no]) =
              data_stubs[physical_node_no]->AsyncWrite(
                  &contexts[physical_node_no], request, &cq);
          std::get<3>(reply_map[physical_node_no]) = block_no;
          std::get<0>(reply_map[physical_node_no])
              ->Finish(&std::get<1>(reply_map[physical_node_no]),
                       &std::get<2>(reply_map[physical_node_no]),
                       (void *)physical_node_no);
        }
        while (!reply_map.empty()) {
          void *got_tag = nullptr;
          bool ok = false;
          if (!cq.Next(&got_tag, &ok) || !ok) {
            throw std::runtime_error("cg next failed");
          }
          auto physical_node_no = reinterpret_cast<uint64_t>(got_tag);

          auto node = reply_map.extract(physical_node_no);
          if (node.empty()) {
            throw std::runtime_error(
                fmt::format("invalid grpc tag {}", got_tag));
          }
          auto &[_, reply, grpc_status, block_no] = node.mapped();

          if (!grpc_status.ok()) {
            LOG_ERROR("write block {} failed:{}", block_no,
                      grpc_status.error_message());
            continue;
          }
          if (reply.has_error()) {
            LOG_ERROR("write block {} failed:{}", block_no, reply.error());
            continue;
          }
          raid_results.insert(block_no);
        }
        raid_blocks = std::move(new_raid_blocks);
      }
      return raid_results;
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
