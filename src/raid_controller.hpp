/*!
 * \file raid_controller.hpp
 *
 * \brief
 */
#pragma once

#include <algorithm>
#include <cassert>
#include <map>
#include <mutex>
#include <ranges>
#include <set>
#include <utility>

#include <cyy/naive_lib/log/log.hpp>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <spdlog/fmt/fmt.h>

#include "block.hpp"
#include "config.hpp"
#include "galois_field.hpp"
#include "raid.grpc.pb.h"

namespace raid_fs {
  class RAIDController {
  public:
    virtual ~RAIDController() = default;
    virtual size_t get_capacity() = 0;
    virtual std::map<LogicalAddressRange, byte_stream_type>
    read(const std::set<LogicalAddressRange> &data_ranges) = 0;
    virtual std::set<uint64_t>
    write(std::map<uint64_t, byte_stream_type> blocks) = 0;

  protected:
    static std::map<uint64_t, byte_stream_type> parallel_read_blocks(
        const std::vector<std::unique_ptr<RAIDNode::Stub>> &stubs,
        const std::map<uint64_t, uint64_t> &block_locations) {
      std::map<uint64_t, byte_stream_type> raid_blocks;
      grpc::CompletionQueue cq;
      std::map<
          uint64_t,
          std::tuple<
              std::unique_ptr<grpc::ClientAsyncResponseReader<BlockReadReply>>,
              BlockReadReply, ::grpc::Status, ::grpc::ClientContext>>
          reply_map;

      for (auto [physical_node_no, physical_block_no] : block_locations) {
        BlockReadRequest request;
        request.set_block_no(physical_block_no);

        std::get<0>(reply_map[physical_node_no]) =
            stubs[physical_node_no]->AsyncRead(
                &std::get<3>(reply_map[physical_node_no]), request, &cq);
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
          throw std::runtime_error(fmt::format("invalid grpc tag {}", got_tag));
        }
        auto &[_, reply, grpc_status, __] = node.mapped();

        if (!grpc_status.ok()) {
          LOG_ERROR("read block from {} failed:{}", physical_node_no,
                    grpc_status.error_message());
          continue;
        }
        if (reply.has_error()) {
          LOG_ERROR("read block from {} failed:{}", physical_node_no,
                    Error_Name(reply.error()));
          continue;
        }
        raid_blocks[physical_node_no] = reply.ok().block();
      }
      return raid_blocks;
    }

    static std::set<uint64_t> write_raid_row(
        const std::vector<std::unique_ptr<RAIDNode::Stub>> &stubs,
        uint64_t physical_block_no,
        const std::map<uint64_t, const_byte_stream_view_type> &row_blocks) {
      grpc::CompletionQueue cq;
      std::map<
          uint64_t,
          std::tuple<
              std::unique_ptr<grpc::ClientAsyncResponseReader<BlockWriteReply>>,
              BlockWriteReply, ::grpc::Status, ::grpc::ClientContext>>
          reply_map;

      for (auto &[physical_node_no, raid_block] : row_blocks) {
        BlockWriteRequest request;
        request.set_block_no(physical_block_no);
        request.set_block(byte_stream_type(raid_block));

        std::get<0>(reply_map[physical_node_no]) =
            stubs[physical_node_no]->AsyncWrite(
                &std::get<3>(reply_map[physical_node_no]), request, &cq);
        std::get<0>(reply_map[physical_node_no])
            ->Finish(&std::get<1>(reply_map[physical_node_no]),
                     &std::get<2>(reply_map[physical_node_no]),
                     (void *)physical_node_no);
      }
      std::set<uint64_t> succ_physical_nodes;
      while (!reply_map.empty()) {
        void *got_tag = nullptr;
        bool ok = false;
        if (!cq.Next(&got_tag, &ok) || !ok) {
          throw std::runtime_error("cg next failed");
        }
        auto physical_node_no = reinterpret_cast<uint64_t>(got_tag);

        auto node = reply_map.extract(physical_node_no);
        if (node.empty()) {
          throw std::runtime_error(fmt::format("invalid grpc tag {}", got_tag));
        }
        auto &[_, reply, grpc_status, __] = node.mapped();

        if (!grpc_status.ok()) {
          LOG_ERROR("write block {} failed:{}", physical_node_no,
                    grpc_status.error_message());
          continue;
        }
        if (reply.has_error()) {
          LOG_ERROR("write block {} failed:{}", physical_node_no,
                    reply.error());
          continue;
        }
        succ_physical_nodes.insert(physical_node_no);
      }
      return succ_physical_nodes;
    }
  };
  class RAID6Controller : public RAIDController {
  public:
    explicit RAID6Controller(const RAIDConfig &raid_config)
        : data_node_number(raid_config.data_ports.size()),
          capacity(raid_config.disk_capacity * data_node_number),
          block_size(raid_config.block_size) {

      auto ports = raid_config.data_ports;
      ports.insert(ports.end(), raid_config.parity_ports.begin(),
                   raid_config.parity_ports.end());
      for (auto port : ports) {
        auto channel =
            grpc::CreateChannel(fmt::format("localhost:{}", port),
                                ::grpc::InsecureChannelCredentials());
        stubs.emplace_back(RAIDNode::NewStub(channel));
      }
      P_node_idx = data_node_number;
      Q_node_idx = P_node_idx + 1;
    }
    ~RAID6Controller() override = default;
    size_t get_capacity() override { return capacity; }

    std::map<LogicalAddressRange, byte_stream_type>
    read(const std::set<LogicalAddressRange> &data_ranges) override {
      std::shared_lock lk(data_mutex);
      auto raid_blocks = read_blocks(get_raid_block_no(data_ranges));
      std::map<LogicalAddressRange, byte_stream_type> results;
      for (auto const &range : data_ranges) {
        bool const has_raid_block =
            std::ranges::all_of(get_raid_block_no({range}), [&](auto block_no) {
              return raid_blocks.contains(block_no);
            });
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
    std::set<uint64_t>
    write(std::map<uint64_t, byte_stream_type> blocks) override {
      std::map<uint64_t, const_byte_stream_view_type> raid_blocks;
      for (auto const &[offset, block] : blocks) {
        assert(offset % block_size == 0);
        assert(!block.empty());
        assert(block.size() % block_size == 0);
        for (size_t p = offset; p < offset + block.size(); p += block_size) {
          raid_blocks.emplace(p / block_size,
                              const_byte_stream_view_type{
                                  block.data() + p - offset, block_size});
        }
      }
      auto raid_res = write_blocks(raid_blocks);
      std::set<uint64_t> block_result;
      for (auto const &[data_offset, block] : blocks) {
        bool const write_succ = std::ranges::all_of(
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
    get_raid_block_no(const std::set<LogicalAddressRange> &data_ranges) const {
      std::set<uint64_t> raid_block_no_set;
      for (auto const &range : data_ranges) {
        for (auto sub_range : range.split(block_size)) {
          raid_block_no_set.emplace(sub_range.offset / block_size);
        }
      }
      return raid_block_no_set;
    }

    std::map<uint64_t, byte_stream_type>
    read_blocks(std::set<uint64_t> block_no_set) {
      std::map<uint64_t, byte_stream_type> raid_blocks;
      while (!block_no_set.empty()) {
        std::set<uint64_t> new_block_no_set;
        std::map<uint64_t, uint64_t> block_locations;

        for (auto block_no : block_no_set) {
          auto physical_block_no = block_no / data_node_number;
          auto physical_node_no = block_no % data_node_number;
          if (block_locations.contains(physical_node_no)) {
            new_block_no_set.insert(block_no);
            continue;
          }
          block_locations[physical_node_no] = physical_block_no;
        }
        auto res = parallel_read_blocks(stubs, block_locations);
        std::map<uint64_t, byte_stream_type> read_raid_blocks;
        for (auto &[physical_node_no, block] : res) {
          auto block_no = block_locations[physical_node_no] * data_node_number +
                          physical_node_no;
          read_raid_blocks[block_no] = std::move(block);
        }
        auto failed_nodes = block_locations.size() - read_raid_blocks.size();
        if (failed_nodes > 0) {
          recover_data(block_locations, read_raid_blocks);
        }
        raid_blocks.merge(std::move(read_raid_blocks));
        block_no_set = std::move(new_block_no_set);
      }
      return raid_blocks;
    }

    // Recover missing data using RAID 6 mechanism
    void recover_data(const std::map<uint64_t, uint64_t> &block_locations,
                      std::map<uint64_t, byte_stream_type> &read_raid_blocks) {
      // recover from failure
      std::map<uint64_t, std::set<uint64_t>> failed_blocks;
      for (auto [physical_node_no, physical_block_no] : block_locations) {
        auto block_no = physical_block_no * data_node_number + physical_node_no;
        if (!read_raid_blocks.contains(block_no)) {
          failed_blocks[physical_block_no].emplace(physical_node_no);
        }
      }
      bool P_node_avaiable = !invalid_P_node;
      bool Q_node_avaiable = !invalid_Q_node;
      for (auto &[failed_physical_block_no, failed_row_nodes] : failed_blocks) {
        if (failed_row_nodes.size() == 1) {
          // can't recover
          if (!P_node_avaiable && !Q_node_avaiable) {
            continue;
          }
        } else if (failed_row_nodes.size() == 2) {
          // can't recover
          if (!P_node_avaiable || !Q_node_avaiable) {
            continue;
          }
        } else {
          // can't recover
          continue;
        }

        std::map<uint64_t, uint64_t> row_locations;
        std::map<uint64_t, byte_stream_view_type> row_block_views;

        for (uint64_t idx = 0; idx < stubs.size(); idx++) {
          if (failed_row_nodes.contains(idx)) {
            continue;
          }
          if (idx == P_node_idx && !P_node_avaiable) {
            continue;
          }
          if (idx == Q_node_idx && !Q_node_avaiable) {
            continue;
          }
          auto block_no = failed_physical_block_no * data_node_number + idx;
          if (read_raid_blocks.contains(block_no)) {
            row_block_views[idx] = read_raid_blocks[block_no];
            continue;
          }
          row_locations[idx] = failed_physical_block_no;
        }
        auto res = parallel_read_blocks(stubs, row_locations);
        for (auto const &[physical_node_no, _] : row_locations) {
          if (res.contains(physical_node_no)) {
            row_block_views[physical_node_no] = res[physical_node_no];
            continue;
          }
          failed_row_nodes.insert(physical_node_no);
          if (physical_node_no == P_node_idx) {
            P_node_avaiable = false;
          }
          if (physical_node_no == Q_node_idx) {
            Q_node_avaiable = false;
          }
        }

        if (failed_row_nodes.size() == 1) {
          // can't recover
          if (!P_node_avaiable && !Q_node_avaiable) {
            continue;
          }
        } else if (failed_row_nodes.size() == 2) {
          // can't recover
          if (!P_node_avaiable || !Q_node_avaiable) {
            continue;
          }
        } else {
          // can't recover
          continue;
        }
      }
    }

    std::map<uint64_t, std::map<uint64_t, const_byte_stream_view_type>>
    convert_to_physical_nodes(
        const std::map<uint64_t, const_byte_stream_view_type> &raid_blocks)
        const {
      std::map<uint64_t, std::map<uint64_t, const_byte_stream_view_type>>
          physical_blocks;
      for (auto &[block_no, raid_block] : raid_blocks) {
        auto physical_node_no = block_no % data_node_number;
        auto physical_block_no = block_no / data_node_number;
        physical_blocks[physical_block_no][physical_node_no] = raid_block;
      }
      return physical_blocks;
    }

    std::set<uint64_t>
    write_blocks(std::map<uint64_t, const_byte_stream_view_type> raid_blocks) {
      std::lock_guard lk(data_mutex);
      std::set<uint64_t> raid_results;
      auto physical_blocks = convert_to_physical_nodes(std::move(raid_blocks));
      for (auto &[physical_block_no, row_map] : physical_blocks) {
        std::map<uint64_t, uint64_t> block_locations;
        std::set<uint64_t> row_data_nodes;
        if (!invalid_P_node || !invalid_Q_node) {
          for (auto const &[physical_node_no, _] : row_map) {
            block_locations.emplace(physical_node_no, physical_block_no);
            row_data_nodes.insert(physical_node_no);
          }
          if (!invalid_P_node) {
            block_locations.emplace(P_node_idx, physical_block_no);
          }
          if (!invalid_Q_node) {
            block_locations.emplace(Q_node_idx, physical_block_no);
          }
          auto read_res = parallel_read_blocks(stubs, block_locations);
          // write P and Q
          std::optional<block_data_type> P_block_opt;
          if (std::ranges::includes(std::views::keys(read_res),
                                    row_data_nodes)) {
            if (!invalid_P_node && read_res.contains(P_node_idx)) {
              /* auto old_block = */
              /*     byte_stream_type(read_res[P_node_idx_opt.value()]); */
              P_block_opt = std::move(read_res[P_node_idx]);
              galois_field::Element sum(
                  byte_stream_view_type(P_block_opt.value()));
              for (auto const &[physical_node_no, block] : read_res) {
                if (physical_block_no != P_node_idx) {
                  sum += block;
                }
              }
              /* auto new_block = */
              /*     byte_stream_type(read_res[P_node_idx_opt.value()]); */
              /* assert(old_block != new_block); */
              row_map[P_node_idx] = P_block_opt.value();
            }
          }
        }
        auto succ_raid_nodes =
            write_raid_row(stubs, physical_block_no, row_map);
        for (auto physical_node_no : succ_raid_nodes) {
          raid_results.insert(physical_block_no * data_node_number +
                              physical_node_no);
        }
        if (!invalid_P_node) {
          if (!succ_raid_nodes.contains(P_node_idx)) {
            LOG_ERROR("write to P node failed or the data is stale");
            invalid_P_node = true;
          } else if (!std::ranges::includes(succ_raid_nodes, row_data_nodes)) {
            LOG_ERROR("write to some data node failed and P data is stale");
            invalid_P_node = true;
          }
        }
      }
      return raid_results;
    }

  private:
    std::vector<std::unique_ptr<RAIDNode::Stub>> stubs;
    size_t data_node_number{};
    size_t P_node_idx{};
    size_t Q_node_idx{};
    bool invalid_P_node{false};
    bool invalid_Q_node{false};
    size_t capacity{};
    size_t block_size{};
    static inline std::shared_mutex data_mutex;
  };

  inline std::shared_ptr<RAIDController>
  get_RAID_controller(const RAIDConfig &raid_config) {
    return std::make_shared<RAID6Controller>(raid_config);
  }
} // namespace raid_fs
