/*!
 * \file config.hpp
 *
 * \brief Configuration
 */
#pragma once
#include <filesystem>
#include <memory>
#include <regex>
#include <string>

#include <fmt/format.h>

#include <yaml-cpp/yaml.h>

namespace raid_fs {
  struct Config {
    explicit Config(const std::filesystem::path &config_file) {
        yaml_node = YAML::LoadFile(config_file.string());
      disk_capacity = parse_size(yaml_node, "disk_capacity");
      debug_log = yaml_node["debug_log"].as<bool>();
    }
    static size_t parse_size(const YAML::Node &node, const std::string &key) {
      size_t result = 0;
      std::smatch match;
      std::regex re("^([0-9]+)([KMGT]?)B$");
      auto config_value = node[key].as<std::string>();
      if (std::regex_search(config_value, match, re)) {
        result = std::stoull(match.str(1));
        if (match.str(2) == "K") {
          result *= 1024;
        } else if (match.str(2) == "M") {
          result *= 1024;
          result *= 1024;
        } else if (match.str(2) == "G") {
          result *= 1024;
          result *= 1024;
          result *= 1024;
        } else if (match.str(2) == "T") {
          result *= 1024;
          result *= 1024;
          result *= 1024;
          result *= 1024;
        }
        return result;
      }
      throw std::invalid_argument(
          fmt::format("invalid value for {}:{}", key, config_value));
    }
    size_t disk_capacity{};
    bool debug_log{};
    YAML::Node yaml_node;
  };
  struct FileSystemConfig : public Config {
    explicit FileSystemConfig(const std::filesystem::path &config_file)
        : Config(config_file) {
      auto sub_yaml_node=yaml_node["filesystem"];
      port = sub_yaml_node["port"].as<uint16_t>();
      block_size = parse_size(sub_yaml_node, "block_size");
      if (disk_capacity % block_size != 0) {
        throw std::invalid_argument("disk can't be partitioned into blocks");
      }
      block_pool_size = parse_size(sub_yaml_node, "block_pool_size");
    }
    uint16_t port{};
    size_t block_size{};
    size_t block_pool_size{};
  };
  struct RAIDConfig : public Config {
    explicit RAIDConfig(const std::filesystem::path &config_file)
        : Config(config_file) {
      auto sub_yaml_node=yaml_node["RAID"];
      auto port = sub_yaml_node["first_node_port"].as<uint16_t>();
      auto node_number = sub_yaml_node["node_number"].as<size_t>();
      for (size_t i = 0; i < node_number; i++) {
        ports.emplace_back(port + i);
      }

      block_size = parse_size(sub_yaml_node, "block_size");
      if (disk_capacity % block_size != 0) {
        throw std::invalid_argument("disk can't be partitioned into blocks");
      }
      use_memory_disk = sub_yaml_node["use_memory_disk"].as<bool>();
      disk_path_prefix = sub_yaml_node["disk_path_prefix"].as<std::string>();
    }
    std::vector<uint16_t> ports;
    size_t block_size{};
    bool use_memory_disk{};
    std::string disk_path_prefix{};
  };
} // namespace raid_fs
