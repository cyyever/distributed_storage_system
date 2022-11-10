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
      YAML::Node yaml_node = YAML::LoadFile(config_file);

      disk_capacity = parse_size(yaml_node, "disk_capacity");
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
  };
  struct FileSystemConfig : public Config {
    explicit FileSystemConfig(const std::filesystem::path &config_file)
        : Config(config_file) {
      YAML::Node yaml_node = YAML::LoadFile(config_file)["filesystem"];
      port = yaml_node["port"].as<uint16_t>();
      block_size = parse_size(yaml_node, "block_size");
      if (disk_capacity % block_size != 0) {
        throw std::invalid_argument("disk can't be partitioned into blocks");
      }
      block_pool_size = parse_size(yaml_node, "block_pool_size");
    }
    uint16_t port{};
    size_t block_size{};
    size_t block_pool_size{};
  };
  struct RAIDConfig : public Config {
    explicit RAIDConfig(const std::filesystem::path &config_file)
        : Config(config_file) {
      YAML::Node yaml_node = YAML::LoadFile(config_file)["RAID"];
      auto port = yaml_node["first_node_port"].as<uint16_t>();
      auto node_number = yaml_node["node_number"].as<size_t>();
      for (size_t i = 0; i < node_number; i++) {
        ports.emplace_back(port + i);
      }

      block_size = parse_size(yaml_node, "block_size");
      if (disk_capacity % block_size != 0) {
        throw std::invalid_argument("disk can't be partitioned into blocks");
      }
      use_memory_disk = yaml_node["use_memory_disk"].as<bool>();
    }
    std::vector<uint16_t> ports;
    size_t block_size{};
    bool use_memory_disk{};
  };
} // namespace raid_fs
