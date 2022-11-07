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
      std::regex re("^([0-9]+)([KG]?)B$");
      auto config_value = node[key].as<std::string>();
      if (std::regex_search(config_value, match, re)) {
        result = std::stoull(match.str(1));
        if (match.str(2) == "K") {
          result *= 1024;
        } else if (match.str(2) == "G") {
          result *= 1024 * 1024 * 1024;
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
    }
    uint16_t port{};
    size_t block_size{};
  };
} // namespace raid_fs
