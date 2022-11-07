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
#include <yaml-cpp/yaml.h>

namespace raid_fs {
struct config {
  explicit config(const std::filesystem::path &config_file) {
    YAML::Node yaml_config = YAML::LoadFile(config_file);

    fs_port = yaml_config["fs_port"].as<uint16_t>();

    std::regex re("^([0-9]+)GB$");
    std::smatch match;
    auto disk_capacity_str = yaml_config["disk_capacity"].as<std::string>();
    if (std::regex_search(disk_capacity_str, match, re)) {
      disk_capacity = std::stoull(match.str(1));
      disk_capacity *= 1024 * 1024 * 1024;
    } else {
      throw std::invalid_argument(std::string("invalid disk capacity:") +
                                  disk_capacity_str);
    }

    re = "^([0-9]+)KB$";
    auto fs_block_size_str = yaml_config["fs_block_size"].as<std::string>();
    if (std::regex_search(fs_block_size_str, match, re)) {
      fs_block_size = std::stoull(match.str(1));
      fs_block_size *= 1024;
    } else {
      throw std::invalid_argument(
          std::string("invalid file system block size:") + fs_block_size_str);
    }
  }
  uint16_t fs_port{};
  size_t disk_capacity{};
  size_t fs_block_size{};
};
} // namespace raid_fs
