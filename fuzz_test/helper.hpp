/*!
 * \file helper.hpp
 *
 * \brief fuzzing helper functions
 * \author cyy
 */
#pragma once

#include "../config.hpp"
#include "../raid_file_system.hpp"

inline ::raid_fs::RAIDFileSystem &get_file_system_impl() {
  static std::unique_ptr<::raid_fs::RAIDFileSystem> fs_ptr;
  if (!fs_ptr) {
    auto config_path =
        std::filesystem::path(__FILE__).parent_path().parent_path() / "conf" /
        "raid_fs.yaml";
    auto fs_config = ::raid_fs::FileSystemConfig(config_path);
    auto raid_config = ::raid_fs::RAIDConfig(config_path);
    fs_ptr = std::make_unique<::raid_fs::RAIDFileSystem>(
        fs_config, get_RAID_controller(raid_config));
  }
  return *fs_ptr;
}
