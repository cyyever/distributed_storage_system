/*!
 * \file disk.hpp
 *
 * \brief We implement in-memory disk and real disk storage
 */

#pragma once
#include <errno.h>
#include <expected>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <optional>
#include <unistd.h>
#include <vector>

#include <cyy/naive_lib/log/log.hpp>
#include <cyy/naive_lib/util/error.hpp>
#include <spdlog/fmt/fmt.h>
#include <sys/file.h>
#include <sys/types.h>

#include "config.hpp"
#include "type.hpp"

namespace raid_fs {

  class VirtualDisk {
  public:
    explicit VirtualDisk(size_t disk_capacity_, size_t block_size_)
        : block_size(block_size_), block_number(disk_capacity_ / block_size) {}
    virtual ~VirtualDisk() = default;

    virtual std::expected<block_data_type, int> read(size_t block_no) = 0;
    virtual std::optional<int> write(size_t block_no,
                                     const block_data_type &data) = 0;
    size_t get_block_number() const { return block_number; }
    size_t get_block_size() const { return block_size; }

  protected:
    size_t block_size{};
    size_t block_number{};
  };

  class MemoryDisk final : public VirtualDisk {
  public:
    explicit MemoryDisk(size_t disk_capacity_, size_t block_size_)
        : VirtualDisk(disk_capacity_, block_size_) {
      disk.resize(block_number);
    }
    virtual ~MemoryDisk() = default;

    std::expected<block_data_type, int> read(size_t block_no) override {
      auto &block = disk.at(block_no);
      if (block.empty()) {
        block.resize(block_size, '\0');
      }
      return block;
    }
    std::optional<int> write(size_t block_no,
                             const block_data_type &data) override {
      disk[block_no] = data;
      return {};
    }

  private:
    std::vector<std::string> disk;
  };

  class Disk final : public VirtualDisk {
  public:
    Disk(size_t disk_capacity_, size_t block_size_, std::string file_name)
        : VirtualDisk(disk_capacity_, block_size_) {
      fd = open(file_name.c_str(), O_CREAT | O_NOATIME | O_CLOEXEC | O_RDWR,
                S_IRUSR | S_IWUSR);
      if (fd < 0) {
        LOG_ERROR("open disk {} failed:{}", file_name,
                  ::cyy::naive_lib::util::errno_to_str(errno));
        throw std::runtime_error("failed to open disk");
      }
      if (flock(fd, LOCK_EX) != 0) {
        LOG_ERROR("lock disk {} failed:{}", file_name,
                  ::cyy::naive_lib::util::errno_to_str(errno));
        throw std::runtime_error("failed to lock disk");
      }
    }
    std::expected<block_data_type, int> read(size_t block_no) override {
      block_data_type block;
      block.resize(block_size, '\0');
      auto res = pread(fd, block.data(), block_size, block_no * block_size);
      if (res == 0) {
        return block;
      }
      if (res > 0) {
        if (res != block_size) {
          throw std::runtime_error(fmt::format(
              "read disk block {} with unmatched size:{}", block_no, res));
        }
        return block;
      }
      auto old_errno = errno;
      LOG_ERROR("read disk block {} failed:{}", block_no,
                ::cyy::naive_lib::util::errno_to_str(old_errno));
      return std::unexpected<int>(old_errno);
    }
    std::optional<int> write(size_t block_no,
                             const block_data_type &data) override {
      assert(data.size() == block_size);
      auto res = pwrite(fd, data.data(), block_size, block_no * block_size);
      if (res < 0) {
        auto old_errno = errno;
        LOG_ERROR("write disk block {} failed:{}", block_no,
                  ::cyy::naive_lib::util::errno_to_str(old_errno));
        return old_errno;
      }
      if (res != block_size) {
        throw std::runtime_error(fmt::format(
            "read disk block {} with unmatched size:{}", block_no, res));
      }
      return {};
    }

    virtual ~Disk() { close(fd); }

  private:
    int fd{-1};
  };

  inline std::shared_ptr<VirtualDisk> get_disk(const RAIDConfig &raid_config,
                                               size_t disk_id = 0) {
    if (raid_config.use_memory_disk) {
      return std::make_shared<MemoryDisk>(raid_config.disk_capacity,
                                          raid_config.block_size);
    }
    return std::make_shared<Disk>(
        raid_config.disk_capacity, raid_config.block_size,
        raid_config.disk_path_prefix + std::to_string(disk_id));
  }

} // namespace raid_fs
