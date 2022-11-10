
#pragma once
#include <expected>
#include <optional>

#include <sys/types.h>

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
  }; // namespace raid_fs
     //
  inline std::shared_ptr<VirtualDisk> get_disk(const RAIDConfig &raid_config) {
    if (raid_config.use_memory_disk) {
      return std::make_shared<MemoryDisk>(raid_config.disk_capacity,
                                          raid_config.block_size);
    }
    return {};
  }

} // namespace raid_fs
