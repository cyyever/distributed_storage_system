/*!
 * \file flow_network_fuzzing.cpp
 *
 */
#include "helper.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  try {
    bool o_create = true;
    bool o_excl = false;
    if (Size < 100) {
      return 0;
    }
    std::string path(reinterpret_cast<const char *>(Data), Size / 2);
    Data += Size / 2;
    Size -= Size / 2;
    auto open_res = get_file_system_impl().open(path, o_create, o_excl);
    if (!open_res.has_value()) {
      return 0;
    }
    auto fd = open_res.value().first;
    uint64_t offset = *reinterpret_cast<const uint64_t *>(Data);
    Data += sizeof(uint64_t);
    Size -= sizeof(uint64_t);
    uint64_t length = *reinterpret_cast<const uint64_t *>(Data);
    Data += sizeof(uint64_t);
    Size -= sizeof(uint64_t);
    get_file_system_impl().read(fd, {offset, length});
    raid_fs::const_block_data_view_type data_view(
        reinterpret_cast<const char *>(Data), Size);
    get_file_system_impl().write(fd, offset, data_view);
    get_file_system_impl().read(fd, {offset, length});
  } catch (const std::exception &) {
  }
  return 0; // Non-zero return values are reserved for future use.
}
