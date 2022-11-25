/*!
 * \file flow_network_fuzzing.cpp
 *
 */
#include "helper.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  try {
    if (Size < 100) {
      return 0;
    }
    bool o_create = true;
    bool o_excl = true;
    if (Size >= 2) {
      o_create = Data[0];
      o_excl = Data[1];
      Data += 2;
      Size -= 2;
    }
    std::string path(reinterpret_cast<const char *>(Data), Size);
    get_file_system_impl().open(path, o_create, o_excl);
    get_file_system_impl().remove_file(path);
    get_file_system_impl().remove_dir(path);
  } catch (const std::exception &) {
  }
  return 0; // Non-zero return values are reserved for future use.
}
