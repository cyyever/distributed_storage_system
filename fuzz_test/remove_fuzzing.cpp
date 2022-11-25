/*!
 * \file flow_network_fuzzing.cpp
 *
 */
#include "helper.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  try {
    if (Size == 0) {
      return 0;
    }
    std::string path(reinterpret_cast<const char *>(Data), Size);
    get_file_system_impl().remove_file(path);
    get_file_system_impl().remove_dir(path);
  } catch (const std::exception &) {
  }
  return 0; // Non-zero return values are reserved for future use.
}
