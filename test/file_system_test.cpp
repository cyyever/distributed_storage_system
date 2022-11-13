/*!
 * \file file_system_test.cpp
 *
 */
#include <filesystem>

#include <doctest/doctest.h>
#include <fmt/format.h>
#include <grpc/grpc.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "../config.hpp"
#include "fs.grpc.pb.h"

TEST_CASE("file_system") {
  auto config = ::raid_fs::FileSystemConfig(
      std::filesystem::path(__FILE__).parent_path().parent_path() / "conf" /
      "raid_fs.yaml");

  auto channel = grpc::CreateChannel(fmt::format("localhost:{}", config.port),
                                     ::grpc::InsecureChannelCredentials());
  auto stub = ::raid_fs::FileSystem::NewStub(channel);

  SUBCASE("create") {}
}
