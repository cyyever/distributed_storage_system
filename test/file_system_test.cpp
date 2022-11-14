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
  SUBCASE("open /") {
    ::grpc::ClientContext context;
    raid_fs::OpenRequest request;
    request.set_path("/");
    request.set_o_create(false);
    request.set_o_excl(false);
    raid_fs::OpenReply reply;
    auto grpc_status = stub->Open(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(!reply.has_ok());
    REQUIRE_EQ(reply.error(), ::raid_fs::ERROR_PATH_IS_DIR);
  }
  SUBCASE("open nonexistent file") {
    ::grpc::ClientContext context;
    raid_fs::OpenRequest request;
    request.set_path("/foo/bar");
    request.set_o_create(false);
    request.set_o_excl(false);
    raid_fs::OpenReply reply;
    auto grpc_status = stub->Open(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(!reply.has_ok());
    REQUIRE_EQ(reply.error(), ::raid_fs::ERROR_NONEXISTENT_FILE);
  }
  SUBCASE("create file") {
    ::grpc::ClientContext context;
    raid_fs::OpenRequest request;
    request.set_path("/foo/bar");
    request.set_o_create(true);
    request.set_o_excl(true);
    raid_fs::OpenReply reply;
    auto grpc_status = stub->Open(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(reply.has_ok());
  }
  SUBCASE("recreate file") {
    ::grpc::ClientContext context;
    raid_fs::OpenRequest request;
    request.set_path("/foo/bar");
    request.set_o_create(true);
    request.set_o_excl(true);
    raid_fs::OpenReply reply;
    auto grpc_status = stub->Open(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(!reply.has_ok());
    REQUIRE_EQ(reply.error(), ::raid_fs::ERROR_EXISTED_FILE);
  }
}
