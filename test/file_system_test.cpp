/*!
 * \file file_system_test.cpp
 *
 */
#include <filesystem>
#include <optional>

#include <cyy/naive_lib/log/log.hpp>
#include <doctest/doctest.h>
#include <fmt/format.h>
#include <grpc/grpc.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "../block.hpp"
#include "../config.hpp"
#include "fs.grpc.pb.h"

TEST_CASE("file_system") {
  auto config = ::raid_fs::FileSystemConfig(
      std::filesystem::path(__FILE__).parent_path().parent_path() / "conf" /
      "raid_fs.yaml");

  auto channel = grpc::CreateChannel(fmt::format("localhost:{}", config.port),
                                     ::grpc::InsecureChannelCredentials());
  auto stub = ::raid_fs::FileSystem::NewStub(channel);

  SUBCASE("get file system info") {
    ::grpc::ClientContext context;
    ::google::protobuf::Empty request;
    raid_fs::FileSystemInfoReply reply;
    auto grpc_status = stub->GetFileSystemInfo(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(!reply.has_error());
    LOG_INFO("file number {}", reply.ok().file_number());
    LOG_INFO("used data block number {}", reply.ok().used_data_block_number());
  }
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
    REQUIRE(reply.ok().file_size() == 0);
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

  SUBCASE("read empty file") {
    uint64_t fd{};

    {
      ::grpc::ClientContext context;
      raid_fs::OpenRequest request;
      request.set_path("/foo/bar");
      request.set_o_create(false);
      request.set_o_excl(false);
      raid_fs::OpenReply reply;
      auto grpc_status = stub->Open(&context, request, &reply);
      REQUIRE(grpc_status.ok());
      REQUIRE(reply.has_ok());
      REQUIRE(reply.ok().file_size() == 0);
      fd = reply.ok().fd();
    }

    ::grpc::ClientContext context;
    raid_fs::ReadRequest request;
    request.set_fd(fd);
    request.set_offset(1024);
    request.set_count(0);
    raid_fs::ReadReply reply;
    auto grpc_status = stub->Read(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(reply.has_ok());
    REQUIRE(reply.ok().data().empty());
    REQUIRE(reply.ok().file_size() == 0);
  }
  SUBCASE("write file") {
    uint64_t fd{};

    {
      ::grpc::ClientContext context;
      raid_fs::OpenRequest request;
      request.set_path("/foo/bar");
      request.set_o_create(false);
      request.set_o_excl(false);
      raid_fs::OpenReply reply;
      auto grpc_status = stub->Open(&context, request, &reply);
      REQUIRE(grpc_status.ok());
      REQUIRE(reply.has_ok());
      REQUIRE(reply.ok().file_size() == 0);
      fd = reply.ok().fd();
    }

    ::grpc::ClientContext context;
    raid_fs::WriteRequest request;
    request.set_fd(fd);
    uint64_t offset = config.block_size + 1;
    request.set_offset(offset);
    uint64_t written_size = 2 * config.block_size;
    request.set_data(::raid_fs::block_data_type(written_size, '1'));
    raid_fs::WriteReply reply;
    auto grpc_status = stub->Write(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(reply.has_ok());
    REQUIRE_EQ(reply.ok().written_size(), written_size);
    REQUIRE(reply.ok().file_size() == offset + written_size);
  }

  SUBCASE("read file") {
    uint64_t fd{};

    {
      ::grpc::ClientContext context;
      raid_fs::OpenRequest request;
      request.set_path("/foo/bar");
      request.set_o_create(false);
      request.set_o_excl(false);
      raid_fs::OpenReply reply;
      auto grpc_status = stub->Open(&context, request, &reply);
      REQUIRE(grpc_status.ok());
      REQUIRE(reply.has_ok());
      fd = reply.ok().fd();
    }

    ::grpc::ClientContext context;
    raid_fs::ReadRequest request;
    request.set_fd(fd);
    request.set_offset(config.block_size + 1);
    request.set_count(2 * config.block_size);
    raid_fs::ReadReply reply;
    auto grpc_status = stub->Read(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(reply.has_ok());
    REQUIRE_EQ(reply.ok().data().size(), 2 * config.block_size);
    REQUIRE(std::ranges::all_of(reply.ok().data(),
                                [](auto const c) { return c == '1'; }));
  }
  SUBCASE("write large file") {
    uint64_t fd{};

    {
      ::grpc::ClientContext context;
      raid_fs::OpenRequest request;
      request.set_path("/foo/bar");
      request.set_o_create(false);
      request.set_o_excl(false);
      raid_fs::OpenReply reply;
      auto grpc_status = stub->Open(&context, request, &reply);
      REQUIRE(grpc_status.ok());
      REQUIRE(reply.has_ok());
      fd = reply.ok().fd();
    }

    ::grpc::ClientContext context;
    raid_fs::WriteRequest request;
    request.set_fd(fd);
    uint64_t offset = 4 * config.block_size;
    request.set_offset(offset);
    uint64_t written_size = 1024 * config.block_size;
    request.set_data(::raid_fs::block_data_type(written_size, '1'));
    raid_fs::WriteReply reply;
    auto grpc_status = stub->Write(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(reply.has_ok());
    REQUIRE_EQ(reply.ok().written_size(), written_size);
    REQUIRE(reply.ok().file_size() == offset + written_size);
  }

  SUBCASE("remove file") {
    {
      ::grpc::ClientContext context;
      raid_fs::RemoveRequest request;
      request.set_path("/foo");
      raid_fs::RemoveReply reply;
      auto grpc_status = stub->Remove(&context, request, &reply);
      REQUIRE(grpc_status.ok());
      REQUIRE(reply.has_error());
      REQUIRE_EQ(reply.error(), ::raid_fs::ERROR_PATH_IS_DIR);
    }
    {
      ::grpc::ClientContext context;
      raid_fs::RemoveRequest request;
      request.set_path("/foo/bar");
      raid_fs::RemoveReply reply;
      auto grpc_status = stub->Remove(&context, request, &reply);
      REQUIRE(grpc_status.ok());
      REQUIRE(!reply.has_error());
    }
    {
      ::grpc::ClientContext context;
      raid_fs::RemoveRequest request;
      request.set_path("/foo/bar");
      raid_fs::RemoveReply reply;
      auto grpc_status = stub->Remove(&context, request, &reply);
      REQUIRE(grpc_status.ok());
      REQUIRE(reply.has_error());
      REQUIRE_EQ(reply.error(), ::raid_fs::ERROR_NONEXISTENT_FILE);
    }
  }
  SUBCASE("remove dir") {
    ::grpc::ClientContext context;
    raid_fs::RemoveDirRequest request;
    request.set_path("/foo");
    raid_fs::RemoveDirReply reply;
    auto grpc_status = stub->RemoveDir(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(!reply.has_error());
  }
  SUBCASE("get file system info again") {
    ::grpc::ClientContext context;
    ::google::protobuf::Empty request;
    raid_fs::FileSystemInfoReply reply;
    auto grpc_status = stub->GetFileSystemInfo(&context, request, &reply);
    REQUIRE(grpc_status.ok());
    REQUIRE(!reply.has_error());
    LOG_INFO("file number {}", reply.ok().file_number());
    LOG_INFO("used data block number {}", reply.ok().used_data_block_number());
  }
}
