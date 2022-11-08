/*!
 * \file raid_controller.hpp
 *
 * \brief
 */
#pragma once

#include <variant>
#include <optional>

#include "error.pb.h"
namespace raid_fs {
  class RAIDController {
  public:
    virtual std::variant<Error, std::string> read_block(size_t block_no) = 0;
    virtual std::optional<Error> write_block(size_t block_no,
                                             std::string &&block) = 0;
    /* ::grpc::Status Write(::grpc::ServerContext *context, */
    /*                      const ::raid_fs::BlockWriteRequest *request, */
    /*                      ::raid_fs::BlockWriteReply *response) override { */
    /*   if (request->block_no() >= disk.size()) { */
    /*     response->set_error(Error::ERROR_OUT_OF_RANGE); */
    /*     return ::grpc::Status::OK; */
    /*   } */
    /*   if (request->block().size() != block_size) { */
    /*     response->set_error(Error::ERROR_INVALID_BLOCK); */
    /*     return ::grpc::Status::OK; */
    /*   } */
    /*   disk[request->block_no()] = std::move(request->block()); */
    /*   return ::grpc::Status::OK; */
    /* } */
  };
} // namespace raid_fs
