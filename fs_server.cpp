/*!
 * \file fs_server.cpp
 *
 * \brief
 */
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>

#include <yaml-cpp/yaml.h>
/* #include "error.grpc.pb.h" */
#include "fs.grpc.pb.h"
#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

class FS_server final : public raid_fs::FileSystem::Service {
public:
  explicit FS_server() {}
};

int main(int argc, char **argv) {
  if (argc<=1) {
    std::cerr<<"Usage:"<<argv[0]<<" config.yaml"<<std::endl;
    return -1;
  }
  YAML::Node config = YAML::LoadFile(argv[1]);

  std::string server_address(std::string("0.0.0.0:")+config["fs_port"].as<std::string>());
  FS_server service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
  return 0;
}
