cmake_minimum_required(VERSION 3.23)

project(RAID_FS CXX)

include(cmake/all.cmake)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)

set(proto_dir ${CMAKE_CURRENT_LIST_DIR}/proto)
set(proto_files ${proto_dir}/fs.proto ${proto_dir}/error.proto)
set(proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/fs.pb.cc"
               "${CMAKE_CURRENT_BINARY_DIR}/error.pb.cc")
set(grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/fs.grpc.pb.cc")

get_target_property(grpc_cpp_plugin_path gRPC::grpc_cpp_plugin "LOCATION")

add_custom_command(
  OUTPUT ${proto_srcs} ${grpc_srcs}
  COMMAND
    protobuf::protoc ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}" --cpp_out
    "${CMAKE_CURRENT_BINARY_DIR}" -I "${proto_dir}"
    --plugin=protoc-gen-grpc="${grpc_cpp_plugin_path}" ${proto_files})

add_executable(
  fs_server ${CMAKE_CURRENT_LIST_DIR}/fs_server.cpp
            ${CMAKE_CURRENT_LIST_DIR}/config.hpp ${proto_srcs} ${grpc_srcs})

target_include_directories(fs_server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(
  fs_server PUBLIC protobuf::libprotobuf gRPC::grpc++_unsecure
                   gRPC::grpc_unsecure yaml-cpp)
