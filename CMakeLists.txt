cmake_minimum_required(VERSION 3.23)

project(RAID_FS CXX)

set(CMAKE_CXX_STANDARD 23)
include(cmake/all.cmake)

option(USE_ASAN "Build with asan" OFF)
option(USE_UBSAN "Build with ubsan" OFF)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(spdlog REQUIRED)
find_package(CyyNaiveLib REQUIRED)
find_package(Boost REQUIRED)
find_package(CyyAlgorithmLib REQUIRED)

if(USE_ASAN OR USE_UBSAN)
  find_package(GoogleSanitizer REQUIRED)
endif()

set(proto_dir ${CMAKE_CURRENT_LIST_DIR}/proto)
set(proto_files ${proto_dir}/fs.proto ${proto_dir}/error.proto
                ${proto_dir}/raid.proto)
set(error_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/error.pb.cc")
set(fs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/fs.grpc.pb.cc")
set(fs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/fs.pb.cc")
set(raid_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/raid.grpc.pb.cc")
set(raid_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/raid.pb.cc")

get_target_property(grpc_cpp_plugin_path gRPC::grpc_cpp_plugin "LOCATION")

add_custom_command(
  OUTPUT ${error_proto_srcs} ${fs_grpc_srcs} ${fs_proto_srcs} ${raid_proto_srcs}
         ${raid_grpc_srcs}
  COMMAND
    protobuf::protoc ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}" --cpp_out
    "${CMAKE_CURRENT_BINARY_DIR}" -I "${proto_dir}"
    --plugin=protoc-gen-grpc="${grpc_cpp_plugin_path}" ${proto_files})

add_executable(
  fs_server
  ${CMAKE_CURRENT_LIST_DIR}/fs_server.cpp
  ${CMAKE_CURRENT_LIST_DIR}/config.hpp
  ${fs_grpc_srcs}
  ${fs_proto_srcs}
  ${error_proto_srcs}
  ${raid_grpc_srcs}
  ${raid_proto_srcs})

target_include_directories(fs_server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fs_server PUBLIC gRPC::grpc++_unsecure yaml-cpp fmt::fmt
                                       CyyAlgorithmLib CyyNaiveLib::log)

if(USE_ASAN)
  target_link_libraries(fs_server PUBLIC GoogleSanitizer::address)
endif()
if(USE_UBSAN)
  target_link_libraries(fs_server PUBLIC GoogleSanitizer::undefined)
endif()

add_executable(
  raid_node_server
  ${CMAKE_CURRENT_LIST_DIR}/raid_node_server.cpp
  ${CMAKE_CURRENT_LIST_DIR}/config.hpp ${raid_grpc_srcs} ${raid_proto_srcs}
  ${error_proto_srcs})

target_include_directories(raid_node_server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(
  raid_node_server PUBLIC gRPC::grpc++_unsecure yaml-cpp fmt::fmt
                          CyyNaiveLib::log CyyNaiveLib::util)

target_compile_definitions(raid_node_server PRIVATE _LARGEFILE64_SOURCE)
