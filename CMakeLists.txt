cmake_minimum_required(VERSION 3.23)

project(RAID_FS CXX)

include(cmake/all.cmake)

find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
# find_package(PkgConfig REQUIRED)

# pkg_search_module(grpc REQUIRED IMPORTED_TARGET grpc) pkg_search_module(grpc++
# REQUIRED IMPORTED_TARGET grpc++)

set(proto_dir ${CMAKE_CURRENT_LIST_DIR}/proto)
# set(proto_files ${proto_dir}/fs.proto ${proto_dir}/raiyypd.proto)
set(proto_files ${proto_dir}/fs.proto ${proto_dir}/error.proto)
set(proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/fs.pb.cc")
set(proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/fs.pb.h")
set(grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/fs.grpc.pb.cc")
set(grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/fs.grpc.pb.h")

# if(TARGET gRPC::grpc_cpp_plugin) message(STATUS "aaaaaaaaa") message(STATUS
# "$<TARGET_FILE:grpc_cpp_plugin>") endif() set(grpc_cpp_plugin_path
# "$<TARGET_FILE:gRPC::grpc_cpp_plugin>") message(STATUS gRPC::grpc_cpp_plugin)

add_custom_command(
  OUTPUT "${proto_srcs}" "${proto_hdrs}" "${grpc_srcs}" "${grpc_hdrs}"
  COMMAND
    protobuf::protoc ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}" --cpp_out
    "${CMAKE_CURRENT_BINARY_DIR}" -I "${proto_dir}"
    --plugin=protoc-gen-grpc="/opt/homebrew/bin/grpc_cpp_plugin"
    ${proto_files})

add_executable(fs_server ${CMAKE_CURRENT_LIST_DIR}/server.cpp ${proto_srcs}
                         ${grpc_srcs} ${proto_hdrs} ${grpc_hdrs})

target_include_directories(fs_server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(fs_server PRIVATE protobuf::libprotobuf gRPC::grpc++_unsecure)
