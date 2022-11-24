file(GLOB test_sources ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

set(ENV{FUZZING_TIMEOUT} 10)
set(ENV{MAX_FUZZING_TIME} 5)
foreach(test_source IN LISTS test_sources)
  get_filename_component(test_prog ${test_source} NAME_WE)
  add_executable(
    ${test_prog} ${test_source} ${fs_grpc_srcs} ${fs_proto_srcs}
                 ${error_proto_srcs} ${raid_grpc_srcs} ${raid_proto_srcs})
  target_include_directories(
    ${test_prog} PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/.."
                         "${CMAKE_CURRENT_LIST_DIR}/../src")
  target_link_libraries(${test_prog} PUBLIC ${LIBS})
  add_fuzzing(TARGET ${test_prog} ASAN ON UBSAN ON TSAN OFF)
endforeach()
