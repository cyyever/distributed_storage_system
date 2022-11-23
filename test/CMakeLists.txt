find_package(doctest REQUIRED)

file(GLOB test_sources ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

foreach(test_source IN LISTS test_sources)
  get_filename_component(test_prog ${test_source} NAME_WE)
  add_executable(${test_prog} ${test_source} ${fs_grpc_srcs} ${fs_proto_srcs}
                              ${error_proto_srcs})
  target_link_libraries(${test_prog} PUBLIC ${LIBS})
  target_compile_definitions(${test_prog}
                             PRIVATE DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN)
  target_compile_definitions(
    ${test_prog} PRIVATE DOCTEST_CONFIG_NO_EXCEPTIONS_BUT_WITH_ALL_ASSERTS)
  message(STATUS ${CMAKE_CURRENT_BINARY_DIR}/../src)
  target_include_directories(
    ${test_prog} PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/.."
                         "${CMAKE_CURRENT_LIST_DIR}/../src")
  target_link_libraries(${test_prog} PUBLIC ${LIBS})
  add_test(TARGET ${test_prog})
  # add_test_with_runtime_analysis(TARGET ${test_prog} HELGRIND OFF TSAN OFF)
endforeach()
