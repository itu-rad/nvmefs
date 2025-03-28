cmake_minimum_required(VERSION 3.5)

project(google_unit_test)

# GoogleTest requires at least C++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
  googletest
  DOWNLOAD_EXTRACT_TIMESTAMP true
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip 
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${CMAKE_SOURCE_DIR}/duckdb/src/include)

add_executable(nvmefs_gtest "test.cpp" "test_nvmefs_proxy.cpp")

target_link_libraries(nvmefs_gtest GTest::gtest_main ${EXTENSION_NAME} duckdb)
set_target_properties(nvmefs_gtest PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")

include(GoogleTest)
gtest_discover_tests(nvmefs_gtest)
