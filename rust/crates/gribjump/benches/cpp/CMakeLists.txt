cmake_minimum_required(VERSION 3.14)
project(gribjump_bench_cpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE Release)

# Fetch Google Benchmark
include(FetchContent)
FetchContent_Declare(
  benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.8.3
)
set(BENCHMARK_ENABLE_TESTING OFF)
FetchContent_MakeAvailable(benchmark)

# Find gribjump and all its dependencies
find_package(eckit REQUIRED)
find_package(metkit REQUIRED)
find_package(fdb5 REQUIRED)
find_package(gribjump REQUIRED)

add_executable(gribjump_bench gribjump_bench.cpp)
target_link_libraries(gribjump_bench
  PRIVATE
    benchmark::benchmark
    gribjump
    fdb5
    metkit
    eckit
)

# Add gribjump source include path if provided (for private headers like Types.h)
if(GRIBJUMP_SRC_INCLUDE)
  message(STATUS "Adding gribjump source include: ${GRIBJUMP_SRC_INCLUDE}")
  target_include_directories(gribjump_bench PRIVATE ${GRIBJUMP_SRC_INCLUDE})
endif()
