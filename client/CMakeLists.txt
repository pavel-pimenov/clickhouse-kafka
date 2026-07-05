cmake_minimum_required(VERSION 3.16)
project(clickhouse-benchmark CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# clickhouse-cpp via FetchContent
include(FetchContent)
set(CH_USE_ABSEIL_FOR_BIGNUM OFF)
set(CH_MAP_BOOL_TO_UINT8 OFF)
FetchContent_Declare(
    clickhouse_cpp
    GIT_REPOSITORY https://github.com/ClickHouse/clickhouse-cpp.git
    GIT_TAG master
)
FetchContent_MakeAvailable(clickhouse_cpp)

# librdkafka via pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka rdkafka++)

add_executable(benchmark_client
    src/main.cpp
    src/record.cpp
    src/clickhouse_writer.cpp
    src/kafka_writer.cpp
    src/utils.cpp
)

target_include_directories(benchmark_client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${RDKAFKA_INCLUDE_DIRS}
)

target_link_libraries(benchmark_client PRIVATE
    ${RDKAFKA_LIBRARIES}
    clickhouse-cpp-lib
    pthread
)
