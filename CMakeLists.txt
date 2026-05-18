cmake_minimum_required(VERSION 3.16)
project(event_stream_engine)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(spdlog REQUIRED)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

find_library(RDKAFKA_LIB
    NAMES rdkafka++
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
    REQUIRED
)
find_library(RDKAFKA_C_LIB
    NAMES rdkafka
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
    REQUIRED
)

find_path(REDISPLUSPLUS_INCLUDE_DIR sw/redis++/redis++.h
    PATHS /usr/local/include /usr/include
    REQUIRED
)
find_library(REDISPLUSPLUS_LIB
    NAMES redis++
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
    REQUIRED
)
find_library(HIREDIS_LIB
    NAMES hiredis
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
    REQUIRED
)

find_path(CLICKHOUSE_CPP_INCLUDE_DIR clickhouse/client.h
    PATHS /usr/local/include /usr/include
    REQUIRED
)
find_library(CLICKHOUSE_CPP_LIB
    NAMES clickhouse-cpp clickhouse-cpp-lib
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
    REQUIRED
)
find_library(LZ4_LIB
    NAMES lz4
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu
    REQUIRED
)
find_library(CITYHASH_LIB
    NAMES cityhash
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu
    REQUIRED
)
find_library(ZSTD_LIB
    NAMES zstdstatic zstd
    PATHS /usr/local/lib /usr/lib /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu
    REQUIRED
)

# Generate proto + gRPC stubs
set(PROTO_FILE proto/event.proto)

set(PROTO_SRCS ${CMAKE_CURRENT_BINARY_DIR}/event.pb.cc)
set(PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}/event.pb.h)
set(GRPC_SRCS  ${CMAKE_CURRENT_BINARY_DIR}/event.grpc.pb.cc)
set(GRPC_HDRS  ${CMAKE_CURRENT_BINARY_DIR}/event.grpc.pb.h)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND protoc
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
        --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
        --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
        --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin
        ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}
)

add_executable(engine
    src/main.cpp
    src/grpc_server.cpp
    src/kafka_consumer.cpp
    src/schema_registry.cpp
    src/quality_pipeline.cpp
    src/lambda_router.cpp
    src/circuit_breaker.cpp
    src/tenant_router.cpp
    src/clickhouse_writer.cpp
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

target_include_directories(engine PRIVATE
    include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${REDISPLUSPLUS_INCLUDE_DIR}
    ${CLICKHOUSE_CPP_INCLUDE_DIR}
)

target_link_libraries(engine PRIVATE
    gRPC::grpc++
    ${Protobuf_LIBRARIES}
    ${RDKAFKA_LIB}
    ${RDKAFKA_C_LIB}
    ${REDISPLUSPLUS_LIB}
    ${HIREDIS_LIB}
    ${CLICKHOUSE_CPP_LIB}
    ${LZ4_LIB}
    ${CITYHASH_LIB}
    ${ZSTD_LIB}
    OpenSSL::Crypto
    spdlog::spdlog
    Threads::Threads
)

# Unit tests
add_executable(test_queue tests/test_queue.cpp)
target_include_directories(test_queue PRIVATE include)
target_link_libraries(test_queue PRIVATE Threads::Threads)

add_executable(test_pipeline tests/test_pipeline.cpp
    src/quality_pipeline.cpp
    src/schema_registry.cpp
    src/circuit_breaker.cpp
    src/tenant_router.cpp
    src/lambda_router.cpp
    src/clickhouse_writer.cpp
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_include_directories(test_pipeline PRIVATE
    include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${REDISPLUSPLUS_INCLUDE_DIR}
    ${CLICKHOUSE_CPP_INCLUDE_DIR}
)
target_link_libraries(test_pipeline PRIVATE
    gRPC::grpc++
    ${Protobuf_LIBRARIES}
    ${REDISPLUSPLUS_LIB}
    ${HIREDIS_LIB}
    ${RDKAFKA_LIB}
    ${RDKAFKA_C_LIB}
    ${CLICKHOUSE_CPP_LIB}
    ${LZ4_LIB}
    ${CITYHASH_LIB}
    ${ZSTD_LIB}
    OpenSSL::Crypto
    spdlog::spdlog
    Threads::Threads
)

# Benchmark client
add_executable(bench_client tests/bench_client.cpp
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)
target_include_directories(bench_client PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(bench_client PRIVATE
    OpenSSL::Crypto
    gRPC::grpc++
    ${Protobuf_LIBRARIES}
    Threads::Threads
)
