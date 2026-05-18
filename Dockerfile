# event_stream_engine 多阶段构建
# 阶段1：编译环境
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# 系统依赖（gRPC + Protobuf 通过 apt 安装）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git pkg-config \
    libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    libspdlog-dev libfmt-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# librdkafka
RUN git clone --depth 1 --branch v2.3.0 https://github.com/confluentinc/librdkafka.git /tmp/librdkafka \
    && cd /tmp/librdkafka \
    && ./configure --prefix=/usr/local \
    && make -j$(nproc) && make install \
    && rm -rf /tmp/librdkafka

# hiredis + redis++
RUN git clone --depth 1 https://github.com/redis/hiredis.git /tmp/hiredis \
    && cd /tmp/hiredis && make -j$(nproc) && make install \
    && rm -rf /tmp/hiredis
RUN git clone --depth 1 https://github.com/sewenew/redis-plus-plus.git /tmp/redis-plus-plus \
    && cd /tmp/redis-plus-plus \
    && mkdir build && cd build \
    && cmake .. -DREDIS_PLUS_PLUS_CXX_STANDARD=17 -DREDIS_PLUS_PLUS_BUILD_TEST=OFF \
    && make -j$(nproc) && make install \
    && rm -rf /tmp/redis-plus-plus

# clickhouse-cpp
RUN git clone --depth 1 https://github.com/ClickHouse/clickhouse-cpp.git /tmp/clickhouse-cpp \
    && cd /tmp/clickhouse-cpp \
    && mkdir build && cd build \
    && cmake .. -DCMAKE_BUILD_TYPE=Release \
    && make -j$(nproc) && make install \
    && rm -rf /tmp/clickhouse-cpp

# 编译项目
COPY . /src
WORKDIR /src/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release \
    && make -j$(nproc)

# 阶段2：运行环境（精简镜像）
FROM ubuntu:22.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51 libprotobuf23 libssl3 \
    libspdlog-dev libfmt-dev \
    && rm -rf /var/lib/apt/lists/*

# 从 builder 拷贝编译产物和第三方库
COPY --from=builder /usr/local/lib /usr/local/lib
COPY --from=builder /src/build/engine /app/engine
COPY --from=builder /src/build/bench_client /app/bench_client
COPY --from=builder /src/build/test_pipeline /app/test_pipeline
RUN ldconfig

WORKDIR /app
EXPOSE 50051

CMD ["./engine"]
