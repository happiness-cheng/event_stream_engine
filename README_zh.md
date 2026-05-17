# event_stream_engine

**简体中文** | [English](./README.md)

---

实时事件流处理引擎。从 gRPC/Kafka 接收事件，经过 Quality Pipeline 四阶段质量过滤后，通过 Lambda 架构分流至 Redis Streams（实时）和 ClickHouse（离线）。

## 功能特性

- **双通道接入** — 同时支持 gRPC（`SendEvent`/`SendBatch`）和 Kafka 消费者
- **有界队列** — 线程安全阻塞队列（默认容量 10000），mutex + condition_variable 背压控制
- **Quality Pipeline 四阶段质量过滤**：
  - 去重：Redis SET + 内存滑动窗口（最大 10 万条）
  - 纠偏：Watermark 中位数，拒绝偏差超 ±1 小时的时间戳
  - 反伪造：HMAC-SHA256 签名验证
  - 补全：GeoIP 字段自动补全（不拒绝，仅增强）
- **Lambda 架构分流** — `click`/`view` 走热路（Redis Streams），其他走冷路（ClickHouse 批量写入）
- **熔断器** — 可配置失败阈值、超时时间、半开状态探测
- **Schema Registry** — Protobuf 版本管理
- **租户路由** — 一致性哈希，支持多租户隔离
- **优雅关停** — SIGINT/SIGTERM 处理，排空队列后退出

## 架构

```
event_collector ──TCP──> gRPC Server ──> BoundedQueue ──> Pipeline Worker
                                                 │
                              Kafka Consumer ────┘
                                                 │
                                    ┌─────────────┤
                                    ▼             ▼
                              Schema Registry  Quality Pipeline
                              (proto 版本)    (去重→纠偏→反伪造→补全)
                                                      │
                                              Lambda Router
                                            ┌─────────┴─────────┐
                                            ▼                   ▼
                                      热路 (Redis)         冷路 (ClickHouse)
                                      click/view           其他事件
                                      Circuit Breaker      批量写入
                                      熔断降级
                                                      │
                                              Tenant Router
                                              (一致性哈希)
```

## 技术栈

- **语言**: C++17
- **RPC**: gRPC + Protobuf
- **消息队列**: Kafka (librdkafka)
- **缓存/实时**: Redis (redis++)
- **存储**: ClickHouse (clickhouse-cpp)
- **安全**: OpenSSL (HMAC-SHA256)
- **日志**: spdlog
- **构建**: CMake 3.16+

## 快速开始

### 依赖安装 (Ubuntu/WSL)

```bash
sudo apt-get install -y \
    libgrpc++-dev protobuf-compiler-grpc \
    librdkafka-dev \
    libspdlog-dev \
    libboost-all-dev \
    libhiredis-dev \
    libssl-dev
# clickhouse-cpp: https://github.com/ClickHouse/clickhouse-cpp
# redis-plus-plus: https://github.com/sewenew/redis-plus-plus
```

### 编译

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### 运行

```bash
./engine [grpc_addr] [kafka_brokers] [redis_addr] [clickhouse_host]
# 默认: ./engine 0.0.0.0:50051 localhost:9092 tcp://127.0.0.1:6379 localhost
```

### 压测

```bash
./bench_client [events] [threads]
# 示例: ./bench_client 100000 100
```

## Quality Pipeline 详解

| 阶段 | 功能 | 拒绝条件 |
|------|------|----------|
| 去重 | Redis SET / 内存滑动窗口 | 重复 event_id |
| 纠偏 | Watermark 中位数 ±1 小时 | 时间戳偏差过大 |
| 反伪造 | HMAC-SHA256 签名验证 | 签名不匹配 |
| 补全 | GeoIP 字段补全 | 不拒绝，仅补全 |

## 性能数据

| 事件数 | 线程 | QPS | P50 | P99 | 成功率 |
|--------|------|------|------|------|--------|
| 10,000 | 50 | 9,955 | 2.5ms | 5.4ms | 100% |
| 50,000 | 100 | 5,549 | 15.0ms | 61.9ms | 100% |
| 100,000 | 100 | 6,661 | 7.0ms | 52.6ms | 100% |

详见 [tests/PERFORMANCE_REPORT.md](tests/PERFORMANCE_REPORT.md)

## 测试

```bash
# 单元测试
./test_queue      # 队列测试 (5/5)
./test_pipeline   # Pipeline 测试 (8/8)

# 端到端测试
bash tests/test_e2e.sh
```

## 项目结构

```
event_stream_engine/
├── proto/event.proto              # Protobuf 定义
├── include/                       # 头文件
│   ├── queue.h                    # BoundedQueue
│   ├── grpc_server.h              # gRPC 服务端
│   ├── kafka_consumer.h           # Kafka 消费者
│   ├── quality_pipeline.h         # 四阶段质量管线
│   ├── lambda_router.h            # Lambda 热/冷路分流
│   ├── circuit_breaker.h          # 熔断器
│   ├── tenant_router.h            # 租户路由
│   └── clickhouse_writer.h        # ClickHouse 批量写入
├── src/                           # 实现文件
├── tests/
│   ├── test_queue.cpp             # 队列单元测试
│   ├── test_pipeline.cpp          # Pipeline 单元测试
│   ├── bench_client.cpp           # 压测客户端
│   ├── test_e2e.sh                # 端到端测试
│   └── PERFORMANCE_REPORT.md      # 性能报告
├── CMakeLists.txt
├── start.bat                      # Windows 启动脚本
└── README.md                      # 本文件
```

## License

MIT
