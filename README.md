[简体中文](./README_zh.md) &nbsp;&nbsp;|&nbsp;&nbsp; **English**

# event_stream_engine

Real-time event stream processing engine. Receives events from gRPC/Kafka, passes them through a four-stage Quality Pipeline, then routes via Lambda architecture to Redis Streams (hot path) and ClickHouse (cold path).

## Architecture

```
event_collector ──TCP──> gRPC Server ──> BoundedQueue ──> Pipeline Worker
                                                 │
                              Kafka Consumer ────┘
                                                 │
                                    ┌─────────────┤
                                    ▼             ▼
                              Schema Registry  Quality Pipeline
                              (proto versions) (dedup→drift→anti-forgery→enrich)
                                                      │
                                              Lambda Router
                                            ┌─────────┴─────────┐
                                            ▼                   ▼
                                      Hot (Redis)          Cold (ClickHouse)
                                      click/view           other events
                                      Circuit Breaker      batch write
                                            │
                                      Tenant Router (consistent hashing)
```

## Quick Start

```bash
git clone https://github.com/happiness-cheng/event_stream_engine.git
cd event_stream_engine
mkdir build && cd build && cmake .. && make -j$(nproc)
./engine  # default: gRPC :50051, Kafka localhost:9092, Redis localhost:6379, ClickHouse localhost
```

## Quality Pipeline

| Stage | Function | Reject Condition |
|-------|----------|------------------|
| Dedup | Redis SET / in-memory sliding window | Duplicate event_id |
| Drift correction | Watermark median ±1 hour | Timestamp too far off |
| Anti-forgery | HMAC-SHA256 verification | Signature mismatch |
| Enrichment | GeoIP field enrichment | Never rejects |

## Performance

| Events | Threads | QPS | P50 | P99 | Success |
|--------|---------|------|------|------|--------|
| 10,000 | 50 | 9,955 | 2.5ms | 5.4ms | 100% |
| 100,000 | 100 | 6,661 | 7.0ms | 52.6ms | 100% |

## Tech Stack

C++17, gRPC, Protobuf, Kafka, Redis, ClickHouse, OpenSSL, spdlog, CMake

See [README_zh.md](./README_zh.md) for full documentation.

## License

MIT
