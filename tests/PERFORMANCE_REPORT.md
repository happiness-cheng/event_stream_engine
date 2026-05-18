# event_stream_engine 性能测试报告

## 测试环境

| 项目 | 值 |
|------|-----|
| 操作系统 | Linux WSL2 6.6.87.2-microsoft-standard |
| CPU | AMD Ryzen 5 5600U (6核/12线程) |
| 内存 | 6.7GB（可用 5.0GB） |
| 架构 | x86_64，小端序 |
| 部署方式 | 客户端与服务端同机部署 |
| 接口 | gRPC（protobuf 序列化） |
| Pipeline 工作线程 | 2 |

## 测试说明

event_stream_engine 通过 gRPC 接收事件，经过 Quality Pipeline 四阶段处理后分流：
- **Quality Pipeline**：Redis SADD 去重（1h TTL，Redis 不可用降级内存滑动窗口）→ Watermark 中位数纠偏（±1h 容忍）→ HMAC-SHA256 常量时间签名验证 → GeoIP 字段补全
- **Lambda Router**：click/view/purchase/add_to_cart → 热路（Redis Streams XADD），其余 → 冷路（ClickHouse 批量写入）
- **Circuit Breaker**：Redis 连接失败时自动熔断，热路降级到冷路（5 次失败触发、30s 超时半开探测、2 次成功恢复）
- **Tenant Router**：一致性哈希多租户路由（3 租户，150 虚拟节点/租户）

## 测试 A：事件梯度 + 延迟分布（全开：Redis + Kafka + ClickHouse）

| 事件数 | 线程 | QPS | Avg | P50 | P95 | P99 | P99.9 | Max | 成功/总数 |
|--------|------|------|------|------|------|------|------|------|-----------|
| 1,000 | 10 | 998 | 1.2ms | 0.9ms | 1.3ms | 21.5ms | 22.1ms | 22.1ms | 1,000/1,000 |
| 1,000 | 50 | 995 | 3.1ms | 2.8ms | 7.3ms | 8.6ms | 8.9ms | 8.9ms | 1,000/1,000 |
| 5,000 | 20 | 4,990 | 1.6ms | 1.4ms | 2.3ms | 3.7ms | 17.9ms | 18.2ms | 5,000/5,000 |
| 5,000 | 50 | 4,976 | 3.4ms | 3.2ms | 5.3ms | 6.1ms | 7.0ms | 9.5ms | 5,000/5,000 |
| 10,000 | 20 | 9,980 | 1.4ms | 1.4ms | 1.9ms | 2.3ms | 12.8ms | 13.2ms | 10,000/10,000 |
| 10,000 | 50 | 4,985 | 7.4ms | 3.3ms | 18.6ms | 33.9ms | 50.8ms | 68.5ms | 10,000/10,000 |
| 10,000 | 100 | 4,969 | 17.9ms | 7.8ms | 38.1ms | 71.9ms | 106.8ms | 141.3ms | 10,000/10,000 |
| 50,000 | 50 | 3,331 | 14.2ms | 17.0ms | 20.7ms | 36.1ms | 53.1ms | 87.5ms | 50,000/50,000 |
| 50,000 | 100 | 3,121 | 30.8ms | 34.4ms | 40.4ms | 70.8ms | 103.2ms | 162.6ms | 50,000/50,000 |
| 100,000 | 50 | 3,123 | 15.6ms | 17.0ms | 20.3ms | 35.6ms | 52.5ms | 75.7ms | 100,000/100,000 |
| 100,000 | 100 | 3,028 | 32.7ms | 34.3ms | 41.5ms | 71.6ms | 104.4ms | 176.0ms | 100,000/100,000 |
| 100,000 | 200 | 3,122 | 63.0ms | 69.3ms | 79.8ms | 143.4ms | 210.6ms | 343.7ms | 100,000/100,000 |

**结论**：
- **小规模（1K-10K 低并发）**：QPS 稳定在 ~5,000-10,000，P50 稳定在 1-3ms
- **大规模（50K-100K）**：QPS 稳定在 ~3,100，P50 升至 17-69ms。Pipeline 处理（Quality Pipeline 四阶段 + Redis XADD + ClickHouse 批量写入）是主要瓶颈
- **高并发退化（50→100 线程）**：10K 事件 QPS 从 4,985 降至 4,969（微降），P50 从 3.3ms 升至 7.8ms（2x），锁竞争加剧
- **全量零丢包**：12 组测试全部 100% 成功率

## 测试 B：单元测试

```
test_queue: PASS（push_pop / blocking_pop / capacity / drain / multithreaded 5000 items）
test_pipeline: PASS（schema_registry / quality_pipeline / hmac_sign_verify / circuit_breaker / tenant_router / consistent_hash_stability / lambda_router / end_to_end_pipeline）
```

共 13 个测试用例全部通过。

## 发现的问题

1. **Pipeline 处理是瓶颈**：QPS 从接入层的 ~10,000（20 线程）降至大规模场景的 ~3,000，原因：
   - Quality Pipeline 四阶段处理（Redis SADD + Watermark + HMAC + GeoIP）开销
   - 热路事件需 Redis XADD 写入
   - 冷路事件需 ClickHouse 批量写入（100 条/批次）
2. **Kafka Consumer 后台消耗 CPU**：`consume_loop` 持续调用 `consumer_->consume(1000)`，即使没有消息也在轮询，占用 CPU 时间片
3. **ClickHouse 批量写入阻塞**：冷路事件积攒到 100 条时同步写入 ClickHouse，阻塞 Pipeline Worker
4. **Redis 热路引入额外延迟**：P99 毛刺（21.5ms@1K、33.9ms@10K）来自 Redis XADD 写入延迟
5. **10K/50 QPS 骤降**：从 ~10,000 降至 ~5,000，50 线程竞争 2 个 Pipeline Worker 的 BoundedQueue 锁

## 核心指标

| 指标 | 值 |
|------|-----|
| 峰值 QPS（低并发） | 9,980（10K 事件 / 20 线程） |
| 稳态 QPS（大规模） | ~3,100（100K 事件） |
| P50 延迟（低并发） | 0.9-1.4ms |
| P50 延迟（高并发） | 34-69ms（100-200 线程） |
| P99 延迟（低并发） | 2.3-3.7ms |
| P99 延迟（高并发） | 72-143ms |
| 成功率 | 100%（所有测试） |
| 单元测试 | 13/13 通过 |
