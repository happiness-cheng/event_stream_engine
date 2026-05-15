#pragma once
#include <string>
#include <vector>
#include <functional>
#include <mutex>
#include <sw/redis++/redis++.h>
#include "circuit_breaker.h"
#include "clickhouse_writer.h"
#include "event.pb.h"

struct RouteStats {
    uint64_t hot_count = 0;
    uint64_t cold_count = 0;
    uint64_t hot_fallback_cold = 0;
};

class LambdaRouter {
public:
    LambdaRouter(const std::string& redis_addr = "tcp://127.0.0.1:6379",
                 ClickHouseWriter* ch_writer = nullptr);
    std::string route(const event::Event& ev);
    bool write_hot(const event::Event& ev);
    void write_cold(const event::Event& ev);
    int flush_cold();
    RouteStats stats() const;
private:
    bool is_hot_event(const std::string& event_type) const;
    std::unique_ptr<sw::redis::Redis> redis_;
    CircuitBreaker redis_cb_;
    ClickHouseWriter* ch_writer_;
    std::vector<std::string> cold_buffer_;
    mutable std::mutex cold_mtx_;
    RouteStats stats_;
    mutable std::mutex stats_mtx_;
};
