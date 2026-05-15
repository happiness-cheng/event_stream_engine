#pragma once
#include <atomic>
#include <chrono>
#include <mutex>

enum class CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN
};

class CircuitBreaker {
public:
    CircuitBreaker(int failure_threshold = 5,
                   std::chrono::seconds timeout = std::chrono::seconds(30),
                   int success_threshold = 2);

    bool allow_request();
    void record_success();
    void record_failure();

    CircuitState state() const { return state_; }
    uint64_t total_failures() const { return total_failures_; }
    uint64_t total_successes() const { return total_successes_; }

private:
    void transition_to(CircuitState new_state);
    CircuitState state_ = CircuitState::CLOSED;
    int failure_threshold_;
    std::chrono::seconds timeout_;
    int success_threshold_;
    int failure_count_ = 0;
    int success_count_ = 0;
    std::chrono::steady_clock::time_point last_failure_time_;
    std::atomic<uint64_t> total_failures_{0};
    std::atomic<uint64_t> total_successes_{0};
    mutable std::mutex mtx_;
};
