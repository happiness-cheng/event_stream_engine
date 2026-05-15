#include "circuit_breaker.h"
#include <spdlog/spdlog.h>

CircuitBreaker::CircuitBreaker(int failure_threshold, std::chrono::seconds timeout, int success_threshold)
    : failure_threshold_(failure_threshold), timeout_(timeout), success_threshold_(success_threshold) {}

bool CircuitBreaker::allow_request() {
    std::lock_guard<std::mutex> lk(mtx_);
    switch (state_) {
        case CircuitState::CLOSED: return true;
        case CircuitState::OPEN: {
            auto elapsed = std::chrono::steady_clock::now() - last_failure_time_;
            if (elapsed >= timeout_) { transition_to(CircuitState::HALF_OPEN); return true; }
            return false;
        }
        case CircuitState::HALF_OPEN: return true;
    }
    return false;
}

void CircuitBreaker::record_success() {
    std::lock_guard<std::mutex> lk(mtx_);
    total_successes_++;
    if (state_ == CircuitState::HALF_OPEN) {
        success_count_++;
        if (success_count_ >= success_threshold_) transition_to(CircuitState::CLOSED);
    }
}

void CircuitBreaker::record_failure() {
    std::lock_guard<std::mutex> lk(mtx_);
    total_failures_++; failure_count_++;
    last_failure_time_ = std::chrono::steady_clock::now();
    if (state_ == CircuitState::CLOSED && failure_count_ >= failure_threshold_) transition_to(CircuitState::OPEN);
    else if (state_ == CircuitState::HALF_OPEN) transition_to(CircuitState::OPEN);
}

void CircuitBreaker::transition_to(CircuitState new_state) {
    auto old = state_; state_ = new_state; failure_count_ = 0; success_count_ = 0;
    const char* names[] = {"CLOSED", "OPEN", "HALF_OPEN"};
    spdlog::info("circuit_breaker: {} -> {}", names[static_cast<int>(old)], names[static_cast<int>(new_state)]);
}
