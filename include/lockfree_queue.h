#pragma once
#include <vector>
#include <atomic>
#include <chrono>
#include <optional>
#include <string>
#include <thread>

// Lock-free MPMC bounded queue using ring buffer + sequence numbers
// Based on the LMAX Disruptor pattern
template <typename T>
class LockFreeQueue {
public:
    explicit LockFreeQueue(size_t capacity = 65536)
        : buffer_(next_power_of_two(capacity)),
          mask_(buffer_.size() - 1),
          head_(0),
          tail_(0) {
        for (size_t i = 0; i < buffer_.size(); ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    bool try_push(const T& value) {
        //alignas(64) std::atomic<size_t> tail_;
        size_t pos = tail_.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = buffer_[pos & mask_];
            size_t seq = slot.sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (diff == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed)) {
                    slot.data = value;
                    slot.sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                return false;  // full
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    bool try_push(T&& value) {
        size_t pos = tail_.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = buffer_[pos & mask_];
            size_t seq = slot.sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (diff == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed)) {
                    slot.data = std::move(value);
                    slot.sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                return false;
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    // Blocking push: spin with yield to avoid CPU starvation
    void push(const T& value) {
        int spins = 0;
        while (!try_push(value)) {
            if (++spins > 100) {
                std::this_thread::yield();
                spins = 0;
            }
        }
    }

    void push(T&& value) {
        int spins = 0;
        while (!try_push(std::move(value))) {
            if (++spins > 100) {
                std::this_thread::yield();
                spins = 0;
            }
        }
    }

    bool try_pop(T& value) {
        size_t pos = head_.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = buffer_[pos & mask_];
            size_t seq = slot.sequence.load(std::memory_order_acquire);
            intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
            if (diff == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed)) {
                    value = std::move(slot.data);
                    slot.sequence.store(pos + mask_ + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                return false;  // empty
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }
    }

    std::optional<T> try_pop_for(std::chrono::milliseconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        T value;
        while (std::chrono::steady_clock::now() < deadline) {
            if (try_pop(value)) return value;
        }
        return std::nullopt;
    }

    size_t size() const {
        // Approximate, not thread-safe
        size_t t = tail_.load(std::memory_order_relaxed);
        size_t h = head_.load(std::memory_order_relaxed);
        return t > h ? t - h : 0;
    }

private:
    static size_t next_power_of_two(size_t v) {
        size_t n = 1;
        while (n < v) n <<= 1;
        return n;
    }

    struct Slot {
        std::atomic<size_t> sequence;
        T data;
    };

    std::vector<Slot> buffer_;
    size_t mask_;
    alignas(64) std::atomic<size_t> head_;  // cache-line aligned
    alignas(64) std::atomic<size_t> tail_;
};
