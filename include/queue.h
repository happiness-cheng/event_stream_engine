#pragma once
#include <optional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <cstddef>
#include <utility>
#include <chrono>

template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity = 10000)
        : capacity_(capacity) {}
    void push(const T& value) {
        std::unique_lock<std::mutex> lk(mtx_);
        not_full_.wait(lk, [this]{ return queue_.size() < capacity_; });
        queue_.push(value);
        lk.unlock();
        not_empty_.notify_one();
    }
    void push(T&& value) {
        std::unique_lock<std::mutex> lk(mtx_);
        not_full_.wait(lk, [this]{ return queue_.size() < capacity_; });
        queue_.push(std::move(value));
        lk.unlock();
        not_empty_.notify_one();
    }
    bool try_pop(T& data) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (queue_.empty()) return false;
        data = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return true;
    }
    std::optional<T> try_pop_for(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lk(mtx_);
        bool got = not_empty_.wait_for(lk, timeout, [this]{ return !queue_.empty(); });
        if (!got) return std::nullopt;
        T val = std::move(queue_.front());
        queue_.pop();
        lk.unlock();
        not_full_.notify_one();
        return val;
    }
    std::size_t size() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return queue_.size();
    }
    void drain() {
        std::lock_guard<std::mutex> lk(mtx_);
        while (!queue_.empty()) queue_.pop();
        not_full_.notify_all();
        not_empty_.notify_all();
        not_empty_.notify_all();  // 唤醒等待 not_empty_ 的消费者
    }
private:
    std::queue<T> queue_;
    mutable std::mutex mtx_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    std::size_t capacity_;
};
