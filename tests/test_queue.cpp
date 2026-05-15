#include "queue.h"
#include <cassert>
#include <thread>
#include <vector>
#include <atomic>
#include <iostream>
#include <chrono>

void test_push_pop() {
    BoundedQueue<int> q(5); q.push(1); q.push(2);
    int v; assert(q.try_pop(v) && v == 1); assert(q.try_pop(v) && v == 2); assert(!q.try_pop(v));
    std::cout << "PASS: push_pop" << std::endl;
}

void test_blocking_pop() {
    BoundedQueue<int> q(5);
    std::thread producer([&q]() { std::this_thread::sleep_for(std::chrono::milliseconds(50)); q.push(42); });
    auto result = q.try_pop_for(std::chrono::milliseconds(200));
    assert(result.has_value() && result.value() == 42); producer.join();
    std::cout << "PASS: blocking_pop" << std::endl;
}

void test_capacity() {
    BoundedQueue<int> q(3); q.push(1); q.push(2); q.push(3); assert(q.size() == 3);
    std::thread pusher([&q]() { q.push(4); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50)); assert(pusher.joinable());
    int v; q.try_pop(v); pusher.join(); assert(q.size() == 3);
    std::cout << "PASS: capacity" << std::endl;
}

void test_drain() {
    BoundedQueue<int> q(10); for (int i = 0; i < 10; ++i) q.push(i);
    assert(q.size() == 10); q.drain(); assert(q.size() == 0);
    std::cout << "PASS: drain" << std::endl;
}

void test_multithreaded() {
    BoundedQueue<int> q(1000); const int count = 5000; std::atomic<int> popped{0};
    std::thread producer([&q]() { for (int i = 0; i < count; ++i) q.push(i); });
    std::thread consumer([&]() { int v; for (int i = 0; i < count; ++i) { while (!q.try_pop_for(std::chrono::milliseconds(100))) {} popped++; } });
    producer.join(); consumer.join(); assert(popped == count);
    std::cout << "PASS: multithreaded (" << count << " items)" << std::endl;
}

int main() {
    test_push_pop(); test_blocking_pop(); test_capacity(); test_drain(); test_multithreaded();
    std::cout << "All queue tests passed!" << std::endl; return 0;
}
