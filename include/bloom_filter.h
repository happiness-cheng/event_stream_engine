#pragma once
#include <vector>
#include <string>
#include <cstdint>
#include <mutex>

class BloomFilter {
public:
    explicit BloomFilter(uint64_t capacity = 1000000, double false_positive_rate = 0.01);

    void add(const std::string& key);
    bool might_contain(const std::string& key) const;
    uint64_t size() const { return count_; }

private:
    uint64_t hash1(const std::string& key) const;
    uint64_t hash2(const std::string& key) const;

    std::vector<bool> bits_;
    uint64_t num_bits_;
    uint64_t num_hashes_;
    uint64_t count_ = 0;
    mutable std::mutex mtx_;
};
