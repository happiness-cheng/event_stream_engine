#include "bloom_filter.h"
#include <cmath>
#include <cstring>
// uint64_t hash1(const std::string& key) const;
//     uint64_t hash2(const std::string& key) const;

//     std::vector<bool> bits_;
//     uint64_t num_bits_;
//     uint64_t num_hashes_;
//     uint64_t count_ = 0;
//     mutable std::mutex mtx_;
BloomFilter::BloomFilter(uint64_t capacity, double false_positive_rate) {//初始化，设定几个变量但是为什么要这么设定呢，有什么特殊的用意吗？
    num_bits_ = static_cast<uint64_t>(
        -static_cast<double>(capacity) * std::log(false_positive_rate) / (std::log(2) * std::log(2)));
    if (num_bits_ < 64) num_bits_ = 64;
    bits_.resize(num_bits_, false);

    num_hashes_ = static_cast<uint64_t>(
        static_cast<double>(num_bits_) / capacity * std::log(2));
    if (num_hashes_ < 1) num_hashes_ = 1;
    if (num_hashes_ > 20) num_hashes_ = 20;
}

uint64_t BloomFilter::hash1(const std::string& key) const {//这是在干嘛这些运算有什么特殊的用意吗？
    uint64_t h = 0xcbf29ce484222325ULL;
    for (char c : key) {
        h ^= static_cast<uint64_t>(c);
        h *= 0x100000001b3ULL;
    }
    return h % num_bits_;
}

uint64_t BloomFilter::hash2(const std::string& key) const {//同上
    uint64_t h = 0x517cc1b727220a95ULL;
    for (char c : key) {
        h ^= static_cast<uint64_t>(c);
        h *= 0x2545f4914f6cdd1dULL;
    }
    return h % num_bits_;
}

void BloomFilter::add(const std::string& key) {//看不懂，运算有什么特殊的用意吗？
    std::lock_guard<std::mutex> lk(mtx_);
    uint64_t h1 = hash1(key);
    uint64_t h2 = hash2(key);
    for (uint64_t i = 0; i < num_hashes_; ++i) {
        uint64_t idx = (h1 + i * h2) % num_bits_;
        bits_[idx] = true;
    }
    count_++;
}

bool BloomFilter::might_contain(const std::string& key) const {//同上
    std::lock_guard<std::mutex> lk(mtx_);
    uint64_t h1 = hash1(key);
    uint64_t h2 = hash2(key);
    for (uint64_t i = 0; i < num_hashes_; ++i) {
        uint64_t idx = (h1 + i * h2) % num_bits_;
        if (!bits_[idx]) return false;
    }
    return true;
}
