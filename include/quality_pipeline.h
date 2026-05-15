#pragma once
#include <string>
#include <vector>
#include <deque>
#include <mutex>
#include <cstdint>
#include <sw/redis++/redis++.h>
#include "event.pb.h"

struct QualityResult {
    bool passed;
    std::string stage;
    std::string reason;
};

class QualityPipeline {
public:
    QualityPipeline(const std::string& hmac_secret = "default_secret_key",
                    sw::redis::Redis* redis = nullptr);
    QualityResult process(event::Event& ev);
    static std::string sign(const event::Event& ev, const std::string& secret);
    uint64_t total_processed() const { return total_; }
    uint64_t total_rejected() const { return rejected_; }
    uint64_t dedup_rejected() const { return dedup_rejected_; }
private:
    bool dedup(const std::string& event_id);
    bool watermark_check(int64_t ts);
    bool verify_hmac(const event::Event& ev);
    void enrich_geoip(event::Event& ev);
    static std::string compute_hmac(const std::string& data, const std::string& secret);
    std::string hmac_secret_;
    sw::redis::Redis* redis_;
    std::unordered_set<std::string> seen_ids_;
    std::deque<std::string> seen_order_;
    mutable std::mutex dedup_mtx_;
    static constexpr size_t MAX_SEEN = 100000;
    std::deque<int64_t> recent_ts_;
    int64_t watermark_ = 0;
    mutable std::mutex watermark_mtx_;
    std::atomic<uint64_t> total_{0};
    std::atomic<uint64_t> rejected_{0};
    std::atomic<uint64_t> dedup_rejected_{0};
};
