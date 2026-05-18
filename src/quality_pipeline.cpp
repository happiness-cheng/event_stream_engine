#include "quality_pipeline.h"
#include <openssl/hmac.h>
#include <algorithm>
#include <cstring>
#include <spdlog/spdlog.h>

static const size_t HMAC_LEN = 32;

QualityPipeline::QualityPipeline(const std::string& hmac_secret, sw::redis::Redis* redis)
    : hmac_secret_(hmac_secret), redis_(redis) {}

std::string QualityPipeline::compute_hmac(const std::string& data, const std::string& secret) {
    unsigned char result[EVP_MAX_MD_SIZE]; unsigned int len = 0;
    HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
         reinterpret_cast<const unsigned char*>(data.data()), static_cast<int>(data.size()), result, &len);
    return std::string(reinterpret_cast<char*>(result), HMAC_LEN);
}

std::string QualityPipeline::sign(const event::Event& ev, const std::string& secret) {
    std::string data = ev.event_id() + ev.user_id() + ev.event_type() + std::to_string(ev.ts()) + ev.payload();
    return compute_hmac(data, secret);
}

QualityResult QualityPipeline::process(event::Event& ev) {
    total_++;
    if (!dedup(ev.event_id())) { rejected_++; dedup_rejected_++; return {false, "dedup", "duplicate event_id: " + ev.event_id()}; }
    if (!watermark_check(ev.ts())) { rejected_++; return {false, "watermark", "timestamp too far from watermark"}; }
    if (!verify_hmac(ev)) { rejected_++; return {false, "hmac", "hmac verification failed"}; }
    enrich_geoip(ev);
    return {true, "", ""};
}

bool QualityPipeline::dedup(const std::string& event_id) {
    if (redis_) {
        try {
            auto added = redis_->sadd("dedup:set", event_id);
            if (added == 0) return false;
            static std::atomic<bool> expire_set{false};
            if (!expire_set.exchange(true)) redis_->expire("dedup:set", 3600);
            return true;
        } catch (const std::exception& e) { spdlog::warn("redis dedup failed, falling back to memory: {}", e.what()); }
    }
    std::lock_guard<std::mutex> lk(dedup_mtx_);
    if (seen_ids_.count(event_id)) return false;
    seen_ids_.insert(event_id); seen_order_.push_back(event_id);
    while (seen_order_.size() > MAX_SEEN) { seen_ids_.erase(seen_order_.front()); seen_order_.pop_front(); }
    return true;
}

bool QualityPipeline::watermark_check(int64_t ts) {
    std::lock_guard<std::mutex> lk(watermark_mtx_);
    recent_ts_.push_back(ts);
    if (recent_ts_.size() > 1000) recent_ts_.pop_front();
    if (recent_ts_.size() < 10) { watermark_ = ts; return true; }
    std::vector<int64_t> sorted(recent_ts_.begin(), recent_ts_.end());
    std::nth_element(sorted.begin(), sorted.begin() + sorted.size() / 2, sorted.end());
    watermark_ = sorted[sorted.size() / 2];
    return std::abs(ts - watermark_) <= 3600000;
}

bool QualityPipeline::verify_hmac(const event::Event& ev) {
    const std::string& payload = ev.payload();
    if (payload.size() < HMAC_LEN + 1) return false;
    std::string original_payload = payload.substr(0, payload.size() - HMAC_LEN);
    std::string received_hmac = payload.substr(payload.size() - HMAC_LEN);
    std::string data = ev.event_id() + ev.user_id() + ev.event_type() + std::to_string(ev.ts()) + original_payload;
    std::string expected_hmac = compute_hmac(data, hmac_secret_);
    if (received_hmac.size() != expected_hmac.size()) return false;
    volatile unsigned char diff = 0;
    for (size_t i = 0; i < expected_hmac.size(); ++i) diff |= received_hmac[i] ^ expected_hmac[i];
    return diff == 0;
}

void QualityPipeline::enrich_geoip(event::Event& ev) {
    const auto& platform = ev.platform();
    if (platform != "ios" && platform != "android" && platform != "web") spdlog::debug("unknown platform: {}", platform);
}