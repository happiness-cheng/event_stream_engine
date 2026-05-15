#include "lambda_router.h"
#include <spdlog/spdlog.h>

LambdaRouter::LambdaRouter(const std::string& redis_addr, ClickHouseWriter* ch_writer) : ch_writer_(ch_writer) {
    try { redis_ = std::make_unique<sw::redis::Redis>(redis_addr); redis_->ping(); spdlog::info("lambda_router: Redis connected"); }
    catch (const std::exception& e) { spdlog::warn("lambda_router: Redis not available: {}", e.what()); redis_.reset(); }
}

bool LambdaRouter::is_hot_event(const std::string& event_type) const {
    return event_type == "click" || event_type == "view" || event_type == "purchase" || event_type == "add_to_cart";
}

std::string LambdaRouter::route(const event::Event& ev) {
    if (is_hot_event(ev.event_type())) { std::lock_guard<std::mutex> lk(stats_mtx_); stats_.hot_count++; return "hot"; }
    else { std::lock_guard<std::mutex> lk(stats_mtx_); stats_.cold_count++; return "cold"; }
}

bool LambdaRouter::write_hot(const event::Event& ev) {
    if (!redis_ || !redis_cb_.allow_request()) { write_cold(ev); std::lock_guard<std::mutex> lk(stats_mtx_); stats_.hot_fallback_cold++; return false; }
    try {
        using Attrs = std::vector<std::pair<std::string, std::string>>;
        Attrs attrs = {{"event_id", ev.event_id()}, {"user_id", ev.user_id()}, {"event_type", ev.event_type()}, {"platform", ev.platform()}, {"ts", std::to_string(ev.ts())}};
        redis_->xadd("hot_events", "*", attrs.begin(), attrs.end()); redis_cb_.record_success(); return true;
    } catch (const std::exception& e) {
        redis_cb_.record_failure(); spdlog::warn("lambda_router: Redis write failed: {}", e.what());
        write_cold(ev); std::lock_guard<std::mutex> lk(stats_mtx_); stats_.hot_fallback_cold++; return false;
    }
}

void LambdaRouter::write_cold(const event::Event& ev) {
    if (ch_writer_) { ch_writer_->write(ev.event_id(), ev.user_id(), ev.event_type(), ev.platform(), ev.ts()); }
    else { std::string data; ev.SerializeToString(&data); std::lock_guard<std::mutex> lk(cold_mtx_); cold_buffer_.push_back(std::move(data)); }
}

int LambdaRouter::flush_cold() {
    int flushed = 0; if (ch_writer_) flushed = ch_writer_->flush();
    std::lock_guard<std::mutex> lk(cold_mtx_); flushed += static_cast<int>(cold_buffer_.size()); cold_buffer_.clear(); return flushed;
}

RouteStats LambdaRouter::stats() const { std::lock_guard<std::mutex> lk(stats_mtx_); return stats_; }
