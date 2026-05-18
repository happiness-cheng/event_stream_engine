#include "clickhouse_writer.h"
#include <clickhouse/block.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/numeric.h>
#include <spdlog/spdlog.h>

ClickHouseWriter::ClickHouseWriter(const std::string& host, int port,
                                   const std::string& db)
    : client_([&]() {
        clickhouse::ClientOptions opts;
        opts.SetHost(host);
        opts.SetPort(port);
        opts.SetDefaultDatabase(db);
        return opts;
      }()) {
    try {
        client_.Execute(
            "CREATE TABLE IF NOT EXISTS events ("
            "event_id String, "
            "user_id String, "
            "event_type String, "
            "platform String, "
            "ts Int64"
            ") ENGINE = MergeTree() "
            "ORDER BY (ts, event_type)"
        );
        spdlog::info("clickhouse: table 'events' ready");
    } catch (const std::exception& e) {
        spdlog::warn("clickhouse: create table failed: {}", e.what());
    }
    flush_thread_ = std::thread(&ClickHouseWriter::flush_loop, this);
}

ClickHouseWriter::~ClickHouseWriter() {
    running_ = false;
    flush_cv_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();
    std::lock_guard<std::mutex> lk(mtx_);
    if (!buffer_.empty()) do_flush();
}

void ClickHouseWriter::write(const std::string& event_id,
                              const std::string& user_id,
                              const std::string& event_type,
                              const std::string& platform,
                              int64_t ts) {
    std::lock_guard<std::mutex> lk(mtx_);
    buffer_.push_back({event_id, user_id, event_type, platform, ts});
    if (buffer_.size() >= BATCH_SIZE) flush_cv_.notify_one();
}

int ClickHouseWriter::flush() {
    std::lock_guard<std::mutex> lk(mtx_);
    int n = static_cast<int>(buffer_.size());
    if (n > 0) do_flush();
    return n;
}

size_t ClickHouseWriter::pending() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return buffer_.size();
}

void ClickHouseWriter::do_flush() {
    if (buffer_.empty()) return;
    std::vector<EventRow> batch;
    batch.swap(buffer_);

    auto col_event_id = std::make_shared<clickhouse::ColumnString>();
    auto col_user_id = std::make_shared<clickhouse::ColumnString>();
    auto col_event_type = std::make_shared<clickhouse::ColumnString>();
    auto col_platform = std::make_shared<clickhouse::ColumnString>();
    auto col_ts = std::make_shared<clickhouse::ColumnInt64>();

    for (const auto& row : batch) {
        col_event_id->Append(row.event_id);
        col_user_id->Append(row.user_id);
        col_event_type->Append(row.event_type);
        col_platform->Append(row.platform);
        col_ts->Append(row.ts);
    }

    clickhouse::Block block;
    block.AppendColumn("event_id", col_event_id);
    block.AppendColumn("user_id", col_user_id);
    block.AppendColumn("event_type", col_event_type);
    block.AppendColumn("platform", col_platform);
    block.AppendColumn("ts", col_ts);

    try {
        client_.Insert("events", block);
        spdlog::debug("clickhouse: flushed {} events", batch.size());
    } catch (const std::exception& e) {
        spdlog::error("clickhouse: insert failed: {}", e.what());
    }
}

void ClickHouseWriter::flush_loop() {
    while (running_) {
        std::unique_lock<std::mutex> lk(flush_mtx_);
        flush_cv_.wait_for(lk, std::chrono::milliseconds(FLUSH_INTERVAL_MS),
                           [this]{ return !running_.load(); });
        std::lock_guard<std::mutex> buf_lk(mtx_);
        if (!buffer_.empty()) do_flush();
    }
}