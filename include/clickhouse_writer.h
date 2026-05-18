#pragma once
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <clickhouse/client.h>

class ClickHouseWriter {
public:
    ClickHouseWriter(const std::string& host = "localhost", int port = 9000,
                     const std::string& db = "default");
    ~ClickHouseWriter();

    void write(const std::string& event_id, const std::string& user_id,
               const std::string& event_type, const std::string& platform,
               int64_t ts);

    int flush();
    size_t pending() const;

private:
    void do_flush();
    void flush_loop();

    struct EventRow {
        std::string event_id;
        std::string user_id;
        std::string event_type;
        std::string platform;
        int64_t ts;
    };

    clickhouse::Client client_;
    std::vector<EventRow> buffer_;
    mutable std::mutex mtx_;

    std::thread flush_thread_;
    std::atomic<bool> running_{true};
    std::condition_variable flush_cv_;
    std::mutex flush_mtx_;

    static constexpr size_t BATCH_SIZE = 1000;
    static constexpr int FLUSH_INTERVAL_MS = 200;
};