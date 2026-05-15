#pragma once
#include <string>
#include <vector>
#include <mutex>
#include <clickhouse/client.h>

class ClickHouseWriter {
public:
    ClickHouseWriter(const std::string& host = "localhost", int port = 9000,
                     const std::string& db = "default");
    void write(const std::string& event_id, const std::string& user_id,
               const std::string& event_type, const std::string& platform,
               int64_t ts);
    int flush();
    size_t pending() const;
private:
    void do_flush();
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
    static constexpr size_t BATCH_SIZE = 100;
};
