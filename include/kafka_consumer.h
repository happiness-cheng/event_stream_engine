#pragma once
#include <string>
#include <functional>
#include <atomic>
#include <thread>
#include <librdkafka/rdkafkacpp.h>

using EventCallback = std::function<void(const std::string&)>;

class KafkaConsumer {
public:
    KafkaConsumer(const std::string& brokers, const std::string& topic,
                  const std::string& group_id, EventCallback callback);
    ~KafkaConsumer();
    void start(int num_threads = 1);
    void stop();
private:
    void consume_loop();
    std::string brokers_;
    std::string topic_;
    std::string group_id_;
    EventCallback callback_;
    std::atomic<bool> running_{false};
    std::vector<std::thread> threads_;
    RdKafka::KafkaConsumer* consumer_ = nullptr;
    RdKafka::Conf* conf_ = nullptr;
};
