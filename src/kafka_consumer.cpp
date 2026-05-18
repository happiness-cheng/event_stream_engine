#include "kafka_consumer.h"
#include <spdlog/spdlog.h>
#include <librdkafka/rdkafkacpp.h>

KafkaConsumer::KafkaConsumer(const std::string& brokers, const std::string& topic, const std::string& group_id, EventCallback callback)
    : brokers_(brokers), topic_(topic), group_id_(group_id), callback_(std::move(callback)) {
    std::string errstr; conf_ = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf_->set("bootstrap.servers", brokers_, errstr); conf_->set("group.id", group_id_, errstr);
    conf_->set("auto.offset.reset", "earliest", errstr); conf_->set("enable.auto.commit", "true", errstr);
}

KafkaConsumer::~KafkaConsumer() { stop(); delete conf_; }

void KafkaConsumer::start(int num_threads) {
    std::string errstr; consumer_ = RdKafka::KafkaConsumer::create(conf_, errstr);
    if (!consumer_) { spdlog::error("Failed to create Kafka consumer: {}", errstr); return; }
    RdKafka::ErrorCode err = consumer_->subscribe({topic_});
    if (err != RdKafka::ERR_NO_ERROR) { spdlog::error("Failed to subscribe to {}: {}", topic_, RdKafka::err2str(err)); return; }
    running_ = true;
    for (int i = 0; i < num_threads; ++i) threads_.emplace_back(&KafkaConsumer::consume_loop, this);
    spdlog::info("Kafka consumer started, topic={}, group={}, threads={}", topic_, group_id_, num_threads);
}

void KafkaConsumer::stop() {
    running_ = false;
    for (auto& t : threads_) { if (t.joinable()) t.join(); }
    threads_.clear();
    if (consumer_) { consumer_->close(); delete consumer_; consumer_ = nullptr; }
    spdlog::info("Kafka consumer stopped");
}

void KafkaConsumer::consume_loop() {
    while (running_) {
        RdKafka::Message* msg = consumer_->consume(5000);
        if (!msg) continue;
        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR: { std::string data(static_cast<const char*>(msg->payload()), msg->len()); callback_(data); break; }
            case RdKafka::ERR__PARTITION_EOF: case RdKafka::ERR__TIMED_OUT: break;
            default: spdlog::warn("Kafka consume error: {}", msg->errstr()); break;
        }
        delete msg;
    }
}