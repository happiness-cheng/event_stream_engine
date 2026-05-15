#include "grpc_server.h"
#include "kafka_consumer.h"
#include "schema_registry.h"
#include "quality_pipeline.h"
#include "lambda_router.h"
#include "circuit_breaker.h"
#include "tenant_router.h"
#include "clickhouse_writer.h"
#include "event.pb.h"
#include <thread>
#include <atomic>
#include <csignal>
#include <spdlog/spdlog.h>

static std::atomic<bool> g_running{true};
void signal_handler(int sig) { spdlog::info("Received signal {}, shutting down...", sig); g_running = false; }

int main(int argc, char* argv[]) {
    try {
        std::string grpc_addr = "0.0.0.0:50051"; std::string kafka_brokers = "localhost:9092";
        std::string kafka_topic = "event_stream"; std::string kafka_group = "event_engine_group";
        std::string redis_addr = "tcp://127.0.0.1:6379"; std::string ch_host = "localhost";
        if (argc > 1) grpc_addr = argv[1]; if (argc > 2) kafka_brokers = argv[2];
        if (argc > 3) redis_addr = argv[3]; if (argc > 4) ch_host = argv[4];

        BoundedQueue<std::string> queue(10000);
        sw::redis::Redis redis(redis_addr);
        SchemaRegistry schema;
        QualityPipeline pipeline("event_engine_hmac_key", &redis);
        ClickHouseWriter ch_writer(ch_host);
        LambdaRouter router(redis_addr, &ch_writer);
        CircuitBreaker cb(5, std::chrono::seconds(30), 2);
        TenantRouter tenant_router;
        tenant_router.add_tenant("tenant_a"); tenant_router.add_tenant("tenant_b"); tenant_router.add_tenant("tenant_c");

        std::atomic<uint64_t> total_routed{0}, hot_routed{0}, cold_routed{0}, pipeline_rejected{0};

        auto pipeline_worker = [&]() {
            while (g_running) {
                auto data = queue.try_pop_for(std::chrono::milliseconds(100));
                if (!data) continue;
                event::Event ev;
                if (!schema.parse(*data, schema.latest_version(), ev)) { pipeline_rejected++; continue; }
                auto result = pipeline.process(ev);
                if (!result.passed) { pipeline_rejected++; continue; }
                std::string tenant_node = tenant_router.route(ev.user_id());
                std::string path = router.route(ev);
                if (path == "hot") { router.write_hot(ev); hot_routed++; }
                else { router.write_cold(ev); cold_routed++; }
                total_routed++;
            }
        };

        KafkaConsumer kafka(kafka_brokers, kafka_topic, kafka_group, [&queue](const std::string& data) { queue.push(data); });
        GrpcServer grpc(grpc_addr, queue);
        std::signal(SIGINT, signal_handler); std::signal(SIGTERM, signal_handler);

        int num_workers = 2; std::vector<std::thread> workers;
        for (int i = 0; i < num_workers; ++i) workers.emplace_back(pipeline_worker);
        std::thread kafka_thread([&kafka]() { kafka.start(1); });
        std::thread grpc_thread([&grpc]() { grpc.start(); });

        spdlog::info("engine ready: grpc={} kafka={}:{} redis={} clickhouse={} workers={}", grpc_addr, kafka_brokers, "9092", redis_addr, ch_host, num_workers);
        while (g_running) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            auto s = router.stats();
            spdlog::info("stats: routed={} hot={} cold={} rejected={} queue={} ch_pending={}", total_routed.load(), hot_routed.load(), cold_routed.load(), pipeline_rejected.load(), queue.size(), ch_writer.pending());
        }

        spdlog::info("draining queue ({} events pending)...", queue.size());
        grpc.stop(); kafka.stop(); grpc_thread.join(); kafka_thread.join();
        for (auto& w : workers) w.join();
        int cold_flushed = router.flush_cold(); auto s = router.stats();
        spdlog::info("final: routed={} hot={} cold={} fallback={} rejected={} flushed={}", total_routed.load(), hot_routed.load(), cold_routed.load(), s.hot_fallback_cold, pipeline_rejected.load(), cold_flushed);
        spdlog::info("shutdown complete"); return 0;
    } catch (const std::exception& e) { spdlog::error("fatal: {}", e.what()); return 1; }
}
