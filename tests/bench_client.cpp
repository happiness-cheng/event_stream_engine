#include "event.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <openssl/hmac.h>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <algorithm>
#include <iomanip>

using grpc::Channel; using grpc::ClientContext; using grpc::Status;
using event::Event; using event::Ack; using event::EventBatch; using event::EventStream;

static std::string get_hmac_secret() {
    const char* env = std::getenv("ENGINE_HMAC_KEY");
    return env ? env : "event_engine_hmac_key";
}
static const std::string HMAC_SECRET = get_hmac_secret();

std::string compute_hmac(const std::string& data) {
    unsigned char result[EVP_MAX_MD_SIZE]; unsigned int len = 0;
    HMAC(EVP_sha256(), HMAC_SECRET.data(), static_cast<int>(HMAC_SECRET.size()),
         reinterpret_cast<const unsigned char*>(data.data()), static_cast<int>(data.size()), result, &len);
    return std::string(reinterpret_cast<char*>(result), 32);
}

void sign_event(Event& ev) {
    std::string data = ev.event_id() + ev.user_id() + ev.event_type() + std::to_string(ev.ts()) + ev.payload();
    ev.set_payload(ev.payload() + compute_hmac(data));
}

class BenchClient {
public:
    explicit BenchClient(std::shared_ptr<Channel> channel) : stub_(EventStream::NewStub(channel)) {}
    int64_t send_event(const Event& ev) {
        Ack reply; ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        auto start = std::chrono::steady_clock::now();
        Status status = stub_->SendEvent(&ctx, ev, &reply);
        auto end = std::chrono::steady_clock::now();
        if (!status.ok() || !reply.success()) return -1;
        return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    }
private:
    std::unique_ptr<EventStream::Stub> stub_;
};

struct LatencyStats { int64_t p50, p95, p99, p999, max; double avg; };

LatencyStats calc_stats(std::vector<int64_t>& latencies) {
    LatencyStats s{};
    if (latencies.empty()) return s;
    std::sort(latencies.begin(), latencies.end());
    size_t n = latencies.size(); LatencyStats s;
    s.p50 = latencies[n*50/100]; s.p95 = latencies[n*95/100]; s.p99 = latencies[n*99/100];
    s.p999 = latencies[std::min(n-1, n*999/1000)]; s.max = latencies.back();
    double sum = 0; for (auto l : latencies) sum += l; s.avg = sum / n; return s;
}

int main(int argc, char* argv[]) {
    int total_events = 10000, num_threads = 50;
    if (argc > 1) total_events = std::stoi(argv[1]);
    if (argc > 2) num_threads = std::stoi(argv[2]);
    auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
    std::atomic<int> success{0}, fail{0}, sent{0};
    std::vector<std::vector<int64_t>> thread_latencies(num_threads);
    auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    int events_per_thread = total_events / num_threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            BenchClient client(channel); std::mt19937_64 rng(t + 42);
            std::uniform_int_distribution<int> type_dist(0, 4);
            const char* types[] = {"click", "view", "signup", "purchase", "page_view"};
            const char* platforms[] = {"ios", "android", "web"};
            auto& latencies = thread_latencies[t]; latencies.reserve(events_per_thread);
            for (int i = 0; i < events_per_thread; ++i) {
                Event ev;
                ev.set_event_id("bench_" + std::to_string(t) + "_" + std::to_string(i));
                ev.set_user_id("user_" + std::to_string(rng() % 1000));
                ev.set_event_type(types[type_dist(rng)]); ev.set_platform(platforms[rng() % 3]);
                ev.set_ts(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
                ev.set_payload("bench_data_" + std::to_string(i));
                sign_event(ev);
                int64_t latency = client.send_event(ev);
                if (latency >= 0) { latencies.push_back(latency); success++; } else fail++;
                sent++;
            }
        });
    }
    while (sent < total_events) { std::this_thread::sleep_for(std::chrono::seconds(1)); auto elapsed = std::chrono::steady_clock::now() - start; double secs = std::chrono::duration<double>(elapsed).count(); std::cerr << "\r  sent=" << sent << "/" << total_events << " qps=" << static_cast<int>(sent/secs) << std::flush; }
    for (auto& t : threads) t.join();
    auto end = std::chrono::steady_clock::now(); double elapsed = std::chrono::duration<double>(end - start).count();
    std::vector<int64_t> all_latencies; for (auto& tl : thread_latencies) all_latencies.insert(all_latencies.end(), tl.begin(), tl.end());
    auto stats = calc_stats(all_latencies);
    std::cout << total_events << "," << num_threads << "," << static_cast<int>(total_events/elapsed) << ","
              << std::fixed << std::setprecision(1) << stats.avg/1000.0 << "ms," << stats.p50/1000.0 << "ms,"
              << stats.p95/1000.0 << "ms," << stats.p99/1000.0 << "ms," << stats.p999/1000.0 << "ms,"
              << stats.max/1000.0 << "ms," << elapsed << "s," << success << "/" << total_events << std::endl;
    return 0;
}
