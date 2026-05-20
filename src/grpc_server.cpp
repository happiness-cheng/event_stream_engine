#include "grpc_server.h"
#include "event.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

using grpc::Server; using grpc::ServerBuilder; using grpc::ServerContext; using grpc::Status;
using event::Event; using event::Ack; using event::EventBatch; using event::EventStream;

class EventStreamServiceImpl final : public EventStream::Service {
public:
    explicit EventStreamServiceImpl(BoundedQueue<std::string>& queue) : queue_(queue) {}
    // 单条事件和批量事件的最大限制常量
    static constexpr size_t MAX_PAYLOAD_SIZE = 65536;   // 64KB
    static constexpr int MAX_BATCH_SIZE = 1000;

    Status SendEvent(ServerContext* ctx, const Event* req, Ack* reply) override {
        // 输入大小校验：单条事件不超过 64KB，防止 OOM
        if (req->payload().size() > MAX_PAYLOAD_SIZE) {
            return Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "payload too large (>64KB)");
        }
        std::string data;
        if (!req->SerializeToString(&data)) { reply->set_success(false); reply->set_message("serialize failed"); return Status::OK; }
        queue_.push(std::move(data)); received_++; reply->set_success(true); reply->set_message("ok"); return Status::OK;
    }
    Status SendBatch(ServerContext* ctx, const EventBatch* req, Ack* reply) override {
        // 批量不超过 1000 条，防止单次占用过多内存
        if (req->events().size() > MAX_BATCH_SIZE) {
            return Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "batch too large (>1000 events)");
        }
        int count = 0;
        for (const auto& ev : req->events()) {
            if (ev.payload().size() > 65536) continue;  // 跳过超大事件
            std::string data;
            if (ev.SerializeToString(&data)) { queue_.push(std::move(data)); count++; }
        }
        received_ += count; reply->set_success(true); reply->set_message("batch:" + std::to_string(count)); return Status::OK;
    }
private:
    BoundedQueue<std::string>& queue_; std::atomic<uint64_t> received_{0};
};

GrpcServer::GrpcServer(const std::string& addr, BoundedQueue<std::string>& queue) : addr_(addr), queue_(queue) {}
void GrpcServer::start() {
    svc_ = std::make_unique<EventStreamServiceImpl>(queue_);
    ServerBuilder builder; builder.AddListeningPort(addr_, grpc::InsecureServerCredentials()); builder.RegisterService(svc_.get());
    server_ = builder.BuildAndStart(); spdlog::info("gRPC server listening on {}", addr_); server_->Wait();
}
void GrpcServer::stop() { if (server_) server_->Shutdown(); }
