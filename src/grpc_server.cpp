#include "grpc_server.h"
#include "event.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

using grpc::Server; using grpc::ServerBuilder; using grpc::ServerContext; using grpc::Status;
using event::Event; using event::Ack; using event::EventBatch; using event::EventStream;

class EventStreamServiceImpl final : public EventStream::Service {
public:
    explicit EventStreamServiceImpl(BoundedQueue<std::string>& queue) : queue_(queue) {}
    Status SendEvent(ServerContext* ctx, const Event* req, Ack* reply) override {
        std::string data;
        if (!req->SerializeToString(&data)) { reply->set_success(false); reply->set_message("serialize failed"); return Status::OK; }
        queue_.push(std::move(data)); received_++; reply->set_success(true); reply->set_message("ok"); return Status::OK;
    }
    Status SendBatch(ServerContext* ctx, const EventBatch* req, Ack* reply) override {
        int count = 0;
        for (const auto& ev : req->events()) { std::string data; if (ev.SerializeToString(&data)) { queue_.push(std::move(data)); count++; } }
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
