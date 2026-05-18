#pragma once
#include <string>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/service_type.h>  // grpc::Service 完整定义
#include "queue.h"
#include "event.pb.h"

class GrpcServer {
public:
    GrpcServer(const std::string& addr, BoundedQueue<std::string>& queue);
    void start();
    void stop();
private:
    std::string addr_;
    BoundedQueue<std::string>& queue_;
    std::unique_ptr<grpc::Server> server_;
};
