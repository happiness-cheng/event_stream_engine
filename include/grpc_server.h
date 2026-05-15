#pragma once
#include <string>
#include <memory>
#include <grpcpp/grpcpp.h>
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
