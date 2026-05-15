#pragma once
#include <string>
#include <vector>
#include <functional>
#include <unordered_map>
#include <mutex>

class ConsistentHash {
public:
    explicit ConsistentHash(int num_virtual_nodes = 150);
    void add_node(const std::string& node);
    void remove_node(const std::string& node);
    std::string get_node(const std::string& key) const;
    size_t node_count() const { return nodes_.size(); }
private:
    uint64_t hash(const std::string& key) const;
    int num_virtual_;
    std::vector<std::string> nodes_;
    std::vector<std::pair<uint64_t, std::string>> ring_;
    bool dirty_ = false;
    mutable std::mutex mtx_;
    void rebuild_ring();
};

class TenantRouter {
public:
    TenantRouter();
    std::string route(const std::string& tenant_id) const;
    void add_tenant(const std::string& tenant_id);
    void remove_tenant(const std::string& tenant_id);
    size_t tenant_count() const { return tenants_.size(); }
private:
    ConsistentHash hash_ring_;
    std::vector<std::string> tenants_;
    mutable std::mutex mtx_;
};
