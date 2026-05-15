#include "tenant_router.h"
#include <algorithm>
#include <functional>

ConsistentHash::ConsistentHash(int num_virtual_nodes) : num_virtual_(num_virtual_nodes) {}

uint64_t ConsistentHash::hash(const std::string& key) const {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (char c : key) { h ^= static_cast<uint64_t>(c); h *= 0x100000001b3ULL; }
    return h;
}

void ConsistentHash::add_node(const std::string& node) { std::lock_guard<std::mutex> lk(mtx_); nodes_.push_back(node); dirty_ = true; }
void ConsistentHash::remove_node(const std::string& node) { std::lock_guard<std::mutex> lk(mtx_); nodes_.erase(std::remove(nodes_.begin(), nodes_.end(), node), nodes_.end()); dirty_ = true; }

void ConsistentHash::rebuild_ring() {
    ring_.clear();
    for (const auto& node : nodes_)
        for (int i = 0; i < num_virtual_; ++i) ring_.emplace_back(hash(node + "#" + std::to_string(i)), node);
    std::sort(ring_.begin(), ring_.end()); dirty_ = false;
}

std::string ConsistentHash::get_node(const std::string& key) const {
    std::lock_guard<std::mutex> lk(mtx_);
    if (dirty_) const_cast<ConsistentHash*>(this)->rebuild_ring();
    if (ring_.empty()) return "";
    uint64_t h = hash(key);
    auto it = std::lower_bound(ring_.begin(), ring_.end(), std::make_pair(h, std::string("")));
    if (it == ring_.end()) it = ring_.begin();
    return it->second;
}

TenantRouter::TenantRouter() : hash_ring_(150) {}
void TenantRouter::add_tenant(const std::string& tenant_id) { std::lock_guard<std::mutex> lk(mtx_); tenants_.push_back(tenant_id); hash_ring_.add_node(tenant_id); }
void TenantRouter::remove_tenant(const std::string& tenant_id) { std::lock_guard<std::mutex> lk(mtx_); tenants_.erase(std::remove(tenants_.begin(), tenants_.end(), tenant_id), tenants_.end()); hash_ring_.remove_node(tenant_id); }
std::string TenantRouter::route(const std::string& tenant_id) const { return hash_ring_.get_node(tenant_id); }
