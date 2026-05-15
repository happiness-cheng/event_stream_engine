#include "schema_registry.h"
#include <spdlog/spdlog.h>

SchemaRegistry::SchemaRegistry() : latest_(1) {
    compat_matrix_[1] = true; compat_matrix_[2] = true; compat_matrix_[3] = true;
}

bool SchemaRegistry::parse(const std::string& data, uint32_t schema_version, event::Event& out) {
    if (!out.ParseFromString(data)) { spdlog::warn("schema_registry: parse failed for version {}", schema_version); return false; }
    return true;
}

bool SchemaRegistry::is_compatible(uint32_t from, uint32_t to) const {
    auto it = compat_matrix_.find(from);
    if (it == compat_matrix_.end()) return false;
    return it->second;
}
