#pragma once
#include <string>
#include <unordered_map>
#include <cstdint>
#include "event.pb.h"

class SchemaRegistry {
public:
    SchemaRegistry();
    bool parse(const std::string& data, uint32_t schema_version, event::Event& out);
    bool is_compatible(uint32_t from, uint32_t to) const;
    uint32_t latest_version() const { return latest_; }
private:
    uint32_t latest_;
    std::unordered_map<uint32_t, bool> compat_matrix_;
};
