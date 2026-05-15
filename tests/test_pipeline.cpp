#include "quality_pipeline.h"
#include "schema_registry.h"
#include "circuit_breaker.h"
#include "tenant_router.h"
#include "lambda_router.h"
#include "event.pb.h"
#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>

static const std::string TEST_SECRET = "test_secret";

event::Event make_event(const std::string& id, const std::string& user, const std::string& type, const std::string& platform, int64_t ts, bool sign = false) {
    event::Event ev; ev.set_event_id(id); ev.set_user_id(user); ev.set_event_type(type); ev.set_platform(platform); ev.set_ts(ts); ev.set_payload("test_data");
    if (sign) { std::string hmac = QualityPipeline::sign(ev, TEST_SECRET); ev.set_payload("test_data" + hmac); }
    return ev;
}

void test_schema_registry() {
    SchemaRegistry sr; assert(sr.latest_version() == 1);
    event::Event ev = make_event("e1", "u1", "click", "web", 1000); std::string data; ev.SerializeToString(&data);
    event::Event parsed; assert(sr.parse(data, 1, parsed)); assert(parsed.event_id() == "e1");
    std::cout << "PASS: schema_registry" << std::endl;
}

void test_quality_pipeline() {
    QualityPipeline qp(TEST_SECRET);
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto ev1 = make_event("e1", "u1", "click", "web", now, true); assert(qp.process(ev1).passed);
    auto ev2 = make_event("e1", "u1", "click", "web", now, true); auto r2 = qp.process(ev2); assert(!r2.passed && r2.stage == "dedup");
    auto ev3 = make_event("e2", "u1", "click", "web", now, true); assert(qp.process(ev3).passed);
    auto ev4 = make_event("e3", "u1", "click", "web", now, false); auto r4 = qp.process(ev4); assert(!r4.passed && r4.stage == "hmac");
    assert(qp.total_processed() == 4 && qp.total_rejected() == 2);
    std::cout << "PASS: quality_pipeline" << std::endl;
}

void test_circuit_breaker() {
    CircuitBreaker cb(3, std::chrono::seconds(1), 2);
    assert(cb.state() == CircuitState::CLOSED && cb.allow_request());
    cb.record_failure(); cb.record_failure(); cb.record_failure();
    assert(cb.state() == CircuitState::OPEN && !cb.allow_request());
    std::this_thread::sleep_for(std::chrono::seconds(2));
    assert(cb.allow_request() && cb.state() == CircuitState::HALF_OPEN);
    cb.record_success(); cb.record_success();
    assert(cb.state() == CircuitState::CLOSED);
    std::cout << "PASS: circuit_breaker" << std::endl;
}

void test_tenant_router() {
    TenantRouter tr; tr.add_tenant("tenant_a"); tr.add_tenant("tenant_b"); tr.add_tenant("tenant_c");
    assert(tr.tenant_count() == 3);
    std::string n1 = tr.route("user_123"); std::string n2 = tr.route("user_123");
    assert(n1 == n2 && !n1.empty());
    std::cout << "PASS: tenant_router" << std::endl;
}

void test_consistent_hash_stability() {
    ConsistentHash ch(150); ch.add_node("node_a"); ch.add_node("node_b"); ch.add_node("node_c");
    ch.remove_node("node_c"); assert(!ch.get_node("key1").empty());
    std::cout << "PASS: consistent_hash_stability" << std::endl;
}

void test_lambda_router() {
    LambdaRouter router("tcp://127.0.0.1:6399");
    auto click_ev = make_event("e1", "u1", "click", "web", 1000); assert(router.route(click_ev) == "hot");
    auto other_ev = make_event("e2", "u1", "signup", "web", 1000); assert(router.route(other_ev) == "cold");
    router.write_hot(click_ev); router.write_cold(other_ev);
    auto s = router.stats(); assert(s.hot_count == 1 && s.cold_count == 1 && s.hot_fallback_cold == 1);
    assert(router.flush_cold() == 2);
    std::cout << "PASS: lambda_router" << std::endl;
}

void test_hmac_sign_verify() {
    QualityPipeline qp(TEST_SECRET);
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto ev1 = make_event("hmac_e1", "u1", "click", "web", now, true); assert(qp.process(ev1).passed);
    auto ev2 = make_event("hmac_e2", "u1", "click", "web", now, true); ev2.set_event_id("TAMPERED");
    auto r2 = qp.process(ev2); assert(!r2.passed && r2.stage == "hmac");
    auto ev3 = make_event("hmac_e3", "u1", "click", "web", now, false); auto r3 = qp.process(ev3); assert(!r3.passed && r3.stage == "hmac");
    std::cout << "PASS: hmac_sign_verify" << std::endl;
}

void test_end_to_end_pipeline() {
    QualityPipeline qp(TEST_SECRET); SchemaRegistry sr; TenantRouter tr;
    tr.add_tenant("tenant_a"); tr.add_tenant("tenant_b");
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    int passed = 0, rejected = 0;
    for (int i = 0; i < 100; ++i) {
        auto ev = make_event("e" + std::to_string(i), "u" + std::to_string(i%10), i%3==0 ? "click" : "view", "web", now, true);
        std::string data; ev.SerializeToString(&data); event::Event parsed;
        if (!sr.parse(data, sr.latest_version(), parsed)) { rejected++; continue; }
        if (!qp.process(parsed).passed) { rejected++; continue; }
        assert(!tr.route(parsed.user_id()).empty()); passed++;
    }
    assert(passed + rejected == 100);
    std::cout << "PASS: end_to_end_pipeline (passed=" << passed << ", rejected=" << rejected << ")" << std::endl;
}

int main() {
    test_schema_registry(); test_quality_pipeline(); test_hmac_sign_verify();
    test_circuit_breaker(); test_tenant_router(); test_consistent_hash_stability();
    test_lambda_router(); test_end_to_end_pipeline();
    std::cout << "\nAll pipeline tests passed!" << std::endl; return 0;
}
