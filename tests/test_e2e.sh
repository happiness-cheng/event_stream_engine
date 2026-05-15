#!/bin/bash
# E2E Integration Test: gRPC client -> event_stream_engine -> Redis/ClickHouse
set -e

ENGINE=./engine
BENCH=./bench_client
GRPC_ADDR="0.0.0.0:50051"

echo "=== event_stream_engine E2E Integration Test ==="
echo ""

echo "[1/5] Checking dependencies..."
redis-cli ping > /dev/null 2>&1 || { echo "FAIL: Redis not running"; exit 1; }
echo "  Redis: OK"

echo "[2/5] Starting engine..."
$ENGINE $GRPC_ADDR localhost:9092 tcp://127.0.0.1:6379 localhost > /tmp/engine_e2e.log 2>&1 &
ENGINE_PID=$!
sleep 3

grep -q "engine ready" /tmp/engine_e2e.log 2>/dev/null || { echo "FAIL: engine did not start"; cat /tmp/engine_e2e.log; kill $ENGINE_PID 2>/dev/null; exit 1; }
echo "  Engine PID=$ENGINE_PID: OK"

echo "[3/5] Running benchmarks..."
echo "  10K/50 threads..."
$BENCH 10000 50 2>/dev/null

echo "  50K/100 threads..."
$BENCH 50000 100 2>/dev/null

echo "[4/5] Checking Redis dedup..."
DEDUP_COUNT=$(redis-cli SCCARD dedup:set 2>/dev/null || echo "0")
echo "  Dedup set size: $DEDUP_COUNT"

echo "[5/5] Checking engine logs..."
ROUTES=$(grep "stats:" /tmp/engine_e2e.log | tail -1)
echo "  $ROUTES"

kill $ENGINE_PID 2>/dev/null
wait $ENGINE_PID 2>/dev/null

echo ""
echo "=== E2E Test Complete ==="
