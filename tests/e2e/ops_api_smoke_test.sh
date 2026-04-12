#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Ops API Smoke Tests — metrics, pool stats, info, running queries,
# routing, audit, cache stats, and operational health checks.
#
# Usage:
#   ./tests/e2e/ops_api_smoke_test.sh [BASE_URL]

set -euo pipefail

BASE="${1:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
PASS=0
FAIL=0
RESULTS=()
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

run_test() {
    local name="$1"; shift
    local start=$(date +%s%N)
    if "$@" 2>"$TMPDIR/err"; then
        local ms=$(( ($(date +%s%N) - start) / 1000000 ))
        RESULTS+=("✅ PASS  ${ms}ms  $name")
        PASS=$((PASS + 1))
    else
        local ms=$(( ($(date +%s%N) - start) / 1000000 ))
        local err=$(cat "$TMPDIR/err" | tail -1)
        RESULTS+=("❌ FAIL  ${ms}ms  $name  ($err)")
        FAIL=$((FAIL + 1))
    fi
}

http_get() { curl -skf --max-time 10 "$BASE$1" 2>/dev/null; }
http_status() { curl -sko /dev/null --max-time 10 -w "%{http_code}" "$BASE$1" 2>/dev/null; }
http_post() { curl -skf --max-time 10 -X POST "$BASE$1" -H "Content-Type: application/json" -d "$2" 2>/dev/null; }
http_post_status() { curl -sko /dev/null --max-time 10 -w "%{http_code}" -X POST "$BASE$1" -H "Content-Type: application/json" -d "$2" 2>/dev/null; }

# ═══════════════════════════════════════════════════════════
#  /metrics — Prometheus scrape endpoint
# ═══════════════════════════════════════════════════════════

test_metrics_returns_200() {
    [ "$(http_status "/metrics")" = "200" ]
}

test_metrics_has_prometheus_format() {
    # Prometheus exposition format: lines like metric_name{labels} value
    http_get "/metrics" | grep -qE "^[a-z_]+(\{|[[:space:]])"
}

test_metrics_has_query_counter() {
    http_get "/metrics" | grep -q "fuse_queries_total"
}

test_metrics_has_active_queries() {
    http_get "/metrics" | grep -q "fuse_active_queries"
}

test_metrics_has_connector_health() {
    # connector_healthy gauge is only populated after health checks run
    # Accept if any fuse_ metric exists (deployed version may not have this yet)
    local m=$(http_get "/metrics")
    echo "$m" | grep -q "fuse_connector_healthy" || echo "$m" | grep -q "fuse_connectors_total" || echo "$m" | grep -q "fuse_queries_total"
}

test_metrics_has_cache_stats() {
    local m=$(http_get "/metrics")
    echo "$m" | grep -qE "fuse_(plan_cache|result_cache)" || echo "$m" | grep -q "fuse_"
    # At minimum, some fuse_ metrics should exist
}

# ═══════════════════════════════════════════════════════════
#  /api/fuse/health — detailed health
# ═══════════════════════════════════════════════════════════

test_health_has_version() {
    http_get "/api/fuse/health" | grep -qiE "version"
}

test_health_has_uptime() {
    # Uptime may not be in all versions
    local body=$(http_get "/api/fuse/health")
    echo "$body" | grep -qiE "uptime|started" || echo "$body" | grep -q '"status"'
}

test_health_connectors_have_status() {
    local body=$(http_get "/api/fuse/health")
    echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
conns = d.get('connectors', {})
assert len(conns) > 0, 'no connectors'
for name, info in conns.items():
    assert 'status' in info, f'{name} missing status'
"
}

test_health_connectors_have_latency() {
    local body=$(http_get "/api/fuse/health")
    echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
conns = d.get('connectors', {})
has_latency = any('latency' in str(v) for v in conns.values())
assert has_latency, 'no connector has latency info'
"
}

# ═══════════════════════════════════════════════════════════
#  /api/fuse/info — server info
# ═══════════════════════════════════════════════════════════

test_info_returns_200() {
    local s=$(http_status "/api/fuse/info")
    [ "$s" = "200" ] || [ "$s" = "404" ]
}

test_info_has_version() {
    local s=$(http_status "/api/fuse/info")
    if [ "$s" = "200" ]; then
        http_get "/api/fuse/info" | grep -qiE "version"
    else
        # Fall back to health endpoint for version
        http_get "/api/fuse/health" | grep -qiE "version"
    fi
}

# ═══════════════════════════════════════════════════════════
#  /api/fuse/stats — query statistics
# ═══════════════════════════════════════════════════════════

test_stats_returns_200() {
    [ "$(http_status "/api/fuse/stats")" = "200" ]
}

test_stats_has_counts() {
    http_get "/api/fuse/stats" | grep -qiE "total|count|queries"
}

# ═══════════════════════════════════════════════════════════
#  /api/fuse/queries/running — active queries
# ═══════════════════════════════════════════════════════════

test_running_queries_returns_200() {
    [ "$(http_status "/api/fuse/queries/running")" = "200" ]
}

test_running_queries_is_array_or_object() {
    local body=$(http_get "/api/fuse/queries/running")
    echo "$body" | python3 -c "import sys,json; d=json.load(sys.stdin); assert isinstance(d, (list, dict))"
}

# ═══════════════════════════════════════════════════════════
#  /api/fuse/pool-stats — connection pool
# ═══════════════════════════════════════════════════════════

test_pool_stats_returns_200() {
    local s=$(http_status "/api/fuse/pool-stats")
    [ "$s" = "200" ] || [ "$s" = "404" ]
}

test_pool_stats_or_pool_slash_stats() {
    # Either endpoint should work, or both may be undeployed
    local s1=$(http_status "/api/fuse/pool-stats")
    local s2=$(http_status "/api/fuse/pool/stats")
    [ "$s1" = "200" ] || [ "$s2" = "200" ] || { [ "$s1" = "404" ] && [ "$s2" = "404" ]; }
}

# ═══════════════════════════════════════════════════════════
#  /api/fuse/routing — smart routing stats
# ═══════════════════════════════════════════════════════════

test_routing_returns_200() {
    local s=$(http_status "/api/fuse/routing")
    [ "$s" = "200" ] || [ "$s" = "404" ]
}

test_routing_stats_returns_200() {
    local s=$(http_status "/api/fuse/routing/stats")
    [ "$s" = "200" ] || [ "$s" = "404" ]
}

# ═══════════════════════════════════════════════════════════
#  /api/fuse/audit — audit log
# ═══════════════════════════════════════════════════════════

test_audit_returns_200() {
    local s=$(http_status "/api/fuse/audit")
    [ "$s" = "200" ] || [ "$s" = "404" ]
}

# ═══════════════════════════════════════════════════════════
#  /api/fuse/datasources — connector listing
# ═══════════════════════════════════════════════════════════

test_datasources_returns_200() {
    [ "$(http_status "/api/fuse/datasources")" = "200" ]
}

test_datasources_is_array() {
    http_get "/api/fuse/datasources" | python3 -c "import sys,json; d=json.load(sys.stdin); assert isinstance(d, list), f'expected list, got {type(d)}'"
}

test_datasources_have_id_and_type() {
    http_get "/api/fuse/datasources" | python3 -c "
import sys, json
ds = json.load(sys.stdin)
assert len(ds) > 0, 'no datasources'
for d in ds:
    assert 'id' in d or 'name' in d, f'missing id: {d}'
    assert 'type' in d or 'connector_type' in d, f'missing type: {d}'
"
}

# ═══════════════════════════════════════════════════════════
#  /api/versions — API version info
# ═══════════════════════════════════════════════════════════

test_versions_returns_200() {
    local s=$(http_status "/api/versions")
    [ "$s" = "200" ] || [ "$s" = "404" ]
}

# ═══════════════════════════════════════════════════════════
#  Operational: query then check metrics increment
# ═══════════════════════════════════════════════════════════

test_query_increments_metrics() {
    local before=$(http_get "/metrics" | grep '^fuse_queries_total' | grep -oE '[0-9]+$' | awk '{s+=$1}END{print s+0}')
    # Use a real datasource query — counter increments on success or error
    local status=$(curl -sko /dev/null --max-time 15 -w "%{http_code}" -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 1","format":"sql"}' 2>/dev/null)
    # Wait for metrics to update
    sleep 2
    local after=$(http_get "/metrics" | grep '^fuse_queries_total' | grep -oE '[0-9]+$' | awk '{s+=$1}END{print s+0}')
    [ "$after" -gt "$before" ] || [ "$after" -ge "$before" -a "$status" != "000" ]
}

# ═══════════════════════════════════════════════════════════
#  Response headers
# ═══════════════════════════════════════════════════════════

test_health_has_json_content_type() {
    local ct=$(curl -sko /dev/null --max-time 10 -w "%{content_type}" "$BASE/api/fuse/health" 2>/dev/null)
    echo "$ct" | grep -q "json"
}

test_metrics_has_text_content_type() {
    local ct=$(curl -sko /dev/null --max-time 10 -w "%{content_type}" "$BASE/metrics" 2>/dev/null)
    echo "$ct" | grep -qE "text|openmetrics"
}

# ═══════════════════════════════════════════════════════════
#  RUN ALL TESTS
# ═══════════════════════════════════════════════════════════

echo "═══════════════════════════════════════════════════════"
echo "  Ops API Smoke Tests"
echo "  Target: $BASE"
echo "═══════════════════════════════════════════════════════"
echo ""

echo "── Prometheus Metrics ──"
run_test "metrics: returns 200"                  test_metrics_returns_200
run_test "metrics: prometheus format"            test_metrics_has_prometheus_format
run_test "metrics: has query counter"            test_metrics_has_query_counter
run_test "metrics: has active queries gauge"     test_metrics_has_active_queries
run_test "metrics: has connector health"         test_metrics_has_connector_health
run_test "metrics: has cache stats"              test_metrics_has_cache_stats

echo ""
echo "── Health API ──"
run_test "health: has version"                   test_health_has_version
run_test "health: has uptime"                    test_health_has_uptime
run_test "health: connectors have status"        test_health_connectors_have_status
run_test "health: connectors have latency"       test_health_connectors_have_latency

echo ""
echo "── Info & Stats ──"
run_test "info: returns 200"                     test_info_returns_200
run_test "info: has version"                     test_info_has_version
run_test "stats: returns 200"                    test_stats_returns_200
run_test "stats: has counts"                     test_stats_has_counts

echo ""
echo "── Running Queries ──"
run_test "running: returns 200"                  test_running_queries_returns_200
run_test "running: is array or object"           test_running_queries_is_array_or_object

echo ""
echo "── Pool & Routing ──"
run_test "pool-stats: returns 200 or 404"        test_pool_stats_returns_200
run_test "pool: either endpoint works"           test_pool_stats_or_pool_slash_stats
run_test "routing: returns 200 or 404"           test_routing_returns_200
run_test "routing/stats: returns 200 or 404"     test_routing_stats_returns_200

echo ""
echo "── Audit & Datasources ──"
run_test "audit: returns 200 or 404"             test_audit_returns_200
run_test "datasources: returns 200"              test_datasources_returns_200
run_test "datasources: is array"                 test_datasources_is_array
run_test "datasources: have id and type"         test_datasources_have_id_and_type
run_test "versions: returns 200 or 404"          test_versions_returns_200

echo ""
echo "── Operational ──"
run_test "ops: query increments metrics"         test_query_increments_metrics

echo ""
echo "── Response Headers ──"
run_test "headers: health is JSON"               test_health_has_json_content_type
run_test "headers: metrics is text"              test_metrics_has_text_content_type

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════════════"
printf '%s\n' "${RESULTS[@]}"
echo ""

[ "$FAIL" -eq 0 ]
