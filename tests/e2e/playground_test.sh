#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fuse Playground E2E Smoke Tests
# Run after every deploy to verify the live service.
#
# Usage:
#   ./tests/e2e/playground_test.sh [BASE_URL]
#
# Default: http://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com

set -euo pipefail

BASE="${1:-http://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
PASS=0
FAIL=0
SKIP=0
RESULTS=()

# ── Helpers ──

run_test() {
    local name="$1"
    local start
    start=$(date +%s%N)
    shift
    if "$@"; then
        local ms=$(( ($(date +%s%N) - start) / 1000000 ))
        RESULTS+=("✅ PASS  ${ms}ms  $name")
        PASS=$((PASS + 1))
    else
        local ms=$(( ($(date +%s%N) - start) / 1000000 ))
        RESULTS+=("❌ FAIL  ${ms}ms  $name")
        FAIL=$((FAIL + 1))
    fi
}

skip_test() {
    local name="$1"
    RESULTS+=("⏭️  SKIP        $name")
    SKIP=$((SKIP + 1))
}

http_get() {
    curl -sf --max-time 10 "$BASE$1" 2>/dev/null
}

http_post() {
    curl -sf --max-time 10 -X POST "$BASE$1" \
        -H "Content-Type: application/json" \
        -d "$2" 2>/dev/null
}

http_status() {
    curl -so /dev/null --max-time 10 -w "%{http_code}" "$@" 2>/dev/null
}

# ── Tests ──

test_health_returns_200() {
    local body
    body=$(http_get "/api/fuse/health")
    echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert d['status'] in ('healthy', 'degraded'), f'unexpected status: {d[\"status\"]}'
assert 'connectors' in d
assert 'cluster_a' in d['connectors'], 'cluster_a missing'
assert 'cluster_b' in d['connectors'], 'cluster_b missing'
"
}

test_health_connectors_healthy() {
    local body
    body=$(http_get "/api/fuse/health")
    echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert d['status'] == 'healthy', f'overall: {d[\"status\"]}'
for name, c in d['connectors'].items():
    assert c['status'] == 'healthy', f'{name}: {c[\"status\"]}'
    assert c['latency_ms'] is not None, f'{name}: no latency'
"
}

test_datasources_lists_both() {
    local body
    body=$(http_get "/api/fuse/datasources")
    echo "$body" | python3 -c "
import sys, json
ds = json.load(sys.stdin)
ids = {d['id'] for d in ds}
assert 'cluster_a' in ids, 'cluster_a missing'
assert 'cluster_b' in ids, 'cluster_b missing'
assert all(d['connector_type'] == 'opensearch' for d in ds)
"
}

test_schema_discovery_cluster_a() {
    local body
    body=$(http_get "/api/fuse/datasources/cluster_a/schemas")
    echo "$body" | python3 -c "
import sys, json
schemas = json.load(sys.stdin)
assert isinstance(schemas, list), 'expected array'
names = [s['name'] for s in schemas]
assert 'application_logs' in names, f'application_logs not found in {names}'
"
}

test_schema_discovery_cluster_b() {
    local body
    body=$(http_get "/api/fuse/datasources/cluster_b/schemas")
    echo "$body" | python3 -c "
import sys, json
schemas = json.load(sys.stdin)
names = [s['name'] for s in schemas]
assert 'application_logs' in names, f'application_logs not found in {names}'
"
}

test_sql_query_returns_rows() {
    local body
    body=$(http_post "/api/fuse/query" \
        '{"query": "SELECT * FROM cluster_a.application_logs LIMIT 5", "format": "sql"}')
    echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert len(d['columns']) > 0, 'no columns'
assert d['metadata']['total_rows'] > 0, 'no rows returned'
assert d['metadata']['total_rows'] <= 5, 'limit not applied'
"
}

test_ppl_query_returns_rows() {
    local body
    body=$(http_post "/api/fuse/query" \
        '{"query": "source = cluster_a.application_logs | head 5", "format": "ppl"}')
    echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert d['metadata']['total_rows'] > 0, 'no rows from PPL query'
"
}

test_cross_cluster_query() {
    local body_a body_b
    body_a=$(http_post "/api/fuse/query" \
        '{"query": "SELECT * FROM cluster_a.application_logs LIMIT 100", "format": "sql"}')
    body_b=$(http_post "/api/fuse/query" \
        '{"query": "SELECT * FROM cluster_b.application_logs LIMIT 100", "format": "sql"}')
    python3 -c "
import json
a = json.loads('$body_a')
b = json.loads('$body_b')
assert a['metadata']['total_rows'] > 0, 'cluster_a returned no rows'
assert b['metadata']['total_rows'] > 0, 'cluster_b returned no rows'
# Verify different services in each cluster
"
}

test_filter_pushdown() {
    local all_body filtered_body
    all_body=$(http_post "/api/fuse/query" \
        '{"query": "SELECT * FROM cluster_a.application_logs LIMIT 100", "format": "sql"}')
    filtered_body=$(http_post "/api/fuse/query" \
        '{"query": "SELECT * FROM cluster_a.application_logs WHERE status >= 500 LIMIT 100", "format": "sql"}')
    python3 -c "
import json
all_rows = json.loads('$all_body')['metadata']['total_rows']
filtered = json.loads('$filtered_body')['metadata']['total_rows']
assert all_rows > 0, 'no rows without filter'
assert filtered < all_rows, f'filter did not reduce rows: {filtered} vs {all_rows}'
"
}

test_validate_good_sql() {
    local body
    body=$(http_post "/api/fuse/query/validate" \
        '{"query": "SELECT * FROM cluster_a.application_logs"}')
    echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert d['valid'] == True, f'expected valid=true, got {d}'
"
}

test_validate_bad_sql() {
    local body
    body=$(http_post "/api/fuse/query/validate" \
        '{"query": "not valid sql at all"}')
    echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert d['valid'] == False, 'expected valid=false'
assert d.get('error') is not None, 'expected error message'
"
}

test_explain_returns_plan() {
    local body
    body=$(http_post "/api/fuse/query/explain" \
        '{"query": "SELECT * FROM cluster_a.application_logs"}')
    echo "$body" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert 'plan' in d, 'no plan in response'
assert len(d['plan']) > 0, 'empty plan'
"
}

test_unknown_datasource_404() {
    local status
    status=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query": "SELECT * FROM nope.logs"}')
    [ "$status" = "404" ]
}

test_malformed_sql_400() {
    local status
    status=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query": "INSERT INTO foo"}')
    [ "$status" = "400" ]
}

# ── Run ──

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Fuse Playground E2E Smoke Tests                  ║"
echo "╠═══════════════════════════════════════════════════╣"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

run_test "Health returns 200 with connectors"       test_health_returns_200
run_test "Both connectors healthy"                   test_health_connectors_healthy
run_test "Datasources lists both clusters"           test_datasources_lists_both
run_test "Schema discovery cluster_a"                test_schema_discovery_cluster_a
run_test "Schema discovery cluster_b"                test_schema_discovery_cluster_b
run_test "SQL query returns rows"                    test_sql_query_returns_rows
run_test "PPL query returns rows"                    test_ppl_query_returns_rows
run_test "Cross-cluster query"                       test_cross_cluster_query
run_test "Filter pushdown reduces rows"              test_filter_pushdown
run_test "Validate accepts good SQL"                 test_validate_good_sql
run_test "Validate rejects bad SQL"                  test_validate_bad_sql
run_test "Explain returns plan"                      test_explain_returns_plan
run_test "Unknown datasource returns 404"            test_unknown_datasource_404
run_test "Malformed SQL returns 400"                 test_malformed_sql_400

# ── Summary ──

echo ""
echo "═══════════════════════════════════════════════════"
echo "  RESULTS"
echo "═══════════════════════════════════════════════════"
for r in "${RESULTS[@]}"; do
    echo "  $r"
done
echo ""
echo "  Total: $((PASS + FAIL + SKIP))  |  Pass: $PASS  |  Fail: $FAIL  |  Skip: $SKIP"
echo "═══════════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
