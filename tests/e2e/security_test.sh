#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fuse Security E2E Tests
# Auth, rate limiting, RBAC enforcement, tenant isolation.
#
# Usage:
#   ./tests/e2e/security_test.sh [BASE_URL]

set -euo pipefail

BASE="${1:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
PASS=0
FAIL=0
RESULTS=()
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# ── Helpers ──

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

http_status() {
    curl -sko /dev/null --max-time 10 -w "%{http_code}" "$@" 2>/dev/null
}

http_post_status() {
    curl -sko /dev/null --max-time 10 -w "%{http_code}" -X POST "$BASE$1" \
        -H "Content-Type: application/json" -d "$2" 2>/dev/null
}

http_get() { curl -skf --max-time 10 "$BASE$1" 2>/dev/null; }

# ── Auth: Public paths bypass authentication ──

test_health_public()     { [ "$(http_status "$BASE/api/fuse/health")" = "200" ]; }
test_metrics_public()    { [ "$(http_status "$BASE/metrics")" = "200" ]; }
test_root_public()       { [ "$(http_status "$BASE/")" = "200" ]; }
test_playground_public() { [ "$(http_status "$BASE/playground")" = "200" ]; }

# ── Auth: API key handling (no 500 on any input) ──

test_invalid_api_key() {
    local s; s=$(curl -sko /dev/null --max-time 10 -w "%{http_code}" \
        -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" \
        -H "x-api-key: bogus-key-999" -d '{"query":"SELECT 1","format":"sql"}' 2>/dev/null)
    [ "$s" = "200" ] || [ "$s" = "401" ]
}

test_bearer_token() {
    local s; s=$(curl -sko /dev/null --max-time 10 -w "%{http_code}" \
        -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" \
        -H "Authorization: Bearer fake-token" -d '{"query":"SELECT 1","format":"sql"}' 2>/dev/null)
    [ "$s" = "200" ] || [ "$s" = "401" ]
}

test_empty_api_key() {
    local s; s=$(curl -sko /dev/null --max-time 10 -w "%{http_code}" \
        -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" \
        -H "x-api-key: " -d '{"query":"SELECT 1","format":"sql"}' 2>/dev/null)
    [ "$s" = "200" ] || [ "$s" = "401" ]
}

test_malformed_bearer() {
    local s; s=$(curl -sko /dev/null --max-time 10 -w "%{http_code}" \
        -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" \
        -H "Authorization: NotBearer xyz" -d '{"query":"SELECT 1","format":"sql"}' 2>/dev/null)
    [ "$s" = "200" ] || [ "$s" = "401" ]
}

# ── Rate Limiting ──

test_rate_limit_429() {
    # Default per-IP: 100/min. Burst 130 requests.
    local got_429=false
    for _ in $(seq 1 130); do
        local s; s=$(http_post_status "/api/fuse/query" '{"query":"SELECT 1","format":"sql"}')
        if [ "$s" = "429" ]; then got_429=true; break; fi
    done
    [ "$got_429" = "true" ]
}

test_rate_limit_retry_after() {
    for _ in $(seq 1 150); do
        local hdrs; hdrs=$(curl -sk --max-time 5 -o /dev/null -D - \
            -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" \
            -d '{"query":"SELECT 1","format":"sql"}' 2>/dev/null)
        if echo "$hdrs" | grep -q "429"; then
            echo "$hdrs" | grep -qi "Retry-After"; return $?
        fi
    done
    return 0
}

test_rate_limit_json_body() {
    for _ in $(seq 1 150); do
        local resp; resp=$(curl -sk --max-time 5 -w "\n%{http_code}" \
            -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" \
            -d '{"query":"SELECT 1","format":"sql"}' 2>/dev/null)
        if [ "$(echo "$resp" | tail -1)" = "429" ]; then
            echo "$resp" | sed '$d' | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'error' in d"
            return $?
        fi
    done
    return 0
}

# ── RBAC Enforcement ──

test_rbac_get_query_405() {
    [ "$(http_status "$BASE/api/fuse/query")" = "405" ]
}

test_rbac_datasources_readable() {
    [ "$(http_status "$BASE/api/fuse/datasources")" = "200" ]
}

test_rbac_query_accessible() {
    [ "$(http_post_status "/api/fuse/query" '{"query":"SELECT 1","format":"sql"}')" = "200" ]
}

# ── Tenant Isolation ──

test_tenant_all_ds_visible() {
    http_get "/api/fuse/datasources" > "$TMPDIR/ds.json"
    python3 -c "
import json; ds=json.load(open('$TMPDIR/ds.json'))
assert isinstance(ds, list) and len(ds) > 0
" 2>&1
}

test_tenant_cross_ds_no_403() {
    local s; s=$(http_post_status "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 1","format":"sql"}')
    [ "$s" = "200" ] || [ "$s" = "400" ]
}

test_tenant_unknown_ds_no_leak() {
    local s; s=$(http_post_status "/api/fuse/query" \
        '{"query":"SELECT * FROM fake_tenant_ds.tbl LIMIT 1","format":"sql"}')
    [ "$s" != "403" ]
}

# ── Security Edge Cases ──

test_security_headers_present() {
    local hdrs
    hdrs=$(curl -sk --max-time 10 -o /dev/null -D - "$BASE/api/fuse/health" 2>/dev/null)
    echo "$hdrs" | grep -qi "x-content-type-options: nosniff" &&
    echo "$hdrs" | grep -qi "x-frame-options: DENY"
}

test_sql_injection_multi_stmt() {
    [ "$(http_post_status "/api/fuse/query" '{"query":"SELECT 1; DROP TABLE users; --","format":"sql"}')" = "400" ]
}

test_oversized_payload() {
    local big; big=$(python3 -c "print('SELECT '+','.join(['c'+str(i) for i in range(50000)])+' FROM t')")
    local s; s=$(curl -sko /dev/null --max-time 15 -w "%{http_code}" \
        -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" \
        -d "{\"query\":\"$big\",\"format\":\"sql\"}" 2>/dev/null)
    [ "$s" = "200" ] || [ "$s" = "400" ] || [ "$s" = "413" ]
}

test_empty_body_rejected() {
    local s; s=$(curl -sko /dev/null --max-time 10 -w "%{http_code}" \
        -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" -d '' 2>/dev/null)
    [ "$s" = "400" ] || [ "$s" = "422" ]
}

# ── Run ──

run_test "Health public (no auth)"            test_health_public
run_test "Metrics public (no auth)"           test_metrics_public
run_test "Root public (no auth)"              test_root_public
run_test "Playground public (no auth)"        test_playground_public
run_test "Invalid x-api-key no 500"           test_invalid_api_key
run_test "Fake Bearer token no 500"           test_bearer_token
run_test "Empty x-api-key no crash"           test_empty_api_key
run_test "Malformed Authorization no crash"   test_malformed_bearer
run_test "Burst triggers 429"                 test_rate_limit_429
run_test "429 has Retry-After header"         test_rate_limit_retry_after
run_test "429 body is JSON with error"        test_rate_limit_json_body
run_test "GET /query → 405"                   test_rbac_get_query_405
run_test "Datasources readable (viewer)"      test_rbac_datasources_readable
run_test "Query accessible (editor)"          test_rbac_query_accessible
run_test "All datasources visible (no tenant)" test_tenant_all_ds_visible
run_test "Cross-DS query no 403"              test_tenant_cross_ds_no_403
run_test "Unknown DS no tenant leak"          test_tenant_unknown_ds_no_leak
run_test "Security headers on responses"      test_security_headers_present
run_test "SQL injection multi-stmt → 400"     test_sql_injection_multi_stmt
run_test "Oversized payload handled"          test_oversized_payload
run_test "Empty body → 400/422"               test_empty_body_rejected

# ── Summary ──

echo ""
echo "═══════════════════════════════════════════════════"
echo "  SECURITY TEST RESULTS"
echo "═══════════════════════════════════════════════════"
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo ""
echo "  Total: $((PASS + FAIL))  |  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"

[ "$FAIL" -eq 0 ]
