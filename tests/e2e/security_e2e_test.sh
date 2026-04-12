#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# E2E Security Hardening Tests
# Validates response headers, CORS, input validation, and abuse resistance.
#
# Usage:
#   ./tests/e2e/security_e2e_test.sh [BASE_URL]

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

http_status() { curl -sko /dev/null --max-time 10 -w "%{http_code}" "$@" 2>/dev/null; }

# Fetch full response headers
http_headers() { curl -skI --max-time 10 "$@" 2>/dev/null; }

http_post_headers() {
    curl -sk --max-time 10 -X POST "$BASE$1" \
        -H "Content-Type: application/json" -d "$2" \
        -D "$TMPDIR/resp_headers" -o /dev/null 2>/dev/null
    cat "$TMPDIR/resp_headers"
}

# ── Response Headers ──

test_content_type_json() {
    http_post_headers "/api/fuse/query" \
        '{"query":"SELECT 1","format":"sql"}' > "$TMPDIR/ct_headers"
    grep -qi 'content-type.*application/json' "$TMPDIR/ct_headers" || \
        { echo "missing application/json content-type" >&2; return 1; }
}

test_no_server_header_leak() {
    # Server should not leak detailed version info
    local headers=$(http_headers "$BASE/api/fuse/health")
    if echo "$headers" | grep -qi 'server:.*axum\|server:.*hyper\|server:.*tokio'; then
        echo "server header leaks framework info" >&2
        return 1
    fi
    return 0
}

test_cache_control_on_pages() {
    # Playground pages should have no-cache to prevent stale content
    local headers=$(http_headers "$BASE/")
    echo "$headers" | grep -qi 'cache-control.*no-cache' || \
        { echo "missing no-cache on playground" >&2; return 1; }
}

# ── SQL Injection Variants ──

test_injection_union_select() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT * FROM cluster_a.application_logs WHERE service = '\'' UNION SELECT * FROM information_schema.tables --'\''","format":"sql"}')
    [ "${s:0:1}" != "5" ]  # Must not 5xx
}

test_injection_stacked_queries() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT 1; DROP TABLE users; --","format":"sql"}')
    [ "$s" = "400" ] || [ "$s" = "200" ]  # Reject or safely ignore
}

test_injection_comment_bypass() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT * FROM cluster_a.application_logs WHERE 1=1 /**/OR/**/1=1","format":"sql"}')
    [ "${s:0:1}" != "5" ]
}

test_injection_in_ppl() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"source = cluster_a.application_logs | where service = '\''x'\'' OR 1=1 --","format":"ppl"}')
    [ "${s:0:1}" != "5" ]
}

# ── XSS in Query Parameters ──

test_xss_in_query() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT * FROM cluster_a.application_logs WHERE service = '\''<script>alert(1)</script>'\''","format":"sql"}')
    [ "${s:0:1}" != "5" ]
}

test_xss_in_datasource_path() {
    local s=$(http_status "$BASE/api/fuse/datasources/%3Cscript%3Ealert(1)%3C%2Fscript%3E/schemas")
    [ "$s" = "404" ] || [ "$s" = "400" ] || [ "$s" = "200" ]
}

# ── Path Traversal ──

test_path_traversal_datasource() {
    local s=$(http_status "$BASE/api/fuse/datasources/../../etc/passwd/schemas")
    [ "$s" = "404" ] || [ "$s" = "400" ]
}

test_path_traversal_saved() {
    local s=$(http_status "$BASE/api/fuse/saved/..%2F..%2Fetc%2Fpasswd")
    [ "$s" = "404" ] || [ "$s" = "400" ]
}

# ── HTTP Method Enforcement ──

test_query_rejects_get() {
    local s=$(http_status "$BASE/api/fuse/query")
    [ "$s" = "405" ] || [ "$s" = "404" ]  # POST only
}

test_health_rejects_post() {
    local s=$(http_status -X POST "$BASE/api/fuse/health" \
        -H "Content-Type: application/json" -d '{}')
    [ "$s" = "405" ] || [ "$s" = "404" ]  # GET only
}

test_delete_on_readonly_endpoint() {
    local s=$(http_status -X DELETE "$BASE/api/fuse/health")
    [ "$s" = "405" ] || [ "$s" = "404" ]
}

# ── Oversized / Malformed Input ──

test_oversized_json_body() {
    # 1MB JSON body — should reject, not OOM
    local big=$(python3 -c "print('{\"query\":\"' + 'A' * 1048576 + '\",\"format\":\"sql\"}')")
    local s=$(echo "$big" | curl -sko /dev/null --max-time 15 -w "%{http_code}" \
        -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" -d @- 2>/dev/null)
    [ "$s" != "000" ]  # Must respond, not hang
}

test_malformed_json() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" -d '{invalid json}')
    [ "$s" = "400" ] || [ "$s" = "422" ]
}

test_wrong_content_type() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: text/plain" -d 'SELECT 1')
    [ "$s" = "400" ] || [ "$s" = "415" ] || [ "$s" = "422" ]
}

test_null_bytes_in_query() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT\u0000* FROM cluster_a.application_logs","format":"sql"}')
    [ "${s:0:1}" != "5" ]
}

# ── Rate / Abuse Resistance ──

test_rapid_fire_no_crash() {
    # 20 rapid sequential requests — server should handle without 5xx
    local fail=0
    for i in $(seq 1 20); do
        local s=$(http_status -X POST "$BASE/api/fuse/query" \
            -H "Content-Type: application/json" \
            -d '{"query":"SELECT 1","format":"sql"}')
        if [ "${s:0:1}" = "5" ]; then
            fail=1
            break
        fi
    done
    [ "$fail" -eq 0 ]
}

# ── CORS ──

test_cors_preflight() {
    local headers=$(curl -sk --max-time 10 -X OPTIONS "$BASE/api/fuse/query" \
        -H "Origin: https://evil.example.com" \
        -H "Access-Control-Request-Method: POST" \
        -D - -o /dev/null 2>/dev/null)
    local s=$(echo "$headers" | head -1 | grep -oP '\d{3}')
    # Should either respond with CORS headers (200/204) or reject
    [ "$s" = "200" ] || [ "$s" = "204" ] || [ "$s" = "403" ] || [ "$s" = "405" ]
}

# ── Run ──

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Security Hardening E2E Tests                     ║"
echo "║  Headers · Injection · XSS · Traversal · CORS     ║"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Response headers
run_test "Headers: JSON content-type on API"        test_content_type_json
run_test "Headers: no server framework leak"        test_no_server_header_leak
run_test "Headers: no-cache on playground"          test_cache_control_on_pages

# SQL injection
run_test "Injection: UNION SELECT no 5xx"           test_injection_union_select
run_test "Injection: stacked queries rejected"      test_injection_stacked_queries
run_test "Injection: comment bypass no 5xx"         test_injection_comment_bypass
run_test "Injection: PPL injection no 5xx"          test_injection_in_ppl

# XSS
run_test "XSS: script tag in query no 5xx"          test_xss_in_query
run_test "XSS: script in datasource path"           test_xss_in_datasource_path

# Path traversal
run_test "Traversal: datasource path"               test_path_traversal_datasource
run_test "Traversal: saved query path"              test_path_traversal_saved

# HTTP method enforcement
run_test "Method: query rejects GET"                test_query_rejects_get
run_test "Method: health rejects POST"              test_health_rejects_post
run_test "Method: DELETE on read-only endpoint"     test_delete_on_readonly_endpoint

# Malformed input
run_test "Input: oversized JSON body"               test_oversized_json_body
run_test "Input: malformed JSON"                    test_malformed_json
run_test "Input: wrong content-type"                test_wrong_content_type
run_test "Input: null bytes in query"               test_null_bytes_in_query

# Abuse resistance
run_test "Abuse: 20 rapid-fire no 5xx"              test_rapid_fire_no_crash

# CORS
run_test "CORS: preflight from foreign origin"      test_cors_preflight

# ── Summary ──

echo ""
echo "═══════════════════════════════════════════════════"
echo "  RESULTS"
echo "═══════════════════════════════════════════════════"
for r in "${RESULTS[@]}"; do
    echo "  $r"
done
echo ""
echo "  Total: $((PASS + FAIL))  |  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"

[ "$FAIL" -eq 0 ]
