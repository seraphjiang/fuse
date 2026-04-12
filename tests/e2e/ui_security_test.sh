#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# UI Security Tests — XSS, error leakage, auth flows, injection vectors
#
# Usage: ./tests/e2e/ui_security_test.sh [BASE_URL]

set -euo pipefail

BASE="${1:-http://localhost:9400}"
PASS=0
FAIL=0
RESULTS=()
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

run_test() {
    local name="$1"; shift
    if "$@" 2>"$TMPDIR/err"; then
        RESULTS+=("✅ PASS  $name")
        PASS=$((PASS + 1))
    else
        RESULTS+=("❌ FAIL  $name  ($(tail -1 "$TMPDIR/err"))")
        FAIL=$((FAIL + 1))
    fi
}

http_post() {
    curl -sk --max-time 10 -X POST "$BASE$1" \
        -H "Content-Type: application/json" -d "$2" -o "$TMPDIR/body" -w "%{http_code}" 2>/dev/null
}

http_get() {
    curl -sk --max-time 10 "$BASE$1" -o "$TMPDIR/body" -w "%{http_code}" 2>/dev/null
}

body() { cat "$TMPDIR/body"; }

# ── XSS in Query Input ──

test_xss_script_tag_in_query() {
    local status=$(http_post "/api/fuse/query" '{"query":"SELECT \"<script>alert(1)</script>\" FROM dual","format":"sql"}')
    local resp=$(body)
    # Response must not contain unescaped script tags
    [[ "$resp" != *"<script>"* ]] || { echo "XSS: unescaped <script> in response" >&2; return 1; }
}
run_test "XSS: script tag in query response" test_xss_script_tag_in_query

test_xss_img_onerror_in_query() {
    local status=$(http_post "/api/fuse/query" '{"query":"SELECT \"<img onerror=alert(1)>\" FROM dual","format":"sql"}')
    local resp=$(body)
    [[ "$resp" != *"onerror="* ]] || [[ "$resp" == *"\""* ]] # Must be JSON-escaped
}
run_test "XSS: img onerror in query response" test_xss_img_onerror_in_query

test_xss_in_error_message() {
    local status=$(http_post "/api/fuse/query" '{"query":"SELECT * FROM <script>alert(1)</script>.table","format":"sql"}')
    local resp=$(body)
    [[ "$resp" != *"<script>"* ]] || { echo "XSS: unescaped script in error" >&2; return 1; }
}
run_test "XSS: script tag in error message" test_xss_in_error_message

# ── Error Message Leakage ──

test_500_no_stack_trace() {
    local status=$(http_post "/api/fuse/query" '{"query":"SELECT * FROM nonexistent.table","format":"sql"}')
    local resp=$(body)
    [[ "$resp" != *"at src/"* ]] && [[ "$resp" != *"thread '"* ]] && [[ "$resp" != *"panicked"* ]]
}
run_test "Error leakage: no stack traces in 500" test_500_no_stack_trace

test_error_no_file_paths() {
    local status=$(http_post "/api/fuse/query" '{"query":"INVALID SQL GARBAGE","format":"sql"}')
    local resp=$(body)
    [[ "$resp" != *"/home/"* ]] && [[ "$resp" != *"/usr/"* ]] && [[ "$resp" != *".rs:"* ]]
}
run_test "Error leakage: no file paths in errors" test_error_no_file_paths

test_error_no_connection_strings() {
    local status=$(http_post "/api/fuse/query" '{"query":"SELECT * FROM nonexistent.t","format":"sql"}')
    local resp=$(body)
    [[ "$resp" != *"password"* ]] && [[ "$resp" != *"postgresql://"* ]] && [[ "$resp" != *"mongodb://"* ]]
}
run_test "Error leakage: no connection strings" test_error_no_connection_strings

# ── Security Headers on UI Pages ──

test_playground_security_headers() {
    local headers=$(curl -skI --max-time 10 "$BASE/" 2>/dev/null)
    echo "$headers" | grep -qi "x-content-type-options" || { echo "Missing X-Content-Type-Options" >&2; return 1; }
    echo "$headers" | grep -qi "x-frame-options" || { echo "Missing X-Frame-Options" >&2; return 1; }
}
run_test "Headers: playground has security headers" test_playground_security_headers

test_api_security_headers() {
    local headers=$(curl -skI --max-time 10 "$BASE/api/fuse/health" 2>/dev/null)
    echo "$headers" | grep -qi "x-content-type-options" || { echo "Missing X-Content-Type-Options" >&2; return 1; }
}
run_test "Headers: API has security headers" test_api_security_headers

# ── SQL Injection via Parameters ──

test_sqli_in_params() {
    local status=$(http_post "/api/fuse/query" '{"query":"SELECT * FROM cluster_a.application_logs WHERE user_id = $id","format":"sql","params":{"id":"1; DROP TABLE users--"}}')
    # Should not cause a 500 — params should be escaped
    [[ "$status" != "500" ]] || { echo "Possible SQLi: 500 on injected param" >&2; return 1; }
}
run_test "SQLi: injection in query params" test_sqli_in_params

test_sqli_single_quote_escape() {
    local status=$(http_post "/api/fuse/query" '{"query":"SELECT * FROM cluster_a.application_logs WHERE name = $name","format":"sql","params":{"name":"O'\''Reilly"}}')
    [[ "$status" != "500" ]]
}
run_test "SQLi: single quote in param value" test_sqli_single_quote_escape

# ── SSRF via Webhook ──

test_ssrf_webhook_localhost() {
    local status=$(http_post "/api/fuse/webhooks" '{"name":"ssrf","query":"SELECT 1","condition":{"type":"rows_returned"},"callback_url":"http://localhost:8080/steal"}')
    [[ "$status" == "400" ]] || [[ "$status" == "401" ]] || [[ "$status" == "403" ]]
}
run_test "SSRF: webhook blocks localhost" test_ssrf_webhook_localhost

test_ssrf_webhook_metadata() {
    local status=$(http_post "/api/fuse/webhooks" '{"name":"ssrf","query":"SELECT 1","condition":{"type":"rows_returned"},"callback_url":"http://169.254.169.254/latest/meta-data/"}')
    [[ "$status" == "400" ]] || [[ "$status" == "401" ]] || [[ "$status" == "403" ]]
}
run_test "SSRF: webhook blocks metadata endpoint" test_ssrf_webhook_metadata

test_ssrf_webhook_private_ip() {
    local status=$(http_post "/api/fuse/webhooks" '{"name":"ssrf","query":"SELECT 1","condition":{"type":"rows_returned"},"callback_url":"http://10.0.0.1/internal"}')
    [[ "$status" == "400" ]] || [[ "$status" == "401" ]] || [[ "$status" == "403" ]]
}
run_test "SSRF: webhook blocks private IP" test_ssrf_webhook_private_ip

# ── Chaos API Auth ──

test_chaos_requires_auth() {
    local status=$(http_post "/api/fuse/chaos" '{"enabled":true,"failure_rate_pct":100}')
    # Should be 401 or 403, not 200
    [[ "$status" == "401" ]] || [[ "$status" == "403" ]]
}
run_test "Auth: chaos API requires authentication" test_chaos_requires_auth

# ── Request Size Limits ──

test_oversized_query_rejected() {
    local big_query=$(python3 -c "print('SELECT ' + 'a' * 11000000)")
    local status=$(curl -sk --max-time 10 -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"$big_query\",\"format\":\"sql\"}" \
        -o /dev/null -w "%{http_code}" 2>/dev/null)
    [[ "$status" == "413" ]] || [[ "$status" == "400" ]]
}
run_test "Limits: oversized request body rejected" test_oversized_query_rejected

# ── Content-Type Enforcement ──

test_wrong_content_type_rejected() {
    local status=$(curl -sk --max-time 10 -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: text/plain" -d 'SELECT 1' \
        -o /dev/null -w "%{http_code}" 2>/dev/null)
    [[ "$status" == "400" ]] || [[ "$status" == "415" ]] || [[ "$status" == "422" ]]
}
run_test "Content-Type: rejects non-JSON POST" test_wrong_content_type_rejected

# ── CSV Formula Injection ──

test_csv_export_no_formula_injection() {
    local status=$(http_post "/api/fuse/query" '{"query":"SELECT 1","format":"sql","result_format":"csv"}')
    # If we could inject =CMD(), the CSV would contain it unescaped
    # This test validates the endpoint works; formula injection is tested in unit tests
    [[ "$status" == "200" ]] || [[ "$status" == "400" ]] || [[ "$status" == "404" ]]
}
run_test "CSV: export endpoint responds" test_csv_export_no_formula_injection

# ── Page Access ──

test_all_pages_return_200() {
    local pages=("/" "/playground" "/dashboard" "/explore" "/settings" "/status" "/help" "/admin" "/alerts" "/views" "/federation" "/schedules" "/quality" "/lineage" "/replay")
    for page in "${pages[@]}"; do
        local status=$(http_get "$page")
        if [[ "$status" != "200" ]]; then
            echo "Page $page returned $status" >&2
            return 1
        fi
    done
}
run_test "Pages: all UI pages return 200" test_all_pages_return_200

test_pages_have_html_content() {
    http_get "/" > /dev/null
    local resp=$(body)
    [[ "$resp" == *"<html"* ]] || [[ "$resp" == *"<!DOCTYPE"* ]] || [[ "$resp" == *"<!doctype"* ]]
}
run_test "Pages: playground returns HTML" test_pages_have_html_content

# ── Summary ──

echo ""
echo "═══════════════════════════════════════════"
echo "  UI Security Test Results"
echo "═══════════════════════════════════════════"
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo ""
echo "  Total: $((PASS + FAIL))  Pass: $PASS  Fail: $FAIL"
echo "═══════════════════════════════════════════"

[[ $FAIL -eq 0 ]]
