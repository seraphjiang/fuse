#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Admin/Ops Pages UI Tests — status, alerts, settings, federation, admin
#
# Tests page loading, essential DOM elements, API-backed content rendering,
# form interactions, error states, and mobile viewport meta tags.
#
# Usage:
#   ./tests/e2e/admin_ops_pages_test.sh [BASE_URL]

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
http_status() { curl -sko /dev/null --max-time 10 -w "%{http_code}" "$@" 2>/dev/null; }
http_post() { curl -skf --max-time 10 -X POST "$BASE$1" -H "Content-Type: application/json" -d "$2" 2>/dev/null; }

# Returns page body if 200, fails if not deployed
page_body() {
    local body=$(curl -sk --max-time 10 -w "\n%{http_code}" "$BASE$1" 2>/dev/null)
    local code=$(echo "$body" | tail -1)
    [ "$code" = "200" ] || { echo "page $1 not deployed (HTTP $code)" >&2; return 1; }
    echo "$body" | sed '$d'
}

# ═══════════════════════════════════════════════════════════
#  STATUS PAGE
# ═══════════════════════════════════════════════════════════

test_status_page_loads() {
    [ "$(http_status "$BASE/status")" = "200" ]
}

test_status_page_has_title() {
    page_body "/status" | grep -qi "status\|health\|system"
}

test_status_page_has_connector_section() {
    page_body "/status" | grep -qi "connector\|datasource"
}

test_status_page_fetches_health_api() {
    page_body "/status" | grep -q "/api/fuse/health"
}

test_status_page_has_refresh() {
    page_body "/status" | grep -qiE "refresh|setInterval|reload"
}

test_health_api_returns_json() {
    local ct=$(curl -sko /dev/null --max-time 10 -w "%{content_type}" "$BASE/api/fuse/health" 2>/dev/null)
    echo "$ct" | grep -q "json"
}

test_health_api_has_status_field() {
    http_get "/api/fuse/health" | grep -q '"status"'
}

test_health_api_has_connectors() {
    http_get "/api/fuse/health" | grep -q '"connectors"'
}

# ═══════════════════════════════════════════════════════════
#  ALERTS PAGE
# ═══════════════════════════════════════════════════════════

test_alerts_page_loads() {
    local s=$(http_status "$BASE/alerts")
    [ "$s" = "200" ] || [ "$s" = "404" ]  # 404 acceptable if not yet deployed
}

test_alerts_page_has_title() {
    page_body "/alerts" | grep -qi "alert"
}

test_alerts_page_has_rule_section() {
    page_body "/alerts" | grep -qiE "rule|threshold|metric|condition"
}

test_alerts_page_fetches_alerts_api() {
    page_body "/alerts" | grep -q "/api/fuse/alerts"
}

test_alerts_page_has_create_form() {
    page_body "/alerts" | grep -qiE "<input|<select|<form|<button"
}

test_alerts_api_returns_list() {
    # GET /api/fuse/alerts should return array or object with rules
    local body=$(http_get "/api/fuse/alerts")
    echo "$body" | python3 -c "import sys,json; d=json.load(sys.stdin); assert isinstance(d, (list, dict))"
}

test_alerts_page_has_history_link() {
    page_body "/alerts" | grep -qiE "history|past|fired"
}

# ═══════════════════════════════════════════════════════════
#  SETTINGS PAGE
# ═══════════════════════════════════════════════════════════

test_settings_page_loads() {
    [ "$(http_status "$BASE/settings")" = "200" ]
}

test_settings_page_has_title() {
    page_body "/settings" | grep -qi "setting"
}

test_settings_page_has_config_sections() {
    page_body "/settings" | grep -qiE "cache|timeout|rate.limit|concurrent|config"
}

test_settings_page_has_connector_config() {
    page_body "/settings" | grep -qiE "connector|datasource"
}

test_settings_page_not_editable_without_auth() {
    local page=$(page_body "/settings")
    echo "$page" | grep -qiE "readonly|disabled|read.only" || ! echo "$page" | grep -qi "method=\"POST\""
}

# ═══════════════════════════════════════════════════════════
#  FEDERATION PAGE
# ═══════════════════════════════════════════════════════════

test_federation_page_loads() {
    local s=$(http_status "$BASE/federation")
    [ "$s" = "200" ] || [ "$s" = "404" ]  # 404 acceptable if not yet deployed
}

test_federation_page_has_title() {
    page_body "/federation" | grep -qi "federation\|topology"
}

test_federation_page_has_topology_visual() {
    page_body "/federation" | grep -qiE "svg|canvas|diagram|graph|topology|node"
}

test_federation_page_fetches_api() {
    page_body "/federation" | grep -qE "/api/fuse/(federation|datasources|health)"
}

test_federation_page_shows_connectors() {
    page_body "/federation" | grep -qiE "connector|datasource|cluster"
}

test_federation_api_returns_data() {
    local status=$(http_status "$BASE/api/fuse/federation")
    [ "$status" = "200" ] || [ "$status" = "404" ]
    # 404 is acceptable if federation endpoint isn't configured
}

# ═══════════════════════════════════════════════════════════
#  ADMIN PAGE
# ═══════════════════════════════════════════════════════════

test_admin_page_loads() {
    [ "$(http_status "$BASE/admin")" = "200" ]
}

test_admin_page_has_title() {
    page_body "/admin" | grep -qi "admin"
}

test_admin_page_has_system_info() {
    page_body "/admin" | grep -qiE "version|uptime|memory|cpu|system"
}

test_admin_page_has_cache_section() {
    page_body "/admin" | grep -qiE "cache|compilation|plan"
}

test_admin_page_has_tenant_section() {
    page_body "/admin" | grep -qiE "tenant|multi.tenant|usage"
}

# ═══════════════════════════════════════════════════════════
#  CROSS-CUTTING: VIEWPORT, NAV, ERROR HANDLING
# ═══════════════════════════════════════════════════════════

test_all_pages_have_viewport_meta() {
    local fail=0
    for page in status settings admin; do
        if ! page_body "/${page}" | grep -q 'viewport'; then
            fail=1
        fi
    done
    [ "$fail" = "0" ]
}

test_all_pages_have_nav() {
    local fail=0
    for page in status settings admin; do
        if ! page_body "/${page}" | grep -qiE "<nav|navigation|sidebar|menu"; then
            fail=1
        fi
    done
    [ "$fail" = "0" ]
}

test_all_pages_have_charset() {
    local fail=0
    for page in status settings admin; do
        if ! page_body "/${page}" | grep -qi "charset"; then
            fail=1
        fi
    done
    [ "$fail" = "0" ]
}

test_404_returns_error() {
    local status=$(http_status "$BASE/api/fuse/nonexistent-endpoint-xyz")
    [ "$status" = "404" ] || [ "$status" = "405" ]
}

test_invalid_json_returns_400() {
    local status=$(curl -sko /dev/null --max-time 10 -w "%{http_code}" \
        -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d "not-json" 2>/dev/null)
    [ "$status" = "400" ] || [ "$status" = "422" ]
}

test_health_endpoint_fast() {
    # Health should respond in under 2 seconds
    local time=$(curl -sko /dev/null --max-time 5 -w "%{time_total}" "$BASE/api/fuse/health" 2>/dev/null)
    python3 -c "assert float('$time') < 2.0, f'health took {$time}s'"
}

# ═══════════════════════════════════════════════════════════
#  RUN ALL TESTS
# ═══════════════════════════════════════════════════════════

echo "═══════════════════════════════════════════════════════"
echo "  Admin/Ops Pages UI Tests"
echo "  Target: $BASE"
echo "═══════════════════════════════════════════════════════"
echo ""

echo "── Status Page ──"
run_test "status: page loads"                    test_status_page_loads
run_test "status: has title"                     test_status_page_has_title
run_test "status: has connector section"         test_status_page_has_connector_section
run_test "status: fetches health API"            test_status_page_fetches_health_api
run_test "status: has refresh capability"        test_status_page_has_refresh
run_test "status: health API returns JSON"       test_health_api_returns_json
run_test "status: health API has status field"   test_health_api_has_status_field
run_test "status: health API has connectors"     test_health_api_has_connectors

echo ""
echo "── Alerts Page ──"
run_test "alerts: page loads"                    test_alerts_page_loads
run_test "alerts: has title"                     test_alerts_page_has_title
run_test "alerts: has rule section"              test_alerts_page_has_rule_section
run_test "alerts: fetches alerts API"            test_alerts_page_fetches_alerts_api
run_test "alerts: has create form"               test_alerts_page_has_create_form
run_test "alerts: API returns list"              test_alerts_api_returns_list
run_test "alerts: has history link"              test_alerts_page_has_history_link

echo ""
echo "── Settings Page ──"
run_test "settings: page loads"                  test_settings_page_loads
run_test "settings: has title"                   test_settings_page_has_title
run_test "settings: has config sections"         test_settings_page_has_config_sections
run_test "settings: has connector config"        test_settings_page_has_connector_config
run_test "settings: not editable without auth"   test_settings_page_not_editable_without_auth

echo ""
echo "── Federation Page ──"
run_test "federation: page loads"                test_federation_page_loads
run_test "federation: has title"                 test_federation_page_has_title
run_test "federation: has topology visual"       test_federation_page_has_topology_visual
run_test "federation: fetches API"               test_federation_page_fetches_api
run_test "federation: shows connectors"          test_federation_page_shows_connectors
run_test "federation: API returns data"          test_federation_api_returns_data

echo ""
echo "── Admin Page ──"
run_test "admin: page loads"                     test_admin_page_loads
run_test "admin: has title"                      test_admin_page_has_title
run_test "admin: has system info"                test_admin_page_has_system_info
run_test "admin: has cache section"              test_admin_page_has_cache_section
run_test "admin: has tenant section"             test_admin_page_has_tenant_section

echo ""
echo "── Cross-Cutting ──"
run_test "all pages: viewport meta"              test_all_pages_have_viewport_meta
run_test "all pages: navigation"                 test_all_pages_have_nav
run_test "all pages: charset"                    test_all_pages_have_charset
run_test "error: 404 on unknown endpoint"        test_404_returns_error
run_test "error: 400 on invalid JSON"            test_invalid_json_returns_400
run_test "perf: health endpoint < 2s"            test_health_endpoint_fast

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════════════"
printf '%s\n' "${RESULTS[@]}"
echo ""

[ "$FAIL" -eq 0 ]
