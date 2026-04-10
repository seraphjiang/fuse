#!/usr/bin/env bash
# #542 Connector integration tests against live playground
# Tests all endpoints and query types against the deployed Fuse instance.
set -euo pipefail

BASE="${FUSE_URL:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
PASS=0 FAIL=0 TOTAL=0

run_test() {
    local name="$1"; shift
    TOTAL=$((TOTAL+1))
    if "$@"; then
        PASS=$((PASS+1))
        printf "  ✅ PASS  %s\n" "$name"
    else
        FAIL=$((FAIL+1))
        printf "  ❌ FAIL  %s\n" "$name"
    fi
}

json_query() {
    curl -sk -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" -d "$1" 2>/dev/null
}

has_rows() {
    local resp; resp=$(json_query "$1")
    echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); exit(0 if len(d.get('rows',[])) > 0 else 1)" 2>/dev/null
}

row_count() {
    local resp; resp=$(json_query "$1")
    echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('rows',[])))" 2>/dev/null
}

http_status() {
    curl -sk -o /dev/null -w "%{http_code}" "$@" 2>/dev/null
}

echo "╔═══════════════════════════════════════════════════╗"
echo "║  #542 Connector Integration Tests                ║"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# ── Health & Discovery ──
run_test "Health endpoint" test "$(http_status "$BASE/api/fuse/health")" = "200"
run_test "Datasources lists connectors" \
    bash -c "curl -sk '$BASE/api/fuse/datasources' | python3 -c \"import sys,json; exit(0 if len(json.load(sys.stdin)) >= 2 else 1)\""
run_test "Schema discovery cluster_a" test "$(http_status "$BASE/api/fuse/datasources/cluster_a/schemas")" = "200"

# ── SQL Queries ──
run_test "SQL SELECT LIMIT" has_rows '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 3","format":"sql"}'
run_test "SQL WHERE filter" has_rows '{"query":"SELECT * FROM cluster_a.application_logs WHERE status >= 500 LIMIT 5","format":"sql"}'
run_test "SQL projection" has_rows '{"query":"SELECT service, status FROM cluster_a.application_logs LIMIT 3","format":"sql"}'

# ── PPL ──
run_test "PPL query" has_rows '{"query":"source=cluster_a.application_logs | head 3","format":"ppl"}'

# ── Cross-datasource ──
run_test "UNION ALL" has_rows '{"query":"SELECT service FROM cluster_a.application_logs UNION ALL SELECT service FROM cluster_b.application_logs LIMIT 5","format":"sql"}'
run_test "JOIN" has_rows '{"query":"SELECT * FROM cluster_a.application_logs a JOIN cluster_b.application_logs b ON a.trace_id = b.trace_id LIMIT 3","format":"sql"}'
run_test "GROUP BY across sources" \
    bash -c "[ $(row_count '{"query":"SELECT service, status FROM cluster_a.application_logs UNION ALL SELECT service, status FROM cluster_b.application_logs GROUP BY service","format":"sql"}') -ge 2 ]"

# ── Validation & Explain ──
run_test "Validate good SQL" \
    bash -c "curl -sk -X POST '$BASE/api/fuse/query/validate' -H 'Content-Type: application/json' -d '{\"query\":\"SELECT * FROM cluster_a.application_logs\",\"format\":\"sql\"}' | python3 -c \"import sys,json; exit(0 if json.load(sys.stdin).get('valid') else 1)\""
run_test "Validate bad SQL" \
    bash -c "curl -sk -X POST '$BASE/api/fuse/query/validate' -H 'Content-Type: application/json' -d '{\"query\":\"SELEC FORM\",\"format\":\"sql\"}' | python3 -c \"import sys,json; exit(0 if not json.load(sys.stdin).get('valid',True) else 1)\""

# ── Trace ──
run_test "Trace endpoint" \
    bash -c "curl -sk '$BASE/api/fuse/trace/tr-test' | python3 -c \"import sys,json; d=json.load(sys.stdin); exit(0 if 'datasources_searched' in d else 1)\""

# ── History & Stats ──
run_test "History endpoint" test "$(http_status "$BASE/api/fuse/history")" = "200"
run_test "Stats endpoint" test "$(http_status "$BASE/api/fuse/stats")" = "200"

# ── Pages ──
run_test "Root page" test "$(http_status "$BASE/")" = "200"
run_test "Playground page" test "$(http_status "$BASE/playground")" = "200"
run_test "Dashboard page" test "$(http_status "$BASE/dashboard")" = "200"
run_test "Explore page" test "$(http_status "$BASE/explore")" = "200"

# ── Negative ──
run_test "Unknown datasource 404" test "$(http_status -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" -d '{"query":"SELECT * FROM nonexistent.logs","format":"sql"}')" != "200"
run_test "Empty body 400/422" test "$(http_status -X POST "$BASE/api/fuse/query" -H "Content-Type: application/json" -d '{}')" != "200"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Total: $TOTAL  |  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ]
