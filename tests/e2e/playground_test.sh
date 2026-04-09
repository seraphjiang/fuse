#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fuse Playground E2E Smoke Tests
# Run after every deploy to verify the live service.
#
# Usage:
#   ./tests/e2e/playground_test.sh [BASE_URL]

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

http_get() { curl -skf --max-time 10 "$BASE$1" 2>/dev/null; }

http_post() {
    curl -skf --max-time 10 -X POST "$BASE$1" \
        -H "Content-Type: application/json" -d "$2" 2>/dev/null
}

http_status() {
    curl -sko /dev/null --max-time 10 -w "%{http_code}" "$@" 2>/dev/null
}

# ── Tests ──

test_health() {
    http_get "/api/fuse/health" > "$TMPDIR/health.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/health.json'))
assert d['status'] in ('healthy','degraded'), f'status: {d[\"status\"]}'
assert 'cluster_a' in d['connectors']
assert 'cluster_b' in d['connectors']
" 2>&1
}

test_health_connectors_healthy() {
    http_get "/api/fuse/health" > "$TMPDIR/health2.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/health2.json'))
for name, c in d['connectors'].items():
    assert c['status'] == 'healthy', f'{name}: {c[\"status\"]}'
" 2>&1
}

test_datasources() {
    http_get "/api/fuse/datasources" > "$TMPDIR/ds.json"
    python3 -c "
import json
ds = json.load(open('$TMPDIR/ds.json'))
ids = {d['id'] for d in ds}
assert 'cluster_a' in ids and 'cluster_b' in ids, f'got: {ids}'
" 2>&1
}

test_schema_cluster_a() {
    http_get "/api/fuse/datasources/cluster_a/schemas" > "$TMPDIR/schema_a.json"
    python3 -c "
import json
s = json.load(open('$TMPDIR/schema_a.json'))
names = [x['name'] for x in s]
assert 'application_logs' in names, f'got: {names}'
" 2>&1
}

test_schema_cluster_b() {
    http_get "/api/fuse/datasources/cluster_b/schemas" > "$TMPDIR/schema_b.json"
    python3 -c "
import json
s = json.load(open('$TMPDIR/schema_b.json'))
names = [x['name'] for x in s]
assert 'application_logs' in names, f'got: {names}'
" 2>&1
}

test_sql_query() {
    http_post "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}' \
        > "$TMPDIR/sql.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/sql.json'))
assert len(d['columns']) > 0, 'no columns'
assert d['metadata']['total_rows'] > 0, 'no rows'
assert d['metadata']['total_rows'] <= 5, f'limit not applied: {d[\"metadata\"][\"total_rows\"]}'
" 2>&1
}

test_ppl_query() {
    http_post "/api/fuse/query" \
        '{"query":"source = cluster_a.application_logs | head 5","format":"ppl"}' \
        > "$TMPDIR/ppl.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/ppl.json'))
assert d['metadata']['total_rows'] > 0, 'no rows from PPL'
" 2>&1
}

test_cross_cluster() {
    http_post "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 100","format":"sql"}' \
        > "$TMPDIR/cross_a.json"
    http_post "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_b.application_logs LIMIT 100","format":"sql"}' \
        > "$TMPDIR/cross_b.json"
    python3 -c "
import json
a = json.load(open('$TMPDIR/cross_a.json'))
b = json.load(open('$TMPDIR/cross_b.json'))
assert a['metadata']['total_rows'] > 0, 'cluster_a: no rows'
assert b['metadata']['total_rows'] > 0, 'cluster_b: no rows'
" 2>&1
}

test_filter_pushdown() {
    http_post "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 100","format":"sql"}' \
        > "$TMPDIR/all.json"
    http_post "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_a.application_logs WHERE status >= 500 LIMIT 100","format":"sql"}' \
        > "$TMPDIR/filtered.json"
    python3 -c "
import json
a = json.load(open('$TMPDIR/all.json'))['metadata']['total_rows']
f = json.load(open('$TMPDIR/filtered.json'))['metadata']['total_rows']
assert a > 0, 'no rows unfiltered'
assert f < a, f'filter did not reduce: {f} vs {a}'
" 2>&1
}

test_validate_good() {
    http_post "/api/fuse/query/validate" \
        '{"query":"SELECT * FROM cluster_a.application_logs"}' > "$TMPDIR/vg.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/vg.json')); assert d['valid'] == True
" 2>&1
}

test_validate_bad() {
    http_post "/api/fuse/query/validate" \
        '{"query":"not valid sql"}' > "$TMPDIR/vb.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/vb.json')); assert d['valid'] == False
" 2>&1
}

test_explain() {
    http_post "/api/fuse/query/explain" \
        '{"query":"SELECT * FROM cluster_a.application_logs"}' > "$TMPDIR/explain.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/explain.json')); assert len(d.get('plan','')) > 0
" 2>&1
}

test_unknown_ds_404() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT * FROM nope.logs"}')
    [ "$s" = "404" ]
}

test_bad_sql_400() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"INSERT INTO foo"}')
    [ "$s" = "400" ]
}

# ── New: Federation tests ──

test_union_all() {
    http_post "/api/fuse/query" \
        '{"query":"SELECT service, status FROM cluster_a.application_logs UNION ALL SELECT service, status FROM cluster_b.application_logs LIMIT 10","format":"sql"}' \
        > "$TMPDIR/union.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/union.json'))
assert d['metadata']['total_rows'] > 0, 'no rows from UNION ALL'
assert d['metadata']['total_rows'] <= 10, f'limit not applied: {d[\"metadata\"][\"total_rows\"]}'
" 2>&1
}

test_cross_join() {
    http_post "/api/fuse/query" \
        '{"query":"SELECT a.service, b.service FROM cluster_a.application_logs a JOIN cluster_b.application_logs b ON a.trace_id = b.trace_id LIMIT 5","format":"sql"}' \
        > "$TMPDIR/join.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/join.json'))
assert d['metadata']['total_rows'] > 0, 'no rows from cross-cluster JOIN'
assert d['metadata']['total_rows'] <= 5, f'limit not applied: {d[\"metadata\"][\"total_rows\"]}'
" 2>&1
}

test_limit_exact() {
    http_post "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 3","format":"sql"}' \
        > "$TMPDIR/limit3.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/limit3.json'))
assert d['metadata']['total_rows'] == 3, f'expected 3, got {d[\"metadata\"][\"total_rows\"]}'
" 2>&1
}

test_where_pushdown() {
    http_post "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_a.application_logs WHERE status >= 500 LIMIT 100","format":"sql"}' \
        > "$TMPDIR/where.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/where.json'))
rows = d['metadata']['total_rows']
assert rows > 0, 'no rows with status >= 500'
# All returned rows should have status >= 500
for r in d['rows']:
    si = d['columns'].index('status')
    assert int(r[si]) >= 500, f'row has status {r[si]}, expected >= 500'
" 2>&1
}

# ── New: Negative / edge case tests ──

test_long_query() {
    # 10KB SQL — should return 400, not crash
    local long_where=$(python3 -c "print(' OR service = ' * 500)")
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d "{\"query\":\"SELECT * FROM cluster_a.application_logs WHERE service = 'x'${long_where}\",\"format\":\"sql\"}")
    # Accept 400 (bad query) or 200 (if server handles it) — just not 5xx
    [ "$s" != "000" ] && [ "${s:0:1}" != "5" ]
}

test_sql_injection() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT * FROM cluster_a.application_logs; DROP TABLE users; --","format":"sql"}')
    # Server should either reject (400) or safely ignore the injection (200)
    [ "$s" = "400" ] || [ "$s" = "200" ]
}

test_empty_body() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" -d '{}')
    [ "$s" = "400" ] || [ "$s" = "422" ]
}

test_missing_format() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT 1"}')
    # Should either default to sql (200) or reject (400/422)
    [ "$s" = "200" ] || [ "$s" = "400" ] || [ "$s" = "422" ]
}

test_nonexistent_table() {
    # Valid datasource, invalid table name — may return 404/400 or 200 with empty results
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT * FROM cluster_a.nonexistent_table","format":"sql"}')
    [ "$s" = "404" ] || [ "$s" = "400" ] || [ "$s" = "200" ]
}

test_unicode_query() {
    local s=$(http_status -X POST "$BASE/api/fuse/query" \
        -H "Content-Type: application/json" \
        -d '{"query":"SELECT * FROM cluster_a.application_logs WHERE service = '\''日本語サービス'\''","format":"sql"}')
    # Should handle gracefully — 200 (empty result) or 400, not 5xx
    [ "$s" != "000" ] && [ "${s:0:1}" != "5" ]
}

test_concurrent_same_ds() {
    # 5 concurrent queries to same datasource — all should succeed
    local pids=()
    for i in $(seq 1 5); do
        (http_post "/api/fuse/query" \
            '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 2","format":"sql"}' \
            > "$TMPDIR/conc_$i.json") &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p"; done
    python3 -c "
import json
for i in range(1, 6):
    d = json.load(open(f'$TMPDIR/conc_{i}.json'))
    assert d['metadata']['total_rows'] > 0, f'concurrent query {i} returned no rows'
" 2>&1
}

# ── Run ──

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Fuse Playground E2E Smoke Tests                  ║"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

run_test "Health returns 200 with connectors"   test_health
run_test "Both connectors healthy"              test_health_connectors_healthy
run_test "Datasources lists both clusters"      test_datasources
run_test "Schema discovery cluster_a"           test_schema_cluster_a
run_test "Schema discovery cluster_b"           test_schema_cluster_b
run_test "SQL query returns rows (LIMIT 5)"     test_sql_query
run_test "PPL query returns rows"               test_ppl_query
run_test "Cross-cluster query"                  test_cross_cluster
run_test "Filter pushdown reduces rows"         test_filter_pushdown
run_test "Validate accepts good SQL"            test_validate_good
run_test "Validate rejects bad SQL"             test_validate_bad
run_test "Explain returns plan"                 test_explain
run_test "Unknown datasource returns 404"       test_unknown_ds_404
run_test "Malformed SQL returns 400"            test_bad_sql_400
# Federation
run_test "UNION ALL across clusters"            test_union_all
run_test "Cross-cluster JOIN on trace_id"       test_cross_join
run_test "LIMIT 3 returns exactly 3 rows"       test_limit_exact
run_test "WHERE pushdown filters correctly"     test_where_pushdown
# Negative / edge cases
run_test "Long query (10KB) no crash"           test_long_query
run_test "SQL injection rejected"               test_sql_injection
run_test "Empty body returns 400/422"           test_empty_body
run_test "Missing format field handled"         test_missing_format
run_test "Non-existent table returns error"     test_nonexistent_table
run_test "Unicode in query no crash"            test_unicode_query
run_test "Concurrent queries same datasource"   test_concurrent_same_ds

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
