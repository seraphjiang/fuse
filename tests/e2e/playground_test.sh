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
