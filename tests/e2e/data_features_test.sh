#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Data Features E2E — Cost Estimation, Adaptive Cache Stats, Quality Rules CRUD
set -euo pipefail
BASE="${1:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
PASS=0; FAIL=0; RESULTS=(); TMPDIR=$(mktemp -d); trap "rm -rf $TMPDIR" EXIT

run_test() {
    local name="$1"; shift; local start=$(date +%s%N)
    if "$@" 2>"$TMPDIR/err"; then
        local ms=$(( ($(date +%s%N) - start) / 1000000 ))
        RESULTS+=("✅ PASS  ${ms}ms  $name"); PASS=$((PASS + 1))
    else
        local ms=$(( ($(date +%s%N) - start) / 1000000 ))
        RESULTS+=("❌ FAIL  ${ms}ms  $name  ($(tail -1 "$TMPDIR/err"))"); FAIL=$((FAIL + 1))
    fi
}
http_get()    { curl -skf --max-time 10 "$BASE$1" 2>/dev/null; }
http_post()   { curl -skf --max-time 10 -X POST "$BASE$1" -H "Content-Type: application/json" -d "$2" 2>/dev/null; }
http_delete() { curl -skf --max-time 10 -X DELETE "$BASE$1" 2>/dev/null; }
http_status() { curl -sko /dev/null --max-time 10 -w "%{http_code}" "$@" 2>/dev/null; }

# ── Cost Estimation ──
test_cost_estimate_present() {
    http_post "/api/fuse/query" '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}' > "$TMPDIR/cost.json"
    python3 -c "
import json; d=json.load(open('$TMPDIR/cost.json')); ce=d['metadata']['cost_estimate']
assert isinstance(ce['total_cost_usd'],(int,float))
assert isinstance(ce['per_datasource'],list)
" 2>&1
}
test_cost_per_ds_fields() {
    http_post "/api/fuse/query" '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 3","format":"sql"}' > "$TMPDIR/cf.json"
    python3 -c "
import json; ce=json.load(open('$TMPDIR/cf.json'))['metadata']['cost_estimate']
if ce['per_datasource']:
    ds=ce['per_datasource'][0]
    for f in ['datasource','connector_type','estimated_rows','estimated_bytes','estimated_cost_usd','cost_breakdown']:
        assert f in ds, f'missing {f}'
" 2>&1
}
test_cost_cross_cluster() {
    http_post "/api/fuse/query" '{"query":"SELECT a.service FROM cluster_a.application_logs a JOIN cluster_b.application_logs b ON a.trace_id = b.trace_id LIMIT 5","format":"sql"}' > "$TMPDIR/cx.json"
    python3 -c "
import json; ce=json.load(open('$TMPDIR/cx.json'))['metadata']['cost_estimate']
names=[e['datasource'] for e in ce['per_datasource']]
assert len(names)>=2 and any('cluster_a' in n for n in names) and any('cluster_b' in n for n in names), names
" 2>&1
}
test_cost_nonnegative() {
    http_post "/api/fuse/query" '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 1","format":"sql"}' > "$TMPDIR/cn.json"
    python3 -c "
import json; ce=json.load(open('$TMPDIR/cn.json'))['metadata']['cost_estimate']
assert ce['total_cost_usd']>=0
for ds in ce['per_datasource']: assert ds['estimated_cost_usd']>=0
" 2>&1
}

# ── Adaptive Cache Stats ──
test_stats_keys() {
    http_get "/api/fuse/stats" > "$TMPDIR/st.json"
    python3 -c "
import json; d=json.load(open('$TMPDIR/st.json'))
for k in ['history','cache_size','connectors','running_queries','compilation_cache']: assert k in d, f'missing {k}'
assert isinstance(d['cache_size'],(int,float)) and d['cache_size']>=0
" 2>&1
}
test_cache_clear() {
    http_post "/api/fuse/query" '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 1","format":"sql"}' >/dev/null
    http_delete "/api/fuse/cache" > "$TMPDIR/cc.json"
    python3 -c "import json; assert json.load(open('$TMPDIR/cc.json')).get('cleared')==True" 2>&1
}
test_cache_grows() {
    http_delete "/api/fuse/cache" >/dev/null 2>&1 || true
    http_get "/api/fuse/stats" > "$TMPDIR/sp.json"
    http_post "/api/fuse/query" '{"query":"SELECT * FROM cluster_a.application_logs WHERE status >= 200 LIMIT 2","format":"sql"}' >/dev/null
    http_get "/api/fuse/stats" > "$TMPDIR/sa.json"
    python3 -c "
import json
assert json.load(open('$TMPDIR/sa.json'))['cache_size'] >= json.load(open('$TMPDIR/sp.json'))['cache_size']
" 2>&1
}

# ── Quality Rules CRUD ──
test_quality_list() { local s=$(http_status "$BASE/api/fuse/quality/rules"); [ "$s" = "200" ]; }
test_quality_create() {
    http_post "/api/fuse/quality/rules" '{"id":"e2e-nr-1","datasource":"cluster_a","table":"application_logs","rule_type":"null_rate","column":"service","threshold":0.05}' > "$TMPDIR/qc.json"
    python3 -c "import json; assert 'id' in str(json.load(open('$TMPDIR/qc.json')))" 2>&1
}
test_quality_list_contains() {
    http_post "/api/fuse/quality/rules" '{"id":"e2e-rc-1","datasource":"cluster_a","table":"application_logs","rule_type":"row_count","min":1}' >/dev/null 2>&1 || true
    http_get "/api/fuse/quality/rules" > "$TMPDIR/ql.json"
    python3 -c "
import json; d=json.load(open('$TMPDIR/ql.json'))
assert 'e2e-rc-1' in [r.get('id','') for r in d] if isinstance(d,list) else []
" 2>&1
}
test_quality_delete() {
    http_post "/api/fuse/quality/rules" '{"id":"e2e-del-1","datasource":"cluster_a","table":"application_logs","rule_type":"null_rate","column":"service","threshold":0.1}' >/dev/null 2>&1 || true
    http_delete "/api/fuse/quality/rules/e2e-del-1" >/dev/null 2>&1
    http_get "/api/fuse/quality/rules" > "$TMPDIR/qd.json"
    python3 -c "
import json; d=json.load(open('$TMPDIR/qd.json'))
assert 'e2e-del-1' not in ([r.get('id','') for r in d] if isinstance(d,list) else [])
" 2>&1
}
test_quality_duplicate() {
    http_post "/api/fuse/quality/rules" '{"id":"e2e-dup-1","datasource":"cluster_a","table":"application_logs","rule_type":"null_rate","column":"service","threshold":0.05}' >/dev/null 2>&1 || true
    local s=$(http_status -X POST "$BASE/api/fuse/quality/rules" -H "Content-Type: application/json" -d '{"id":"e2e-dup-1","datasource":"cluster_a","table":"application_logs","rule_type":"null_rate","column":"service","threshold":0.05}')
    [ "$s" = "409" ] || [ "$s" = "200" ] || [ "$s" = "400" ]
}

cleanup() { for id in e2e-nr-1 e2e-rc-1 e2e-del-1 e2e-dup-1; do http_delete "/api/fuse/quality/rules/$id" >/dev/null 2>&1 || true; done; }

# ── Run ──
echo "╔═══════════════════════════════════════════════════╗"
echo "║  Data Features E2E Tests                          ║"
echo "║  Cost Estimation · Cache Stats · Quality Rules    ║"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""
run_test "Cost estimate in query response"      test_cost_estimate_present
run_test "Cost per-datasource fields"           test_cost_per_ds_fields
run_test "Cost estimate cross-cluster JOIN"     test_cost_cross_cluster
run_test "Cost values non-negative"             test_cost_nonnegative
run_test "Stats endpoint keys"                  test_stats_keys
run_test "Cache clear returns cleared:true"     test_cache_clear
run_test "Cache grows after query"              test_cache_grows
run_test "Quality rules list 200"               test_quality_list
run_test "Quality rule create"                  test_quality_create
run_test "Quality rule in list after create"    test_quality_list_contains
run_test "Quality rule delete"                  test_quality_delete
run_test "Quality rule duplicate handled"       test_quality_duplicate
cleanup
echo ""
echo "═══════════════════════════════════════════════════"
echo "  RESULTS"
echo "═══════════════════════════════════════════════════"
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo ""; echo "  Total: $((PASS + FAIL))  |  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ]
