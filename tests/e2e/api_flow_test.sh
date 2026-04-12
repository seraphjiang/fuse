#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Fuse E2E API Flow Tests — SQL, PPL, EXPLAIN, cursor pagination, Sprint 18
# Usage: ./tests/e2e/api_flow_test.sh [BASE_URL]

set -euo pipefail

BASE="${1:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
PASS=0 FAIL=0 RESULTS=()
TMPDIR=$(mktemp -d); trap "rm -rf $TMPDIR" EXIT

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

# 1. SQL
test_sql_basic() {
    http_post "/api/fuse/query" '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}' > "$TMPDIR/sql.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/sql.json'))
assert len(d['columns']) > 0, 'no columns'
assert 0 < d['metadata']['total_rows'] <= 5, f'rows: {d[\"metadata\"][\"total_rows\"]}'
"
}
test_sql_filter() {
    http_post "/api/fuse/query" '{"query":"SELECT service,status FROM cluster_a.application_logs WHERE status>=500 LIMIT 10","format":"sql"}' > "$TMPDIR/sql_f.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/sql_f.json'))
assert d['metadata']['total_rows'] > 0
si = d['columns'].index('status')
for r in d['rows']: assert int(r[si]) >= 500, f'bad status {r[si]}'
"
}

# 2. PPL
test_ppl_basic() {
    http_post "/api/fuse/query" '{"query":"source = cluster_a.application_logs | head 5","format":"ppl"}' > "$TMPDIR/ppl.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/ppl.json'))
assert len(d['columns']) > 0 and 0 < d['metadata']['total_rows'] <= 5
"
}
test_ppl_where() {
    http_post "/api/fuse/query" '{"query":"source = cluster_a.application_logs | where status >= 500 | head 10","format":"ppl"}' > "$TMPDIR/ppl_w.json"
    python3 -c "import json; d = json.load(open('$TMPDIR/ppl_w.json')); assert d['metadata']['total_rows'] > 0"
}

# 3. EXPLAIN
test_explain_endpoint() {
    http_post "/api/fuse/query/explain" '{"query":"SELECT * FROM cluster_a.application_logs"}' > "$TMPDIR/expl.json"
    python3 -c "import json; d = json.load(open('$TMPDIR/expl.json')); assert len(d.get('plan','')) > 0, 'empty plan'"
}
test_explain_prefix() {
    http_post "/api/fuse/query" '{"query":"EXPLAIN SELECT service,count(*) FROM cluster_a.application_logs GROUP BY service","format":"sql"}' > "$TMPDIR/expl_p.json"
    python3 -c "import json; d = json.load(open('$TMPDIR/expl_p.json')); assert d['metadata']['total_rows'] > 0"
}

# 4. Cursor Pagination
test_cursor_pagination() {
    http_post "/api/fuse/query" '{"query":"SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC","format":"sql","page_size":3}' > "$TMPDIR/p1.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/p1.json'))
assert d['metadata']['total_rows'] == 3, f'page1: {d[\"metadata\"][\"total_rows\"]}'
c = d['metadata'].get('next_cursor'); assert c, 'no next_cursor'
open('$TMPDIR/cursor.txt','w').write(c)
"
    local cursor=$(cat "$TMPDIR/cursor.txt")
    http_post "/api/fuse/query" "{\"query\":\"SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC\",\"format\":\"sql\",\"page_size\":3,\"cursor\":\"$cursor\"}" > "$TMPDIR/p2.json"
    python3 -c "
import json
p1,p2 = json.load(open('$TMPDIR/p1.json')), json.load(open('$TMPDIR/p2.json'))
assert p2['metadata']['total_rows'] == 3
assert p1['rows'] != p2['rows'], 'pages identical'
"
}

# 5. Sprint 18: Schedules
test_schedules_list() { [ "$(http_status "$BASE/api/fuse/schedules")" = "200" ]; }
test_schedules_crud() {
    http_post "/api/fuse/schedules" '{"id":"e2e-flow-sched","query":"SELECT count(*) FROM cluster_a.application_logs","cron":"*/10 * * * *","format":"sql"}' > "$TMPDIR/sc.json"
    python3 -c "import json; d = json.load(open('$TMPDIR/sc.json')); assert 'id' in str(d)"
    http_get "/api/fuse/schedules" > "$TMPDIR/sl.json"
    python3 -c "import json; d = json.load(open('$TMPDIR/sl.json')); assert 'e2e-flow-sched' in [s.get('id','') for s in d]"
    http_delete "/api/fuse/schedules/e2e-flow-sched" > /dev/null 2>&1 || true
}

# 5. Sprint 18: Quality Rules
test_quality_rules_list() { [ "$(http_status "$BASE/api/fuse/quality/rules")" = "200" ]; }
test_quality_rule_crud() {
    http_post "/api/fuse/quality/rules" '{"id":"e2e-flow-rule","datasource":"cluster_a","table":"application_logs","rule_type":"null_rate","column":"service","threshold":0.1}' > "$TMPDIR/qc.json"
    python3 -c "import json; d = json.load(open('$TMPDIR/qc.json')); assert 'id' in str(d)"
    http_delete "/api/fuse/quality/rules/e2e-flow-rule" > /dev/null 2>&1 || true
}

# 5. Sprint 18: Lineage
test_lineage_sql() {
    http_post "/api/fuse/lineage" '{"query":"SELECT l.trace_id FROM cluster_a.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id","format":"sql"}' > "$TMPDIR/lin.json"
    python3 -c "
import json; d = json.load(open('$TMPDIR/lin.json'))
assert len([n for n in d['nodes'] if n['node_type']=='source']) == 2
assert len(d['edges']) > 0
"
}
test_lineage_ppl() {
    http_post "/api/fuse/lineage" '{"query":"source = cluster_a.application_logs | where status >= 500 | lookup dynamodb.users user_id","format":"ppl"}' > "$TMPDIR/lin_p.json"
    python3 -c "import json; d = json.load(open('$TMPDIR/lin_p.json')); assert len([n for n in d['nodes'] if n['node_type']=='source']) == 2"
}

# 5. Sprint 18: Replay
test_replay_list() { [ "$(http_status "$BASE/api/fuse/replay/recordings")" = "200" ]; }
test_replay_record() {
    http_post "/api/fuse/replay/record" '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 3","format":"sql"}' > "$TMPDIR/rr.json"
    python3 -c "import json; d = json.load(open('$TMPDIR/rr.json')); assert 'id' in str(d) or 'recorded' in str(d).lower()"
}

# ── Run ──
echo "╔═══════════════════════════════════════════════════╗"
echo "║  Fuse E2E API Flow Tests                          ║"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

run_test "SQL: basic query LIMIT 5"             test_sql_basic
run_test "SQL: WHERE filter pushdown"           test_sql_filter
run_test "PPL: basic query head 5"              test_ppl_basic
run_test "PPL: where clause"                    test_ppl_where
run_test "EXPLAIN: /query/explain endpoint"     test_explain_endpoint
run_test "EXPLAIN: EXPLAIN prefix in SQL"       test_explain_prefix
run_test "Pagination: cursor first→second"      test_cursor_pagination
run_test "Schedules: list 200"                  test_schedules_list
run_test "Schedules: create/list/delete"        test_schedules_crud
run_test "Quality: rules list 200"              test_quality_rules_list
run_test "Quality: rule create/delete"          test_quality_rule_crud
run_test "Lineage: SQL JOIN extraction"         test_lineage_sql
run_test "Lineage: PPL lookup extraction"       test_lineage_ppl
run_test "Replay: list recordings"              test_replay_list
run_test "Replay: record a query"               test_replay_record

echo ""
echo "═══════════════════════════════════════════════════"
echo "  RESULTS"
echo "═══════════════════════════════════════════════════"
for r in "${RESULTS[@]}"; do echo "  $r"; done
echo ""
echo "  Total: $((PASS + FAIL))  |  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ]
