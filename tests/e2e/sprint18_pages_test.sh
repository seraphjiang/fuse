#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Sprint 18 E2E Tests — Schedules, Quality, Lineage, Replay pages & APIs
#
# Usage:
#   ./tests/e2e/sprint18_pages_test.sh [BASE_URL]

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
http_post() { curl -skf --max-time 10 -X POST "$BASE$1" -H "Content-Type: application/json" -d "$2" 2>/dev/null; }
http_delete() { curl -skf --max-time 10 -X DELETE "$BASE$1" 2>/dev/null; }
http_status() { curl -sko /dev/null --max-time 10 -w "%{http_code}" "$@" 2>/dev/null; }

# ── Playground Pages (HTML served) ──

test_schedules_page() {
    local s=$(http_status "$BASE/schedules.html")
    [ "$s" = "200" ]
}

test_quality_page() {
    local s=$(http_status "$BASE/quality.html")
    [ "$s" = "200" ]
}

test_lineage_page() {
    local s=$(http_status "$BASE/lineage.html")
    [ "$s" = "200" ]
}

test_replay_page() {
    local s=$(http_status "$BASE/replay.html")
    [ "$s" = "200" ]
}

# ── Lineage API (#1840) ──

test_lineage_extract_sql() {
    http_post "/api/fuse/lineage" \
        '{"query":"SELECT l.id FROM cluster_a.logs l JOIN dynamodb.users u ON l.uid = u.uid","format":"sql"}' \
        > "$TMPDIR/lineage.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/lineage.json'))
nodes = d['nodes']
sources = [n for n in nodes if n['node_type'] == 'source']
assert len(sources) == 2, f'expected 2 sources, got {len(sources)}'
assert any(n['node_type'] == 'sink' for n in nodes), 'no sink node'
assert any(n['label'] == 'JOIN' for n in nodes), 'no JOIN transform'
assert len(d['edges']) > 0, 'no edges'
" 2>&1
}

test_lineage_extract_ppl() {
    http_post "/api/fuse/lineage" \
        '{"query":"source = cluster_a.logs | where status >= 500 | lookup dynamodb.users user_id","format":"ppl"}' \
        > "$TMPDIR/lineage_ppl.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/lineage_ppl.json'))
sources = [n for n in d['nodes'] if n['node_type'] == 'source']
assert len(sources) == 2, f'expected 2 PPL sources, got {len(sources)}'
" 2>&1
}

test_lineage_single_source() {
    http_post "/api/fuse/lineage" \
        '{"query":"SELECT * FROM cluster_a.logs WHERE status = 500","format":"sql"}' \
        > "$TMPDIR/lineage_single.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/lineage_single.json'))
sources = [n for n in d['nodes'] if n['node_type'] == 'source']
assert len(sources) == 1, f'expected 1 source, got {len(sources)}'
assert any(n['label'] == 'FILTER' for n in d['nodes']), 'no FILTER transform'
" 2>&1
}

# ── Schedules API (#1800) ──

test_schedules_list() {
    local s=$(http_status "$BASE/api/fuse/schedules")
    [ "$s" = "200" ]
}

test_schedules_crud() {
    # Create
    http_post "/api/fuse/schedules" \
        '{"id":"test-sched-1","query":"SELECT count(*) FROM cluster_a.application_logs","cron":"*/5 * * * *","format":"sql"}' \
        > "$TMPDIR/sched_create.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/sched_create.json'))
assert d.get('id') == 'test-sched-1' or 'id' in str(d), f'unexpected: {d}'
" 2>&1

    # List should include it
    http_get "/api/fuse/schedules" > "$TMPDIR/sched_list.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/sched_list.json'))
ids = [s.get('id','') for s in d] if isinstance(d, list) else []
assert 'test-sched-1' in ids, f'schedule not in list: {ids}'
" 2>&1

    # Delete
    http_delete "/api/fuse/schedules/test-sched-1" > /dev/null 2>&1 || true
}

# ── Data Quality API (#1801) ──

test_quality_rules_list() {
    local s=$(http_status "$BASE/api/fuse/quality/rules")
    [ "$s" = "200" ]
}

test_quality_rule_crud() {
    http_post "/api/fuse/quality/rules" \
        '{"id":"test-rule-1","datasource":"cluster_a","table":"application_logs","rule_type":"null_rate","column":"service","threshold":0.05}' \
        > "$TMPDIR/qr_create.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/qr_create.json'))
assert 'id' in str(d), f'unexpected: {d}'
" 2>&1

    # Cleanup
    http_delete "/api/fuse/quality/rules/test-rule-1" > /dev/null 2>&1 || true
}

# ── Replay API (#1812) ──

test_replay_list() {
    local s=$(http_status "$BASE/api/fuse/replay")
    [ "$s" = "200" ]
}

test_replay_record() {
    http_post "/api/fuse/replay" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}' \
        > "$TMPDIR/replay_rec.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/replay_rec.json'))
assert 'id' in str(d) or 'recorded' in str(d).lower(), f'unexpected: {d}'
" 2>&1
}

# ── Run ──

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Sprint 18 E2E Tests — New Pages & APIs           ║"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Playground pages
run_test "Schedules page serves 200"            test_schedules_page
run_test "Quality page serves 200"              test_quality_page
run_test "Lineage page serves 200"              test_lineage_page
run_test "Replay page serves 200"               test_replay_page

# Lineage API
run_test "Lineage: SQL JOIN extraction"         test_lineage_extract_sql
run_test "Lineage: PPL lookup extraction"       test_lineage_extract_ppl
run_test "Lineage: single source + filter"      test_lineage_single_source

# Schedules API
run_test "Schedules: list endpoint"             test_schedules_list
run_test "Schedules: create/list/delete"        test_schedules_crud

# Quality API
run_test "Quality: rules list endpoint"         test_quality_rules_list
run_test "Quality: rule CRUD"                   test_quality_rule_crud

# Replay API
run_test "Replay: list endpoint"                test_replay_list
run_test "Replay: record query"                 test_replay_record

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
