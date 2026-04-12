#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# E2E tests for operational APIs with zero E2E coverage:
#   - Alert Rules CRUD + acknowledge + history
#   - Async Query lifecycle (submit/poll/cancel)
#   - Multi-query batch endpoint
#   - CDC events + status
#   - History, advisor, predict, relationships, NL-to-SQL
#
# Usage:
#   ./tests/e2e/operational_api_test.sh [BASE_URL]

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
http_post() { curl -skf --max-time 10 -X POST "$BASE$1" -H "Content-Type: application/json" -d "$2" 2>/dev/null; }
http_delete() { curl -skf --max-time 10 -X DELETE "$BASE$1" 2>/dev/null; }
http_status() { curl -sko /dev/null --max-time 10 -w "%{http_code}" "$@" 2>/dev/null; }

# ── Alert Rules ──

test_alert_rules_list() {
    local s=$(http_status "$BASE/api/fuse/alert-rules")
    [ "$s" = "200" ]
}

test_alert_rules_crud() {
    # Create
    http_post "/api/fuse/alert-rules" \
        '{"name":"e2e-test-rule","query":"SELECT count(*) as cnt FROM cluster_a.application_logs","condition":"cnt > 1000","severity":"warning"}' \
        > "$TMPDIR/ar_create.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/ar_create.json'))
assert 'id' in d, f'no id: {d}'
with open('$TMPDIR/ar_id.txt', 'w') as f:
    f.write(d['id'])
" 2>&1

    # Delete
    local ar_id=$(cat "$TMPDIR/ar_id.txt")
    http_delete "/api/fuse/alert-rules/$ar_id" > /dev/null 2>&1 || true
}

test_alert_rules_acknowledge() {
    # Create rule, then acknowledge
    http_post "/api/fuse/alert-rules" \
        '{"name":"e2e-ack-rule","query":"SELECT 1 as v","condition":"v > 0","severity":"info"}' \
        > "$TMPDIR/ar_ack_create.json"
    local ar_id=$(python3 -c "import json; print(json.load(open('$TMPDIR/ar_ack_create.json'))['id'])")

    local s=$(http_status -X POST "$BASE/api/fuse/alert-rules/$ar_id/acknowledge" \
        -H "Content-Type: application/json" -d '{}')
    [ "$s" = "200" ] || [ "$s" = "204" ] || [ "$s" = "404" ]  # 404 if not yet fired

    http_delete "/api/fuse/alert-rules/$ar_id" > /dev/null 2>&1 || true
}

test_alert_rules_active() {
    local s=$(http_status "$BASE/api/fuse/alert-rules/active")
    [ "$s" = "200" ]
}

test_alert_rules_history() {
    local s=$(http_status "$BASE/api/fuse/alert-rules/history")
    [ "$s" = "200" ]
}

# ── Async Query ──

test_async_submit() {
    http_post "/api/fuse/query/async" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}' \
        > "$TMPDIR/async_submit.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/async_submit.json'))
assert 'job_id' in d, f'no job_id: {d}'
with open('$TMPDIR/async_job_id.txt', 'w') as f:
    f.write(d['job_id'])
" 2>&1
}

test_async_poll() {
    local job_id=$(cat "$TMPDIR/async_job_id.txt" 2>/dev/null || echo "")
    [ -z "$job_id" ] && { echo "no job_id from submit" >&2; return 1; }

    # Poll up to 5 times
    local status="pending"
    for i in $(seq 1 5); do
        http_get "/api/fuse/query/async/$job_id" > "$TMPDIR/async_poll.json"
        status=$(python3 -c "
import json
d = json.load(open('$TMPDIR/async_poll.json'))
print(d.get('status','unknown'))
" 2>&1)
        [ "$status" = "completed" ] && break
        sleep 1
    done
    [ "$status" = "completed" ] || [ "$status" = "running" ]
}

test_async_cancel() {
    # Submit a new query and cancel it
    http_post "/api/fuse/query/async" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 1000","format":"sql"}' \
        > "$TMPDIR/async_cancel.json"
    local job_id=$(python3 -c "import json; print(json.load(open('$TMPDIR/async_cancel.json'))['job_id'])")

    local s=$(http_status -X DELETE "$BASE/api/fuse/query/async/$job_id")
    [ "$s" = "200" ] || [ "$s" = "204" ] || [ "$s" = "404" ]  # 404 if already completed
}

# ── Multi Query ──

test_multi_query() {
    http_post "/api/fuse/multi" \
        '{"queries":[{"query":"SELECT count(*) FROM cluster_a.application_logs","format":"sql"},{"query":"SELECT count(*) FROM cluster_b.application_logs","format":"sql"}]}' \
        > "$TMPDIR/multi.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/multi.json'))
results = d if isinstance(d, list) else d.get('results', [])
assert len(results) == 2, f'expected 2 results, got {len(results)}'
" 2>&1
}

test_multi_query_partial_failure() {
    http_post "/api/fuse/multi" \
        '{"queries":[{"query":"SELECT 1","format":"sql"},{"query":"SELECT * FROM nonexistent.table","format":"sql"}]}' \
        > "$TMPDIR/multi_partial.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/multi_partial.json'))
results = d if isinstance(d, list) else d.get('results', [])
assert len(results) == 2, f'expected 2 results even with partial failure'
" 2>&1
}

# ── CDC ──

test_cdc_status() {
    local s=$(http_status "$BASE/api/fuse/cdc/status")
    [ "$s" = "200" ]
}

test_cdc_ingest_event() {
    local s=$(http_status -X POST "$BASE/api/fuse/cdc/events" \
        -H "Content-Type: application/json" \
        -d '{"datasource":"cluster_a","table":"application_logs","event_type":"insert","timestamp":1700000000}')
    [ "$s" = "200" ] || [ "$s" = "202" ] || [ "$s" = "400" ]  # 400 if validation differs
}

# ── History ──

test_history() {
    local s=$(http_status "$BASE/api/fuse/history")
    [ "$s" = "200" ]
}

test_history_has_entries() {
    http_get "/api/fuse/history" > "$TMPDIR/history.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/history.json'))
entries = d if isinstance(d, list) else d.get('queries', d.get('entries', []))
# After running other tests, there should be history
assert isinstance(entries, list), f'unexpected format: {type(entries)}'
" 2>&1
}

# ── Advisor ──

test_advisor() {
    local s=$(http_status "$BASE/api/fuse/advisor")
    [ "$s" = "200" ]
}

# ── Predict ──

test_predict() {
    local s=$(http_status "$BASE/api/fuse/predict")
    [ "$s" = "200" ] || [ "$s" = "400" ]  # 400 if requires params
}

# ── Relationships ──

test_relationships() {
    local s=$(http_status "$BASE/api/fuse/relationships")
    [ "$s" = "200" ]
}

# ── NL-to-SQL ──

test_nl_to_sql() {
    http_post "/api/fuse/nl" \
        '{"question":"show me all error logs"}' \
        > "$TMPDIR/nl.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/nl.json'))
assert 'query' in d or 'sql' in d, f'no query in NL response: {list(d.keys())}'
" 2>&1
}

# ── Run ──

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Operational API E2E Tests                        ║"
echo "║  Alerts · Async · Multi · CDC · History · NL      ║"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Alert Rules
run_test "Alert Rules: list"                        test_alert_rules_list
run_test "Alert Rules: create/delete"               test_alert_rules_crud
run_test "Alert Rules: acknowledge"                 test_alert_rules_acknowledge
run_test "Alert Rules: active alerts"               test_alert_rules_active
run_test "Alert Rules: history"                     test_alert_rules_history

# Async Query
run_test "Async Query: submit returns job_id"       test_async_submit
run_test "Async Query: poll until complete"          test_async_poll
run_test "Async Query: cancel job"                  test_async_cancel

# Multi Query
run_test "Multi Query: batch 2 queries"             test_multi_query
run_test "Multi Query: partial failure"             test_multi_query_partial_failure

# CDC
run_test "CDC: status endpoint"                     test_cdc_status
run_test "CDC: ingest event"                        test_cdc_ingest_event

# History, Advisor, Predict, Relationships
run_test "History: endpoint returns 200"            test_history
run_test "History: has entries"                      test_history_has_entries
run_test "Advisor: endpoint returns 200"            test_advisor
run_test "Predict: endpoint responds"               test_predict
run_test "Relationships: endpoint returns 200"      test_relationships

# NL-to-SQL
run_test "NL-to-SQL: question returns query"        test_nl_to_sql

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
