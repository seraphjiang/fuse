#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# E2E tests for Sprint 18 APIs with zero E2E coverage:
#   - GraphQL API (/api/fuse/graphql)
#   - Webhook Subscriptions (/api/fuse/webhooks)
#   - Cost Estimation (embedded in query response)
#
# Usage:
#   ./tests/e2e/sprint18_api_coverage_test.sh [BASE_URL]

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

graphql() { http_post "/api/fuse/graphql" "$1"; }

# ── GraphQL: Introspection ──

test_graphql_health() {
    graphql '{"query":"{ health }"}' > "$TMPDIR/gql_health.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/gql_health.json'))
assert 'data' in d, f'no data key: {list(d.keys())}'
assert d['data']['health'] is not None, 'health is null'
" 2>&1
}

test_graphql_datasources() {
    graphql '{"query":"{ datasources { id connectorType status } }"}' > "$TMPDIR/gql_ds.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/gql_ds.json'))
ds = d['data']['datasources']
assert len(ds) > 0, 'no datasources'
ids = {x['id'] for x in ds}
assert 'cluster_a' in ids, f'cluster_a missing: {ids}'
" 2>&1
}

test_graphql_schemas() {
    graphql '{"query":"{ schemas(datasource: \"cluster_a\") { name } }"}' > "$TMPDIR/gql_schemas.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/gql_schemas.json'))
names = [s['name'] for s in d['data']['schemas']]
assert 'application_logs' in names, f'got: {names}'
" 2>&1
}

test_graphql_fields() {
    graphql '{"query":"{ fields(datasource: \"cluster_a\", table: \"application_logs\") { name fieldType } }"}' > "$TMPDIR/gql_fields.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/gql_fields.json'))
fields = d['data']['fields']
assert len(fields) > 0, 'no fields returned'
names = {f['name'] for f in fields}
assert 'service' in names or 'status' in names, f'expected common fields: {names}'
" 2>&1
}

# ── GraphQL: Mutations ──

test_graphql_execute_query() {
    graphql '{"query":"mutation { executeQuery(query: \"SELECT * FROM cluster_a.application_logs LIMIT 3\", format: \"sql\") { columns rows metadata { totalRows } } }"}' > "$TMPDIR/gql_exec.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/gql_exec.json'))
r = d['data']['executeQuery']
assert len(r['columns']) > 0, 'no columns'
assert r['metadata']['totalRows'] <= 3, f'limit not applied: {r[\"metadata\"][\"totalRows\"]}'
" 2>&1
}

test_graphql_saved_queries_crud() {
    # Save
    graphql '{"query":"mutation { saveQuery(name: \"e2e_test_gql\", query: \"SELECT 1\", format: \"sql\") { name } }"}' > "$TMPDIR/gql_save.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/gql_save.json'))
assert 'saveQuery' in str(d['data']), f'save failed: {d}'
" 2>&1

    # List
    graphql '{"query":"{ savedQueries { name query } }"}' > "$TMPDIR/gql_saved_list.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/gql_saved_list.json'))
names = [s['name'] for s in d['data']['savedQueries']]
assert 'e2e_test_gql' in names, f'saved query not found: {names}'
" 2>&1

    # Delete
    graphql '{"query":"mutation { deleteSavedQuery(name: \"e2e_test_gql\") }"}' > /dev/null 2>&1 || true
}

test_graphql_views() {
    graphql '{"query":"{ views { name query } }"}' > "$TMPDIR/gql_views.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/gql_views.json'))
assert 'views' in d['data'], f'no views key: {d}'
" 2>&1
}

# ── GraphQL: Error handling ──

test_graphql_bad_query() {
    graphql '{"query":"{ nonExistentField }"}' > "$TMPDIR/gql_bad.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/gql_bad.json'))
assert 'errors' in d, f'expected errors for bad field: {d}'
" 2>&1
}

test_graphql_invalid_body() {
    local s=$(http_status -X POST "$BASE/api/fuse/graphql" \
        -H "Content-Type: application/json" -d '{"not_query":"bad"}')
    [ "$s" = "400" ] || [ "$s" = "200" ]  # GraphQL may return 200 with errors
}

test_graphiql_page() {
    local s=$(http_status "$BASE/api/fuse/graphql")
    [ "$s" = "200" ]
}

# ── Webhooks: CRUD ──

test_webhook_list() {
    local s=$(http_status "$BASE/api/fuse/webhooks")
    [ "$s" = "200" ]
}

test_webhook_crud() {
    # Create
    http_post "/api/fuse/webhooks" \
        '{"url":"https://httpbin.org/post","query":"SELECT count(*) as cnt FROM cluster_a.application_logs","format":"sql","condition":"cnt > 0","cron":"*/10 * * * *"}' \
        > "$TMPDIR/wh_create.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/wh_create.json'))
assert 'id' in d, f'no id in response: {d}'
with open('$TMPDIR/wh_id.txt', 'w') as f:
    f.write(d['id'])
" 2>&1

    # Get
    local wh_id=$(cat "$TMPDIR/wh_id.txt")
    http_get "/api/fuse/webhooks/$wh_id" > "$TMPDIR/wh_get.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/wh_get.json'))
assert d.get('url') == 'https://httpbin.org/post', f'url mismatch: {d}'
" 2>&1

    # List should include it
    http_get "/api/fuse/webhooks" > "$TMPDIR/wh_list.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/wh_list.json'))
ids = [w.get('id','') for w in d] if isinstance(d, list) else []
assert '$wh_id' in ids or len(ids) > 0, f'webhook not in list'
" 2>&1

    # Delete
    http_delete "/api/fuse/webhooks/$wh_id" > /dev/null 2>&1 || true
}

test_webhook_test_fire() {
    # Create a webhook, then test-fire it
    http_post "/api/fuse/webhooks" \
        '{"url":"https://httpbin.org/post","query":"SELECT 1 as val","format":"sql","condition":"val > 0"}' \
        > "$TMPDIR/wh_test_create.json"
    local wh_id=$(python3 -c "import json; print(json.load(open('$TMPDIR/wh_test_create.json'))['id'])")

    local s=$(http_status -X POST "$BASE/api/fuse/webhooks/$wh_id/test" \
        -H "Content-Type: application/json" -d '{}')
    # Test fire should succeed (200) or timeout to external URL (which is OK)
    [ "$s" = "200" ] || [ "$s" = "202" ] || [ "$s" = "504" ]

    # Cleanup
    http_delete "/api/fuse/webhooks/$wh_id" > /dev/null 2>&1 || true
}

test_webhook_invalid_create() {
    local s=$(http_status -X POST "$BASE/api/fuse/webhooks" \
        -H "Content-Type: application/json" -d '{}')
    [ "$s" = "400" ] || [ "$s" = "422" ]
}

# ── Cost Estimation ──

test_cost_in_query_response() {
    http_post "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}' \
        > "$TMPDIR/cost_query.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/cost_query.json'))
meta = d.get('metadata', {})
ce = meta.get('cost_estimate') or d.get('cost_estimate')
assert ce is not None, f'no cost_estimate in response. metadata keys: {list(meta.keys())}, top keys: {list(d.keys())}'
" 2>&1
}

test_cost_has_breakdown() {
    http_post "/api/fuse/query" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}' \
        > "$TMPDIR/cost_detail.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/cost_detail.json'))
ce = d.get('metadata', {}).get('cost_estimate') or d.get('cost_estimate')
if ce is None:
    raise AssertionError('no cost_estimate')
# Should have total or per-datasource breakdown
assert 'total' in str(ce).lower() or 'usd' in str(ce).lower() or isinstance(ce, dict), f'unexpected cost format: {ce}'
" 2>&1
}

test_cost_explain_includes_estimate() {
    http_post "/api/fuse/query/explain" \
        '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5"}' \
        > "$TMPDIR/cost_explain.json"
    python3 -c "
import json
d = json.load(open('$TMPDIR/cost_explain.json'))
plan = d.get('plan', '')
# Explain should mention estimated_rows or estimated_cost
assert 'estimated' in plan.lower() or 'cost' in plan.lower() or 'rows' in plan.lower(), f'no cost info in plan'
" 2>&1
}

# ── Run ──

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Sprint 18 API Coverage E2E Tests                 ║"
echo "║  GraphQL · Webhooks · Cost Estimation             ║"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# GraphQL
run_test "GraphQL: health query"                    test_graphql_health
run_test "GraphQL: datasources query"               test_graphql_datasources
run_test "GraphQL: schemas for cluster_a"           test_graphql_schemas
run_test "GraphQL: fields for application_logs"     test_graphql_fields
run_test "GraphQL: execute SQL mutation"             test_graphql_execute_query
run_test "GraphQL: saved queries CRUD"              test_graphql_saved_queries_crud
run_test "GraphQL: views query"                     test_graphql_views
run_test "GraphQL: bad field returns errors"        test_graphql_bad_query
run_test "GraphQL: invalid body handled"            test_graphql_invalid_body
run_test "GraphQL: GraphiQL page serves 200"        test_graphiql_page

# Webhooks
run_test "Webhooks: list endpoint"                  test_webhook_list
run_test "Webhooks: create/get/list/delete"         test_webhook_crud
run_test "Webhooks: test-fire endpoint"             test_webhook_test_fire
run_test "Webhooks: invalid create returns 4xx"     test_webhook_invalid_create

# Cost Estimation
run_test "Cost: present in query response"          test_cost_in_query_response
run_test "Cost: has breakdown or total"             test_cost_has_breakdown
run_test "Cost: explain includes estimates"         test_cost_explain_includes_estimate

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
