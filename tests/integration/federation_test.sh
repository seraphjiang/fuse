#!/usr/bin/env bash
# #1030 Federation integration tests — verify cross-cluster queries via Fuse-to-Fuse connector.
set -euo pipefail

FUSE_A="${FUSE_A_URL:-http://localhost:9400}"
FUSE_B="${FUSE_B_URL:-http://localhost:9401}"
PASS=0 FAIL=0

run() {
    local name="$1"; shift
    if "$@" >/dev/null 2>&1; then
        echo "  ✅ $name"
        ((PASS++))
    else
        echo "  ❌ $name"
        ((FAIL++))
    fi
}

check_json() {
    local name="$1" url="$2" payload="$3" field="$4"
    local resp
    resp=$(curl -sf -X POST "$url/api/fuse/query" \
        -H 'Content-Type: application/json' \
        -d "$payload" 2>/dev/null || echo '{}')
    if echo "$resp" | grep -q "$field"; then
        echo "  ✅ $name"
        ((PASS++))
    else
        echo "  ❌ $name (missing '$field' in response)"
        ((FAIL++))
    fi
}

echo "=== Federation Integration Tests ==="

# --- Instance Health ---
echo ""
echo "--- Instance Health ---"
run "Fuse A is healthy" curl -sf "$FUSE_A/api/fuse/health"
run "Fuse B is healthy" curl -sf "$FUSE_B/api/fuse/health"

# --- Datasource Discovery ---
echo ""
echo "--- Datasource Discovery ---"
run "Fuse A lists datasources" curl -sf "$FUSE_A/api/fuse/datasources"
run "Fuse B lists datasources" curl -sf "$FUSE_B/api/fuse/datasources"

# --- Cross-cluster Query ---
echo ""
echo "--- Cross-cluster Queries ---"

# Query local datasource on A
check_json "Local query on A" "$FUSE_A" \
    '{"query": "SELECT 1 AS v", "format": "sql"}' \
    '"rows"'

# Query local datasource on B
check_json "Local query on B" "$FUSE_B" \
    '{"query": "SELECT 1 AS v", "format": "sql"}' \
    '"rows"'

# Query remote datasource via federation (A queries B's data)
check_json "Federated query A→B" "$FUSE_A" \
    '{"query": "SELECT * FROM remote_b.test_table LIMIT 5", "format": "sql"}' \
    '"rows"'

# Cross-cluster JOIN
check_json "Cross-cluster JOIN" "$FUSE_A" \
    '{"query": "SELECT a.v, b.v FROM local.test a JOIN remote_b.test b ON a.id = b.id LIMIT 5", "format": "sql"}' \
    '"rows"'

# --- Federation Topology ---
echo ""
echo "--- Federation Topology ---"
run "Federation endpoint exists on A" curl -sf "$FUSE_A/api/fuse/federation"

# --- EXPLAIN across federation ---
echo ""
echo "--- Federated EXPLAIN ---"
check_json "EXPLAIN federated query" "$FUSE_A" \
    '{"query": "EXPLAIN SELECT * FROM remote_b.test_table LIMIT 5", "format": "sql"}' \
    '"plan"'

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
