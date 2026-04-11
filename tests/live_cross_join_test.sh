#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# #1601 — Cross-datasource JOIN test on live site
# Usage: FUSE_URL=https://fuse.huanji.profile.aws.dev ./tests/live_cross_join_test.sh

set -euo pipefail

FUSE_URL="${FUSE_URL:-http://localhost:9400}"
PASS=0
FAIL=0

query() {
    local desc="$1" sql="$2"
    local resp
    resp=$(curl -sf -X POST "$FUSE_URL/api/fuse/query" \
        -H 'Content-Type: application/json' \
        -d "{\"query\": \"$sql\", \"format\": \"sql\"}" 2>&1) || {
        echo "FAIL: $desc — connection error"
        FAIL=$((FAIL + 1))
        return
    }
    local rows
    rows=$(echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('metadata',{}).get('total_rows', len(d.get('rows',[]))))" 2>/dev/null)
    local error
    error=$(echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',''))" 2>/dev/null)

    if [ -n "$error" ] && [ "$error" != "" ] && [ "$error" != "None" ]; then
        echo "FAIL: $desc — $error"
        FAIL=$((FAIL + 1))
    elif [ "${rows:-0}" -ge 0 ] 2>/dev/null; then
        echo "PASS: $desc — $rows rows"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $desc — unexpected response"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Cross-datasource JOIN tests against $FUSE_URL ==="
echo ""

# 1. Health check
echo "--- Preflight ---"
curl -sf "$FUSE_URL/api/fuse/health" > /dev/null && echo "PASS: Health check" && PASS=$((PASS + 1)) || { echo "FAIL: Health check"; FAIL=$((FAIL + 1)); }

# 2. Datasources available
DS=$(curl -sf "$FUSE_URL/api/fuse/datasources" | python3 -c "import sys,json; print(' '.join(d['id'] for d in json.load(sys.stdin)))" 2>/dev/null)
echo "Datasources: $DS"

echo ""
echo "--- Cross-datasource JOINs ---"

# 3. OpenSearch + DynamoDB JOIN
query "OS+DDB JOIN" \
    "SELECT l.service, u.name FROM cluster_b.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id LIMIT 10"

# 4. OpenSearch self-join (different indices)
query "OS self-join" \
    "SELECT a.service, a.status FROM cluster_b.application_logs a JOIN cluster_b.application_logs b ON a.trace_id = b.trace_id LIMIT 5"

echo ""
echo "--- UNION ALL ---"

# 5. Two-source UNION
query "2-source UNION" \
    "SELECT service, status FROM cluster_b.application_logs UNION ALL SELECT name AS service, role AS status FROM dynamodb.users LIMIT 20"

echo ""
echo "--- Aggregation across JOIN ---"

# 6. GROUP BY on joined result
query "JOIN + GROUP BY" \
    "SELECT u.role, COUNT(*) as cnt FROM cluster_b.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id GROUP BY u.role"

echo ""
echo "--- Window function on federated data ---"

# 7. ROW_NUMBER over joined data
query "JOIN + ROW_NUMBER" \
    "SELECT * FROM (SELECT l.service, u.name, ROW_NUMBER() OVER (PARTITION BY l.service ORDER BY l.timestamp DESC) as rn FROM cluster_b.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id) WHERE rn <= 3 LIMIT 15"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL
