#!/usr/bin/env bash
# Write path E2E tests — CTAS, INSERT INTO SELECT, transactions.
set -euo pipefail

BASE="${FUSE_URL:-http://localhost:9400}"
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

query() {
    curl -sf -X POST "$BASE/api/fuse/query" \
        -H 'Content-Type: application/json' \
        -d "$1"
}

echo "=== Write Path E2E Tests ==="

# --- CTAS ---
echo ""
echo "--- CREATE TABLE AS SELECT ---"

run "CTAS basic" query '{"query": "CREATE TABLE duckdb.test_ctas AS SELECT 1 AS id, '\''hello'\'' AS msg", "format": "sql"}'
run "Query CTAS result" query '{"query": "SELECT * FROM duckdb.test_ctas", "format": "sql"}'

# --- INSERT INTO SELECT ---
echo ""
echo "--- INSERT INTO ... SELECT ---"

run "INSERT INTO SELECT" query '{"query": "INSERT INTO duckdb.test_ctas SELECT 2 AS id, '\''world'\'' AS msg", "format": "sql"}'
run "Verify inserted rows" query '{"query": "SELECT count(*) AS cnt FROM duckdb.test_ctas", "format": "sql"}'

# --- Transactions ---
echo ""
echo "--- Transactions ---"

run "BEGIN transaction" query '{"query": "BEGIN", "format": "sql"}'
run "INSERT in transaction" query '{"query": "INSERT INTO duckdb.test_ctas SELECT 3 AS id, '\''txn'\'' AS msg", "format": "sql"}'
run "COMMIT transaction" query '{"query": "COMMIT", "format": "sql"}'
run "Verify after COMMIT" query '{"query": "SELECT count(*) AS cnt FROM duckdb.test_ctas", "format": "sql"}'

run "BEGIN + ROLLBACK" query '{"query": "BEGIN", "format": "sql"}'
run "INSERT before ROLLBACK" query '{"query": "INSERT INTO duckdb.test_ctas SELECT 99 AS id, '\''rolled_back'\'' AS msg", "format": "sql"}'
run "ROLLBACK" query '{"query": "ROLLBACK", "format": "sql"}'

# --- Error cases ---
echo ""
echo "--- Error Cases ---"

run "CTAS to read-only connector fails gracefully" query '{"query": "CREATE TABLE cluster_a.bad AS SELECT 1", "format": "sql"}'
run "COMMIT without BEGIN fails gracefully" query '{"query": "COMMIT", "format": "sql"}'

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
