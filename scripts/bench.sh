#!/usr/bin/env bash
# Performance benchmark suite for Fuse query engine.
# Usage: ./scripts/bench.sh [--url URL] [--iterations N] [--warmup N]
set -euo pipefail

URL="${1:-http://localhost:9400}"
ITERATIONS="${2:-10}"
WARMUP="${3:-2}"

query() {
  local name="$1" body="$2"
  # Warmup
  for ((i=0; i<WARMUP; i++)); do
    curl -s -o /dev/null -X POST "$URL/api/fuse/query" -H 'Content-Type: application/json' -d "$body"
  done
  # Timed runs
  local total=0
  for ((i=0; i<ITERATIONS; i++)); do
    local start end elapsed
    start=$(date +%s%N)
    curl -s -o /dev/null -X POST "$URL/api/fuse/query" -H 'Content-Type: application/json' -d "$body"
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))
    total=$((total + elapsed))
  done
  local avg=$((total / ITERATIONS))
  printf "%-40s %6d ms (avg over %d runs)\n" "$name" "$avg" "$ITERATIONS"
}

echo "=== Fuse Performance Benchmark ==="
echo "URL: $URL | Iterations: $ITERATIONS | Warmup: $WARMUP"
echo ""

# Health check
curl -sf "$URL/api/fuse/health" > /dev/null || { echo "ERROR: Fuse not reachable at $URL"; exit 1; }

echo "--- Single Source ---"
query "single_select" '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 100","format":"sql"}'
query "single_filter" '{"query":"SELECT * FROM cluster_a.application_logs WHERE status >= 500 LIMIT 100","format":"sql"}'
query "single_agg" '{"query":"SELECT service, count(*) FROM cluster_a.application_logs GROUP BY service","format":"sql"}'

echo ""
echo "--- Cross-Source JOIN ---"
query "two_source_join" '{"query":"SELECT l.service, u.name FROM cluster_a.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id LIMIT 50","format":"sql"}'

echo ""
echo "--- UNION ALL ---"
query "two_source_union" '{"query":"SELECT * FROM cluster_a.application_logs UNION ALL SELECT * FROM cluster_b.application_logs LIMIT 100","format":"sql"}'

echo ""
echo "--- EXPLAIN ---"
query "explain_plan" '{"query":"EXPLAIN SELECT * FROM cluster_a.application_logs WHERE status >= 500","format":"sql"}'

echo ""
echo "--- PPL ---"
query "ppl_basic" '{"query":"source = cluster_a.application_logs | where status >= 500 | head 50","format":"ppl"}'

echo ""
echo "--- Metadata ---"
start=$(date +%s%N)
curl -s -o /dev/null "$URL/api/fuse/datasources"
end=$(date +%s%N)
printf "%-40s %6d ms\n" "list_datasources" "$(( (end - start) / 1000000 ))"

start=$(date +%s%N)
curl -s -o /dev/null "$URL/api/fuse/health"
end=$(date +%s%N)
printf "%-40s %6d ms\n" "health_check" "$(( (end - start) / 1000000 ))"

echo ""

echo ""
echo "--- Window Functions ---"
query "window_row_number" '{"query":"SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY timestamp DESC) as rn FROM cluster_a.application_logs LIMIT 100","format":"sql"}'

echo ""
echo "--- Cursor Pagination ---"
query "cursor_first_page" '{"query":"SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC","format":"sql","page_size":20}'

echo ""
echo "--- Saved Queries ---"
start=$(date +%s%N)
curl -s -o /dev/null "$URL/api/fuse/saved"
end=$(date +%s%N)
printf "%-40s %6d ms\n" "list_saved_queries" "$(( (end - start) / 1000000 ))"

echo ""
echo "--- History ---"
start=$(date +%s%N)
curl -s -o /dev/null "$URL/api/fuse/history"
end=$(date +%s%N)
printf "%-40s %6d ms\n" "query_history" "$(( (end - start) / 1000000 ))"

echo ""
echo "--- Sprint 12-16 Features ---"
query "prepared_stmt" '{"query":"PREPARE q AS SELECT * FROM cluster_a.application_logs WHERE status >= $1 LIMIT 10; EXECUTE q(500)","format":"sql"}'
query "multi_statement" '{"query":"SELECT count(*) FROM cluster_a.application_logs; SELECT count(*) FROM cluster_b.application_logs","format":"sql"}'
query "ppl_lookup" '{"query":"source = cluster_a.application_logs | where status >= 500 | stats count() by service | sort - count()","format":"ppl"}'
query "validate_only" '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 1","format":"sql"}' # validate endpoint
query "nl_to_sql" '{"question":"show me errors from cluster_a"}'

echo ""
echo "--- Endpoint Latency ---"
for ep in health datasources stats advisor federation; do
  start=$(date +%s%N)
  curl -s -o /dev/null "$URL/api/fuse/$ep"
  end=$(date +%s%N)
  printf "%-40s %6d ms\n" "$ep" "$(( (end - start) / 1000000 ))"
done

echo "=== Benchmark Complete ==="
