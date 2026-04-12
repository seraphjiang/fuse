#!/usr/bin/env bash
# Load test: 100 concurrent mixed queries against Fuse.
# Sprint-17 #1711 — stress test with realistic query mix.
# Usage: ./scripts/loadtest.sh [--url URL] [--concurrency N] [--total N]
set -euo pipefail

URL="${FUSE_URL:-http://localhost:9400}"
CONCURRENCY=100
TOTAL=500
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url) URL="$2"; shift 2;;
    --concurrency) CONCURRENCY="$2"; shift 2;;
    --total) TOTAL="$2"; shift 2;;
    *) echo "Unknown: $1"; exit 1;;
  esac
done

echo "=== Fuse Load Test ==="
echo "URL: $URL | Concurrency: $CONCURRENCY | Total: $TOTAL"

curl -sf "$URL/api/fuse/health" > /dev/null || { echo "ERROR: Fuse not reachable at $URL"; exit 1; }

NAMES=(
  single_select single_filter single_agg cross_join union_all
  explain ppl_basic ppl_stats window_fn cursor_page prepared_stmt
  health datasources
)
BODIES=(
  '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 100","format":"sql"}'
  '{"query":"SELECT * FROM cluster_a.application_logs WHERE status >= 500 LIMIT 100","format":"sql"}'
  '{"query":"SELECT service, count(*) FROM cluster_a.application_logs GROUP BY service","format":"sql"}'
  '{"query":"SELECT l.service, u.name FROM cluster_a.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id LIMIT 50","format":"sql"}'
  '{"query":"SELECT * FROM cluster_a.application_logs UNION ALL SELECT * FROM cluster_b.application_logs LIMIT 100","format":"sql"}'
  '{"query":"EXPLAIN SELECT * FROM cluster_a.application_logs WHERE status >= 500","format":"sql"}'
  '{"query":"source = cluster_a.application_logs | where status >= 500 | head 50","format":"ppl"}'
  '{"query":"source = cluster_a.application_logs | where status >= 500 | stats count() by service | sort - count()","format":"ppl"}'
  '{"query":"SELECT *, ROW_NUMBER() OVER (PARTITION BY service ORDER BY timestamp DESC) as rn FROM cluster_a.application_logs LIMIT 100","format":"sql"}'
  '{"query":"SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC","format":"sql","page_size":20}'
  '{"query":"PREPARE q AS SELECT * FROM cluster_a.application_logs WHERE status >= $1 LIMIT 10; EXECUTE q(500)","format":"sql"}'
  'HEALTH'
  'DATASOURCES'
)
NUM_TYPES=${#NAMES[@]}

run_query() {
  local id=$1 idx=$((id % NUM_TYPES))
  local name="${NAMES[$idx]}" body="${BODIES[$idx]}"
  local start end elapsed status
  start=$(date +%s%N)
  if [[ "$body" == "HEALTH" ]]; then
    status=$(curl -s -o /dev/null -w "%{http_code}" "$URL/api/fuse/health" --max-time 30)
  elif [[ "$body" == "DATASOURCES" ]]; then
    status=$(curl -s -o /dev/null -w "%{http_code}" "$URL/api/fuse/datasources" --max-time 30)
  else
    status=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/api/fuse/query" \
      -H 'Content-Type: application/json' -d "$body" --max-time 30)
  fi
  end=$(date +%s%N)
  elapsed=$(( (end - start) / 1000000 ))
  echo "$id,$name,$status,$elapsed" >> "$TMPDIR/results.csv"
}

echo ""
echo "Running $TOTAL queries with concurrency $CONCURRENCY ($NUM_TYPES query types)..."
START=$(date +%s%N)

active=0
for i in $(seq 1 "$TOTAL"); do
  run_query "$i" &
  active=$((active + 1))
  if [[ $active -ge $CONCURRENCY ]]; then
    wait -n 2>/dev/null || true
    active=$((active - 1))
  fi
done
wait

END=$(date +%s%N)
WALL_MS=$(( (END - START) / 1000000 ))

TOTAL_DONE=$(wc -l < "$TMPDIR/results.csv")
SUCCESS=$(grep -c ",200," "$TMPDIR/results.csv" || echo 0)
ERRORS=$((TOTAL_DONE - SUCCESS))
LATENCIES=$(awk -F, '{print $4}' "$TMPDIR/results.csv" | sort -n)
P50=$(echo "$LATENCIES" | awk "NR==int($TOTAL_DONE*0.5)")
P95=$(echo "$LATENCIES" | awk "NR==int($TOTAL_DONE*0.95)")
P99=$(echo "$LATENCIES" | awk "NR==int($TOTAL_DONE*0.99)")
AVG=$(echo "$LATENCIES" | awk '{s+=$1} END {printf "%.0f", s/NR}')
QPS=$(awk "BEGIN {printf \"%.1f\", $TOTAL_DONE / ($WALL_MS / 1000.0)}")

echo ""
echo "=== Overall Results ==="
echo "Total:       $TOTAL_DONE queries in ${WALL_MS}ms"
echo "Throughput:  $QPS queries/sec"
echo "Success:     $SUCCESS ($((SUCCESS * 100 / TOTAL_DONE))%)"
echo "Errors:      $ERRORS"
echo "Latency avg: ${AVG}ms"
echo "Latency p50: ${P50}ms"
echo "Latency p95: ${P95}ms"
echo "Latency p99: ${P99}ms"

echo ""
echo "=== Per-Query-Type Breakdown ==="
printf "%-20s %6s %6s %8s %8s %8s\n" "Type" "Count" "Errs" "Avg(ms)" "P50(ms)" "P95(ms)"
printf "%-20s %6s %6s %8s %8s %8s\n" "----" "-----" "----" "-------" "-------" "-------"
for name in "${NAMES[@]}"; do
  type_lines=$(grep ",$name," "$TMPDIR/results.csv" || true)
  [[ -z "$type_lines" ]] && continue
  count=$(echo "$type_lines" | wc -l)
  errs=$(echo "$type_lines" | grep -cv ",200," 2>/dev/null || echo 0)
  type_lats=$(echo "$type_lines" | awk -F, '{print $4}' | sort -n)
  tavg=$(echo "$type_lats" | awk '{s+=$1} END {printf "%.0f", s/NR}')
  tp50=$(echo "$type_lats" | awk "NR==int($count*0.5){print}")
  tp95=$(echo "$type_lats" | awk "NR==int($count*0.95){print}")
  printf "%-20s %6d %6d %8s %8s %8s\n" "$name" "$count" "$errs" "$tavg" "${tp50:-0}" "${tp95:-0}"
done

echo ""
[[ $ERRORS -eq 0 ]] && echo "🎉 All queries succeeded!" || echo "⚠️  $ERRORS queries failed"
