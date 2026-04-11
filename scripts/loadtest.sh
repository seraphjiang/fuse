#!/usr/bin/env bash
# Load test: concurrent queries against Fuse.
# Usage: ./scripts/loadtest.sh [--url URL] [--concurrency N] [--total N]
set -euo pipefail

URL="${FUSE_URL:-http://localhost:9400}"
CONCURRENCY=50
TOTAL=200
TMPDIR=$(mktemp -d)

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

# Verify server is up
curl -sf "$URL/api/fuse/health" > /dev/null || { echo "ERROR: Fuse not reachable at $URL"; exit 1; }

QUERIES=(
  '{"query":"SELECT 1 as n","format":"sql"}'
  '{"query":"EXPLAIN SELECT 1","format":"sql"}'
)

run_query() {
  local id=$1
  local q="${QUERIES[$((id % ${#QUERIES[@]}))]}"
  local start end elapsed status
  start=$(date +%s%N)
  status=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL/api/fuse/query" \
    -H 'Content-Type: application/json' -d "$q" --max-time 30)
  end=$(date +%s%N)
  elapsed=$(( (end - start) / 1000000 ))
  echo "$id,$status,$elapsed" >> "$TMPDIR/results.csv"
}

echo ""
echo "Running $TOTAL queries with concurrency $CONCURRENCY..."
START=$(date +%s%N)

# Launch queries with concurrency limit
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

# Analyze results
TOTAL_DONE=$(wc -l < "$TMPDIR/results.csv")
SUCCESS=$(grep -c ",200," "$TMPDIR/results.csv" || echo 0)
ERRORS=$((TOTAL_DONE - SUCCESS))
LATENCIES=$(awk -F, '{print $3}' "$TMPDIR/results.csv" | sort -n)
P50=$(echo "$LATENCIES" | awk "NR==int($TOTAL_DONE*0.5)")
P95=$(echo "$LATENCIES" | awk "NR==int($TOTAL_DONE*0.95)")
P99=$(echo "$LATENCIES" | awk "NR==int($TOTAL_DONE*0.99)")
AVG=$(echo "$LATENCIES" | awk '{s+=$1} END {printf "%.0f", s/NR}')
QPS=$(awk "BEGIN {printf \"%.1f\", $TOTAL_DONE / ($WALL_MS / 1000.0)}")

echo ""
echo "=== Results ==="
echo "Total:       $TOTAL_DONE queries in ${WALL_MS}ms"
echo "Throughput:  $QPS queries/sec"
echo "Success:     $SUCCESS ($((SUCCESS * 100 / TOTAL_DONE))%)"
echo "Errors:      $ERRORS"
echo "Latency avg: ${AVG}ms"
echo "Latency p50: ${P50}ms"
echo "Latency p95: ${P95}ms"
echo "Latency p99: ${P99}ms"

rm -rf "$TMPDIR"
[[ $ERRORS -eq 0 ]] && echo "🎉 All queries succeeded!" || echo "⚠️  $ERRORS queries failed"
