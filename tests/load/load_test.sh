#!/usr/bin/env bash
# Fuse Playground Load Test
# Sends concurrent queries and reports p50/p95/p99 latency, error rate, throughput.
#
# Usage: ./tests/load/load_test.sh [BASE_URL] [CONCURRENCY]

set -euo pipefail

BASE="${1:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
CONCURRENCY="${2:-50}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Query mix: SQL, PPL, UNION ALL, single-source, cross-cluster
QUERIES=(
  '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}'
  '{"query":"SELECT * FROM cluster_b.application_logs LIMIT 5","format":"sql"}'
  '{"query":"SELECT service, status FROM cluster_a.application_logs WHERE status >= 500 LIMIT 10","format":"sql"}'
  '{"query":"source = cluster_a.application_logs | head 5","format":"ppl"}'
  '{"query":"source = cluster_b.application_logs | head 5","format":"ppl"}'
  '{"query":"SELECT service, status FROM cluster_a.application_logs UNION ALL SELECT service, status FROM cluster_b.application_logs LIMIT 10","format":"sql"}'
  '{"query":"SELECT a.service, b.service FROM cluster_a.application_logs a JOIN cluster_b.application_logs b ON a.trace_id = b.trace_id LIMIT 5","format":"sql"}'
  '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 3","format":"sql"}'
  '{"query":"source = cluster_a.application_logs | where status >= 400 | head 10","format":"ppl"}'
  '{"query":"SELECT trace_id, service, message FROM cluster_b.application_logs LIMIT 10","format":"sql"}'
)

NUM_QUERIES=${#QUERIES[@]}

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Fuse Playground Load Test                        ║"
echo "║  Target:      $BASE"
echo "║  Concurrency: $CONCURRENCY"
echo "║  Query mix:   $NUM_QUERIES templates"
echo "║  Time:        $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Fire all queries concurrently
for i in $(seq 1 "$CONCURRENCY"); do
    idx=$(( (i - 1) % NUM_QUERIES ))
    q="${QUERIES[$idx]}"
    (
        start_ns=$(date +%s%N)
        http_code=$(curl -sko /dev/null -w "%{http_code}" --max-time 30 \
            -X POST "$BASE/api/fuse/query" \
            -H "Content-Type: application/json" -d "$q" 2>/dev/null)
        end_ns=$(date +%s%N)
        ms=$(( (end_ns - start_ns) / 1000000 ))
        echo "$ms $http_code" > "$TMPDIR/result_$i"
    ) &
done

echo "Waiting for $CONCURRENCY requests..."
wait
echo ""

# Collect results
LATENCIES=()
OK=0; ERRORS=0; TOTAL_MS=0
for i in $(seq 1 "$CONCURRENCY"); do
    if [ -f "$TMPDIR/result_$i" ]; then
        read -r ms code < "$TMPDIR/result_$i"
        LATENCIES+=("$ms")
        TOTAL_MS=$((TOTAL_MS + ms))
        if [ "$code" = "200" ]; then OK=$((OK + 1)); else ERRORS=$((ERRORS + 1)); fi
    else
        ERRORS=$((ERRORS + 1))
    fi
done

TOTAL=$((OK + ERRORS))

# Sort latencies for percentile calculation
SORTED=($(printf '%s\n' "${LATENCIES[@]}" | sort -n))
COUNT=${#SORTED[@]}

percentile() {
    local p=$1
    local idx=$(( (p * COUNT + 99) / 100 - 1 ))
    [ "$idx" -lt 0 ] && idx=0
    [ "$idx" -ge "$COUNT" ] && idx=$((COUNT - 1))
    echo "${SORTED[$idx]}"
}

P50=$(percentile 50)
P95=$(percentile 95)
P99=$(percentile 99)
MIN="${SORTED[0]}"
MAX="${SORTED[$((COUNT - 1))]}"
AVG=$((TOTAL_MS / (COUNT > 0 ? COUNT : 1)))

# Calculate elapsed wall time (approx from max latency)
ERROR_RATE=$(python3 -c "print(f'{$ERRORS/$TOTAL*100:.1f}' if $TOTAL > 0 else '0.0')")
THROUGHPUT=$(python3 -c "print(f'{$TOTAL/($MAX/1000):.1f}' if $MAX > 0 else '0.0')")

# Report
echo "═══════════════════════════════════════════════════"
echo "  LOAD TEST RESULTS"
echo "═══════════════════════════════════════════════════"
echo ""
echo "| Metric | Value |"
echo "|--------|-------|"
echo "| Requests | $TOTAL |"
echo "| Success | $OK |"
echo "| Errors | $ERRORS |"
echo "| Error Rate | ${ERROR_RATE}% |"
echo "| Min Latency | ${MIN}ms |"
echo "| Avg Latency | ${AVG}ms |"
echo "| p50 Latency | ${P50}ms |"
echo "| p95 Latency | ${P95}ms |"
echo "| p99 Latency | ${P99}ms |"
echo "| Max Latency | ${MAX}ms |"
echo "| Throughput | ${THROUGHPUT} req/s |"
echo ""
echo "═══════════════════════════════════════════════════"

[ "$ERRORS" -le $((TOTAL / 10)) ]  # Fail if >10% error rate
