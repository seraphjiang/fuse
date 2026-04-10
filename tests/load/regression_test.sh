#!/usr/bin/env bash
# Fuse Performance Regression Suite
# Compares current performance against Sprint 2 baseline.
#
# Usage: ./tests/load/regression_test.sh [BASE_URL] [CONCURRENCY]

set -euo pipefail

BASE="${1:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
CONCURRENCY="${2:-10}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Sprint 2 baseline (50 concurrent, 2026-04-09)
BASELINE_P50=413
BASELINE_P95=535
BASELINE_P99=577
BASELINE_ERR=0.0
BASELINE_RPS=86.7

# Regression thresholds (% worse than baseline allowed)
LATENCY_THRESHOLD=50   # 50% slower is a regression
ERROR_THRESHOLD=5       # >5% error rate is a regression

# Query templates — single-source only (most stable for regression tracking)
QUERIES=(
  '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}'
  '{"query":"SELECT * FROM cluster_b.application_logs LIMIT 5","format":"sql"}'
  '{"query":"SELECT service, status FROM cluster_a.application_logs WHERE status >= 500 LIMIT 10","format":"sql"}'
  '{"query":"source = cluster_a.application_logs | head 5","format":"ppl"}'
  '{"query":"source = cluster_b.application_logs | head 5","format":"ppl"}'
  '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 3","format":"sql"}'
  '{"query":"source = cluster_a.application_logs | where status >= 400 | head 10","format":"ppl"}'
  '{"query":"SELECT trace_id, service, message FROM cluster_b.application_logs LIMIT 10","format":"sql"}'
)
NUM_QUERIES=${#QUERIES[@]}

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Fuse Performance Regression Suite                ║"
echo "║  Target:      $BASE"
echo "║  Concurrency: $CONCURRENCY"
echo "║  Queries:     $NUM_QUERIES templates (single-source)"
echo "║  Time:        $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Run 3 rounds for stability
for round in 1 2 3; do
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
            echo "$ms $http_code"
        ) >> "$TMPDIR/round_${round}.txt" &
    done
    wait
done

# Aggregate all rounds
cat "$TMPDIR"/round_*.txt > "$TMPDIR/all.txt"
TOTAL=$(wc -l < "$TMPDIR/all.txt")
OK=$(grep -c " 200$" "$TMPDIR/all.txt" || true)
ERRORS=$((TOTAL - OK))

# Extract latencies for successful requests
grep " 200$" "$TMPDIR/all.txt" | awk '{print $1}' | sort -n > "$TMPDIR/latencies.txt"
COUNT=$(wc -l < "$TMPDIR/latencies.txt")

if [ "$COUNT" -eq 0 ]; then
    echo "❌ No successful requests. Cannot compute metrics."
    exit 1
fi

percentile() {
    local p=$1
    sed -n "$(( (p * COUNT + 99) / 100 ))p" "$TMPDIR/latencies.txt"
}

P50=$(percentile 50)
P95=$(percentile 95)
P99=$(percentile 99)
AVG=$(awk '{s+=$1} END {printf "%.0f", s/NR}' "$TMPDIR/latencies.txt")
ERROR_RATE=$(python3 -c "print(f'{$ERRORS/$TOTAL*100:.1f}')")

# Compare against baseline
p50_delta=$(python3 -c "print(f'{($P50 - $BASELINE_P50) / $BASELINE_P50 * 100:.1f}')")
p95_delta=$(python3 -c "print(f'{($P95 - $BASELINE_P95) / $BASELINE_P95 * 100:.1f}')")
p99_delta=$(python3 -c "print(f'{($P99 - $BASELINE_P99) / $BASELINE_P99 * 100:.1f}')")

echo "═══════════════════════════════════════════════════"
echo "  REGRESSION RESULTS (3 rounds × $CONCURRENCY concurrent)"
echo "═══════════════════════════════════════════════════"
echo ""
echo "| Metric | Baseline | Current | Delta |"
echo "|--------|----------|---------|-------|"
echo "| p50 | ${BASELINE_P50}ms | ${P50}ms | ${p50_delta}% |"
echo "| p95 | ${BASELINE_P95}ms | ${P95}ms | ${p95_delta}% |"
echo "| p99 | ${BASELINE_P99}ms | ${P99}ms | ${p99_delta}% |"
echo "| Error Rate | ${BASELINE_ERR}% | ${ERROR_RATE}% | |"
echo "| Requests | 50 | $TOTAL | |"
echo "| Success | 50 | $OK | |"
echo ""

# Verdict
REGRESSED=false
if python3 -c "exit(0 if float('$p50_delta') > $LATENCY_THRESHOLD else 1)" 2>/dev/null; then
    echo "❌ REGRESSION: p50 latency increased ${p50_delta}% (threshold: ${LATENCY_THRESHOLD}%)"
    REGRESSED=true
fi
if python3 -c "exit(0 if float('$p95_delta') > $LATENCY_THRESHOLD else 1)" 2>/dev/null; then
    echo "❌ REGRESSION: p95 latency increased ${p95_delta}% (threshold: ${LATENCY_THRESHOLD}%)"
    REGRESSED=true
fi
if python3 -c "exit(0 if float('$ERROR_RATE') > $ERROR_THRESHOLD else 1)" 2>/dev/null; then
    echo "❌ REGRESSION: Error rate ${ERROR_RATE}% exceeds threshold ${ERROR_THRESHOLD}%"
    REGRESSED=true
fi

if [ "$REGRESSED" = false ]; then
    echo "✅ NO REGRESSION detected"
fi
echo "═══════════════════════════════════════════════════"

[ "$REGRESSED" = false ]
