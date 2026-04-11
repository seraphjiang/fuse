#!/usr/bin/env bash
# #1130 Soak test — continuous load, detect memory leaks and connection exhaustion.
# Default: 60s (CI). Set SOAK_DURATION=86400 for 24h production soak.
set -euo pipefail

BASE="${FUSE_URL:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
DURATION="${SOAK_DURATION:-60}"
CONCURRENCY="${SOAK_CONCURRENCY:-5}"
INTERVAL=2  # seconds between batches

echo "╔═══════════════════════════════════════════════════╗"
echo "║  #1130 Soak Test                                 ║"
echo "║  Target: $BASE"
echo "║  Duration: ${DURATION}s  Concurrency: $CONCURRENCY"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

QUERIES=(
    '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}'
    '{"query":"SELECT service, COUNT(*) FROM cluster_a.application_logs GROUP BY service","format":"sql"}'
    '{"query":"SELECT * FROM cluster_a.application_logs UNION ALL SELECT * FROM cluster_b.application_logs LIMIT 5","format":"sql"}'
)

TOTAL=0 ERRORS=0 START=$(date +%s)
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

check_health() {
    local h=$(curl -sk --max-time 10 "$BASE/api/fuse/health" 2>/dev/null)
    echo "$h" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'connectors={len(d[\"connectors\"])} status={d[\"status\"]}')" 2>/dev/null || echo "UNREACHABLE"
}

echo "Initial health: $(check_health)"
echo ""

while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START))
    [ "$ELAPSED" -ge "$DURATION" ] && break

    # Fire concurrent queries
    for i in $(seq 1 $CONCURRENCY); do
        Q=${QUERIES[$((RANDOM % ${#QUERIES[@]}))]}
        (
            code=$(curl -sk -o /dev/null -w '%{http_code}' --max-time 15 -X POST "$BASE/api/fuse/query" \
                -H "Content-Type: application/json" -d "$Q" 2>/dev/null || echo "000")
            echo "$code" >> "$TMPDIR/codes"
        ) &
    done
    wait

    TOTAL=$((TOTAL + CONCURRENCY))
    BATCH_ERRORS=$(grep -cE "^[045]" "$TMPDIR/codes" 2>/dev/null || true)
    BATCH_ERRORS=${BATCH_ERRORS:-0}
    ERRORS=$((ERRORS + BATCH_ERRORS))
    > "$TMPDIR/codes"

    # Progress every 10s
    if [ $((ELAPSED % 10)) -lt $INTERVAL ]; then
        printf "  [%3ds/%ds] requests=%d errors=%d health=%s\n" "$ELAPSED" "$DURATION" "$TOTAL" "$ERRORS" "$(check_health)"
    fi

    sleep $INTERVAL
done

echo ""
echo "Final health: $(check_health)"
echo ""

ERROR_RATE=$(python3 -c "print(f'{$ERRORS/$TOTAL*100:.1f}%' if $TOTAL > 0 else 'N/A')")

echo "═══════════════════════════════════════════════════"
echo "  Duration: ${DURATION}s"
echo "  Total requests: $TOTAL"
echo "  Errors: $ERRORS ($ERROR_RATE)"
echo "  Concurrency: $CONCURRENCY"
echo "═══════════════════════════════════════════════════"

if [ "$ERRORS" -gt $((TOTAL / 10)) ]; then
    echo "  ❌ FAIL: Error rate >10%"
    exit 1
else
    echo "  ✅ PASS: Error rate within tolerance"
fi
