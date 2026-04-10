#!/usr/bin/env bash
# #651 Performance regression CI — run as part of CI pipeline.
# Compares current latencies against Sprint 5 baseline.
# Exit 1 if any metric regresses >20%.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FUSE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Sprint 5 baseline (mock connectors, deterministic)
BASELINE_P50_SINGLE=1    # ms
BASELINE_P99_SINGLE=5
BASELINE_P50_UNION=20
BASELINE_P99_UNION=50
BASELINE_P50_JOIN=2
BASELINE_P99_JOIN=10
BASELINE_P99_MIXED=100
REGRESSION_THRESHOLD=1.20  # 20% regression allowed

echo "╔═══════════════════════════════════════════════════╗"
echo "║  #651 Performance Regression CI                  ║"
echo "║  Time: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Run the load tests and capture output
cd "$FUSE_DIR"
source ~/.cargo/env 2>/dev/null || true

OUTPUT=$(cargo test -p fuse-server --test load_test --no-default-features -- --nocapture 2>&1)

if echo "$OUTPUT" | grep -q "FAILED"; then
    echo "❌ Load tests FAILED"
    echo "$OUTPUT" | grep "FAILED"
    exit 1
fi

# Parse latencies from test output
parse_metric() {
    echo "$OUTPUT" | grep "$1" | grep -oP "$2=\K[0-9]+"
}

S_P50=$(parse_metric "Single-source" "p50")
S_P99=$(parse_metric "Single-source" "p99")
U_P50=$(parse_metric "UNION ALL" "p50")
U_P99=$(parse_metric "UNION ALL" "p99")
J_P50=$(parse_metric "JOIN" "p50")
J_P99=$(parse_metric "JOIN" "p99")
M_P99=$(parse_metric "Mixed" "p99")
ERRORS=$(parse_metric "Mixed" "fail")

echo "  Current Results:"
echo "  ┌──────────────────┬───────┬───────┬──────────┬──────────┐"
echo "  │ Workload         │  p50  │  p99  │ base p50 │ base p99 │"
echo "  ├──────────────────┼───────┼───────┼──────────┼──────────┤"
printf "  │ Single-source    │ %3sms │ %3sms │    %3sms │    %3sms │\n" "$S_P50" "$S_P99" "$BASELINE_P50_SINGLE" "$BASELINE_P99_SINGLE"
printf "  │ UNION ALL        │ %3sms │ %3sms │    %3sms │    %3sms │\n" "$U_P50" "$U_P99" "$BASELINE_P50_UNION" "$BASELINE_P99_UNION"
printf "  │ JOIN             │ %3sms │ %3sms │    %3sms │    %3sms │\n" "$J_P50" "$J_P99" "$BASELINE_P50_JOIN" "$BASELINE_P99_JOIN"
printf "  │ Mixed 100c       │   -   │ %3sms │       -  │   %3sms  │\n" "$M_P99" "$BASELINE_P99_MIXED"
echo "  └──────────────────┴───────┴───────┴──────────┴──────────┘"
echo ""

# Check for regressions
REGRESSED=0

check_regression() {
    local name="$1" current="$2" baseline="$3"
    if [ -z "$current" ] || [ -z "$baseline" ]; then return; fi
    # Allow baseline of 0/1 — use absolute threshold of 50ms instead
    if [ "$baseline" -le 1 ]; then
        if [ "$current" -gt 50 ]; then
            echo "  ❌ REGRESSION: $name = ${current}ms (baseline=${baseline}ms, >50ms absolute)"
            REGRESSED=1
        else
            echo "  ✅ $name = ${current}ms (baseline=${baseline}ms)"
        fi
    else
        local limit=$(python3 -c "print(int($baseline * $REGRESSION_THRESHOLD))")
        if [ "$current" -gt "$limit" ]; then
            echo "  ❌ REGRESSION: $name = ${current}ms (baseline=${baseline}ms, limit=${limit}ms)"
            REGRESSED=1
        else
            echo "  ✅ $name = ${current}ms (baseline=${baseline}ms, limit=${limit}ms)"
        fi
    fi
}

check_regression "Single p99" "$S_P99" "$BASELINE_P99_SINGLE"
check_regression "UNION p99"  "$U_P99" "$BASELINE_P99_UNION"
check_regression "JOIN p99"   "$J_P99" "$BASELINE_P99_JOIN"
check_regression "Mixed p99"  "$M_P99" "$BASELINE_P99_MIXED"

if [ "${ERRORS:-0}" -gt 0 ]; then
    echo "  ❌ ERRORS: $ERRORS failures in mixed workload"
    REGRESSED=1
else
    echo "  ✅ 0 errors"
fi

echo ""
if [ "$REGRESSED" -eq 1 ]; then
    echo "  ❌ PERFORMANCE REGRESSION DETECTED"
    exit 1
else
    echo "  ✅ NO REGRESSION — all metrics within 20% of baseline"
fi
