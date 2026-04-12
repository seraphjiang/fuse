#!/usr/bin/env bash
# Performance Regression Detection — compare criterion benchmark runs
#
# Usage:
#   ./perf_regression_detect.sh baseline       # save current run as baseline
#   ./perf_regression_detect.sh check          # run and compare against baseline
#   ./perf_regression_detect.sh compare A.json B.json  # compare two files
#
# Env: PERF_THRESHOLD=15 (% regression allowed)
#      BENCH_PACKAGES="-p fuse-engine" (cargo bench package flags)
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
FUSE_DIR="$(cd "$DIR/../.." && pwd)"
DATA="$DIR/.perf"
PACKAGES="${BENCH_PACKAGES:--p fuse-engine}"
mkdir -p "$DATA"

run_benchmarks() {
  local out="$1"
  echo "Running: cargo bench $PACKAGES"
  cd "$FUSE_DIR"
  cargo bench $PACKAGES 2>&1 | tee "$DATA/_raw.txt"
  python3 "$DIR/parse_criterion.py" "$DATA/_raw.txt" "$out"
}

case "${1:-check}" in
  baseline)
    run_benchmarks "$DATA/baseline.json"
    echo "✅ Baseline saved to $DATA/baseline.json"
    ;;
  check)
    if [ ! -f "$DATA/baseline.json" ]; then
      echo "No baseline — creating one now..."
      run_benchmarks "$DATA/baseline.json"
      echo "✅ Baseline created. Run again to compare."
      exit 0
    fi
    run_benchmarks "$DATA/current.json"
    echo ""
    python3 "$DIR/compare_perf.py" "$DATA/baseline.json" "$DATA/current.json"
    ;;
  compare)
    [ -f "${2:-}" ] && [ -f "${3:-}" ] || { echo "Usage: $0 compare A.json B.json"; exit 2; }
    python3 "$DIR/compare_perf.py" "$2" "$3"
    ;;
  *) echo "Usage: $0 {baseline|check|compare A B}"; exit 2 ;;
esac
