#!/usr/bin/env bash
# Fuse test coverage report using Rust source-based coverage.
# Outputs: text summary + optional HTML report in coverage/html/
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

COV_DIR="$PROJECT_DIR/coverage"
PROF_DIR="$COV_DIR/profraw"
LLVM_BIN="$(dirname "$(find "$(rustc --print sysroot)" -name llvm-profdata | head -1)")"

FORMAT="text"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --html) FORMAT="html"; shift ;;
        --all)  FORMAT="all"; shift ;;
        -h|--help) echo "Usage: $0 [--html|--all]"; exit 0 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

rm -rf "$PROF_DIR" "$COV_DIR/html"
mkdir -p "$PROF_DIR"

echo "[1/4] Building + running tests with coverage..."
CARGO_INCREMENTAL=0 \
RUSTFLAGS="-C instrument-coverage" \
LLVM_PROFILE_FILE="$PROF_DIR/fuse-%p-%m.profraw" \
cargo test --all-targets --no-fail-fast 2>&1 | tail -20

echo "[2/4] Merging profile data..."
"$LLVM_BIN/llvm-profdata" merge -sparse "$PROF_DIR"/*.profraw -o "$COV_DIR/fuse.profdata"

echo "[3/4] Collecting test binaries..."
BINS=()
while IFS= read -r b; do
    BINS+=("-object" "$b")
done < <(
    CARGO_INCREMENTAL=0 \
    RUSTFLAGS="-C instrument-coverage" \
    LLVM_PROFILE_FILE="$PROF_DIR/fuse-%p-%m.profraw" \
    cargo test --all-targets --no-run --message-format=json 2>/dev/null \
    | jq -r 'select(.executable != null) | .executable'
)
echo "  Found $((${#BINS[@]} / 2)) binaries"

IGNORE="--ignore-filename-regex=(.cargo|target|tests/|_test\.rs$)"

echo "[4/4] Generating report..."
"$LLVM_BIN/llvm-cov" report \
    --instr-profile="$COV_DIR/fuse.profdata" \
    "${BINS[@]}" $IGNORE --summary-only \
    | tee "$COV_DIR/summary.txt"

if [[ "$FORMAT" == "html" || "$FORMAT" == "all" ]]; then
    mkdir -p "$COV_DIR/html"
    "$LLVM_BIN/llvm-cov" show \
        --instr-profile="$COV_DIR/fuse.profdata" \
        "${BINS[@]}" $IGNORE \
        --format=html --output-dir="$COV_DIR/html" \
        --show-line-counts-or-regions
    echo "HTML report: $COV_DIR/html/index.html"
fi

echo "Done. Summary: $COV_DIR/summary.txt"
