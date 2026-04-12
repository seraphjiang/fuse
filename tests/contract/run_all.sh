#!/usr/bin/env bash
# Run all contract/quality test suites
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
FUSE="${1:-${FUSE_URL:-https://fuse.huanji.profile.aws.dev}}"
PASS=0 FAIL=0

run() {
  local name="$1" script="$2"
  shift 2
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  $name"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if bash "$script" "$@" 2>&1; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
  fi
  echo ""
}

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Fuse Quality Gate — All Test Suites              ║"
echo "║  Target: $FUSE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

run "API Contract Tests"      "$DIR/api_contract_test.sh"      "$FUSE"
run "Endpoint Coverage"        "$DIR/endpoint_coverage_test.sh" "$FUSE"
run "Chaos Tests"              "$DIR/chaos_test.sh"             "$FUSE"
run "Fuzz Tests (50 iters)"    "$DIR/fuzz_test.sh"              "$FUSE" 50

echo "═══════════════════════════════════════════════════"
TOTAL=$((PASS + FAIL))
echo "  Suites: $PASS/$TOTAL passed"
[ "$FAIL" -gt 0 ] && { echo "  ❌ $FAIL suite(s) failed"; exit 1; }
echo "  ✅ All quality gates passed"
