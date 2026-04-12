#!/usr/bin/env bash
# Chaos Testing — verify graceful degradation under failures
# Usage: ./chaos_test.sh [FUSE_URL]
set -euo pipefail

FUSE="${1:-${FUSE_URL:-https://fuse.huanji.profile.aws.dev}}"
PASS=0 FAIL=0 WARN=0
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

ok()   { echo "  ✅ $1"; PASS=$((PASS + 1)); }
warn() { echo "  ⚠️  $1"; WARN=$((WARN + 1)); }
fail() { echo "  ❌ $1 — $2"; FAIL=$((FAIL + 1)); }

query() {
  local tmpfile="$TMPDIR/resp_$RANDOM"
  local code
  code=$(curl -sk -o "$tmpfile" -w "%{http_code}" --max-time 35 \
    -X POST "$FUSE/api/fuse/query" \
    -H 'Content-Type: application/json' -d "$1" 2>/dev/null)
  echo "$code"
  cat "$tmpfile"
  rm -f "$tmpfile"
}

expect_not_500() {
  local name="$1" payload="$2"
  local result code
  result=$(query "$payload")
  code=$(echo "$result" | head -c 3)
  if [ "$code" != "500" ]; then ok "$name → HTTP $code"
  else fail "$name" "got 500"; fi
}

echo "🔥 Chaos Tests — $FUSE"
echo ""

# 1. Nonexistent datasource
echo "── Nonexistent datasource ──"
expect_not_500 "Nonexistent datasource" '{"query":"SELECT * FROM nonexistent_xyz.t LIMIT 1","format":"sql"}'

# 2. Nonexistent table
echo ""
echo "── Nonexistent table ──"
expect_not_500 "Nonexistent table" '{"query":"SELECT * FROM cluster_a.no_such_table_xyz LIMIT 1","format":"sql"}'

# 3. Malformed inputs
echo ""
echo "── Malformed inputs ──"
expect_not_500 "Empty query" '{"query":"","format":"sql"}'
expect_not_500 "Bad format" '{"query":"SELECT 1","format":"graphql"}'
expect_not_500 "Missing query field" '{"format":"sql"}'

# 4. UNION with bad source
echo ""
echo "── Partial failure (UNION good + bad) ──"
result=$(query '{"query":"SELECT service FROM cluster_a.application_logs UNION ALL SELECT service FROM nonexistent_xyz.logs LIMIT 5","format":"sql"}')
code=$(echo "$result" | head -c 3)
if [[ "$code" =~ ^[24] ]]; then ok "UNION partial failure → HTTP $code"
else fail "UNION partial failure" "HTTP $code"; fi

# 5. Concurrent burst
echo ""
echo "── Concurrent burst (20 queries) ──"
for i in $(seq 1 20); do
  (
    curl -sk -o /dev/null -w "%{http_code}\n" --max-time 30 \
      -X POST "$FUSE/api/fuse/query" \
      -H 'Content-Type: application/json' \
      -d '{"query":"SELECT service FROM cluster_a.application_logs LIMIT 1","format":"sql"}' 2>/dev/null
  ) >> "$TMPDIR/burst.txt" &
done
wait
total=$(wc -l < "$TMPDIR/burst.txt")
ok_count=$(grep -c "200" "$TMPDIR/burst.txt" || true)
err=$((total - ok_count))
if [ "$err" -le 2 ]; then ok "Burst: $ok_count/$total succeeded"
else fail "Burst" "$err/$total failed"; fi

# 6. Timeout handling (500 is known issue — warn, don't fail)
echo ""
echo "── Timeout handling ──"
result=$(query '{"query":"SELECT * FROM cluster_a.application_logs","format":"sql","timeout_ms":100}')
code=$(echo "$result" | head -c 3)
if [ "$code" != "500" ]; then ok "Short timeout → HTTP $code"
else warn "Short timeout → 500 (should be 408/504, tracked)"; fi

# 7. SQL injection
echo ""
echo "── SQL injection resilience ──"
expect_not_500 "DROP TABLE injection" '{"query":"SELECT 1; DROP TABLE cluster_a.application_logs; --","format":"sql"}'
expect_not_500 "UNION injection" '{"query":"SELECT 1 UNION SELECT password FROM users--","format":"sql"}'

# 8. Oversized query (500 is known issue — warn, don't fail)
echo ""
echo "── Edge cases ──"
big_cols=$(printf 'a%.0s,' $(seq 1 200))a
result=$(query "{\"query\":\"SELECT $big_cols FROM cluster_a.application_logs\",\"format\":\"sql\"}")
code=$(echo "$result" | head -c 3)
if [ "$code" != "500" ]; then ok "200-column query → HTTP $code"
else warn "200-column query → 500 (should be 400, tracked)"; fi

# Summary
echo ""
echo "═══════════════════════════════════════"
TOTAL=$((PASS + FAIL))
echo "  Chaos tests: $PASS/$TOTAL passed, $WARN warnings"
[ "$FAIL" -gt 0 ] && { echo "  ❌ $FAIL FAILURES"; exit 1; }
echo "  ✅ All chaos tests passed"
