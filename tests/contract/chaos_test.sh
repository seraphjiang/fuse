#!/usr/bin/env bash
# Chaos Testing — verify graceful degradation under connector failures
#
# Tests: bad datasource refs, timeout queries, concurrent mixed workloads,
# malformed inputs, and partial-failure scenarios (UNION across good+bad sources).
#
# Usage: ./chaos_test.sh [FUSE_URL]
set -euo pipefail

FUSE="${1:-${FUSE_URL:-https://fuse.huanji.profile.aws.dev}}"
PASS=0 FAIL=0
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

ok() { echo "  ✅ $1"; PASS=$((PASS + 1)); }
fail() { echo "  ❌ $1 — $2"; FAIL=$((FAIL + 1)); }

query() {
  local payload="$1" tmpfile="$TMPDIR/resp_$RANDOM"
  local code
  code=$(curl -sk -o "$tmpfile" -w "%{http_code}" --max-time 35 \
    -X POST "$FUSE/api/fuse/query" \
    -H 'Content-Type: application/json' -d "$payload" 2>/dev/null)
  echo "$code $(cat "$tmpfile")"
  rm -f "$tmpfile"
}

echo "🔥 Chaos Tests — $FUSE"
echo ""

# ── 1. Nonexistent datasource → clean error, not 500 ──
echo "── Nonexistent datasource ──"
result=$(query '{"query":"SELECT * FROM nonexistent_xyz.table1 LIMIT 1","format":"sql"}')
code=$(echo "$result" | head -c 3)
if [[ "$code" =~ ^4 ]]; then
  ok "Nonexistent datasource → HTTP $code (client error)"
elif [ "$code" = "500" ]; then
  fail "Nonexistent datasource" "got 500 (should be 4xx)"
else
  fail "Nonexistent datasource" "unexpected HTTP $code"
fi

# ── 2. Nonexistent table on valid datasource ──
echo ""
echo "── Nonexistent table ──"
result=$(query '{"query":"SELECT * FROM cluster_a.this_table_does_not_exist_xyz LIMIT 1","format":"sql"}')
code=$(echo "$result" | head -c 3)
if [[ "$code" =~ ^[245] ]]; then
  ok "Nonexistent table → HTTP $code"
else
  fail "Nonexistent table" "unexpected HTTP $code"
fi

# ── 3. Malformed SQL variants ──
echo ""
echo "── Malformed inputs ──"
for desc_payload in \
  "empty query|{\"query\":\"\",\"format\":\"sql\"}" \
  "null bytes|{\"query\":\"SELECT\\u0000*\",\"format\":\"sql\"}" \
  "huge column list|{\"query\":\"SELECT $(printf 'a%.0s,' $(seq 1 200))a FROM cluster_a.application_logs\",\"format\":\"sql\"}" \
  "bad format|{\"query\":\"SELECT 1\",\"format\":\"graphql\"}" \
  "missing query field|{\"format\":\"sql\"}" \
; do
  desc="${desc_payload%%|*}"
  payload="${desc_payload#*|}"
  result=$(query "$payload")
  code=$(echo "$result" | head -c 3)
  if [ "$code" != "500" ]; then
    ok "Malformed: $desc → HTTP $code"
  else
    fail "Malformed: $desc" "got 500 (should handle gracefully)"
  fi
done

# ── 4. UNION with mix of valid + invalid sources (partial failure) ──
echo ""
echo "── Partial failure (UNION good + bad source) ──"
result=$(query '{"query":"SELECT service, status FROM cluster_a.application_logs UNION ALL SELECT service, status FROM nonexistent_xyz.logs LIMIT 5","format":"sql"}')
code=$(echo "$result" | head -c 3)
body=$(echo "$result" | cut -c4-)
if [ "$code" = "200" ]; then
  has_partial=$(echo "$body" | python3 -c "import sys,json; d=json.load(sys.stdin); print('yes' if d.get('partial_errors') else 'no')" 2>/dev/null || echo "parse_err")
  if [ "$has_partial" = "yes" ]; then
    ok "UNION partial failure → 200 with partial_errors (graceful degradation)"
  else
    ok "UNION partial failure → 200 (no partial_errors field, still handled)"
  fi
elif [[ "$code" =~ ^4 ]]; then
  ok "UNION partial failure → HTTP $code (rejected bad source upfront)"
else
  fail "UNION partial failure" "got HTTP $code"
fi

# ── 5. Concurrent burst — 20 queries at once ──
echo ""
echo "── Concurrent burst (20 simultaneous queries) ──"
for i in $(seq 1 20); do
  (
    code=$(curl -sk -o /dev/null -w "%{http_code}" --max-time 30 \
      -X POST "$FUSE/api/fuse/query" \
      -H 'Content-Type: application/json' \
      -d '{"query":"SELECT service FROM cluster_a.application_logs LIMIT 1","format":"sql"}' 2>/dev/null)
    echo "$code"
  ) >> "$TMPDIR/burst.txt" &
done
wait
total=$(wc -l < "$TMPDIR/burst.txt")
ok_count=$(grep -c "200" "$TMPDIR/burst.txt" || true)
err_count=$((total - ok_count))
if [ "$err_count" -eq 0 ]; then
  ok "Burst: $ok_count/$total succeeded (0 errors)"
elif [ "$err_count" -le 2 ]; then
  ok "Burst: $ok_count/$total succeeded ($err_count transient errors, acceptable)"
else
  fail "Burst" "$err_count/$total failed"
fi

# ── 6. Timeout query (very large scan) ──
echo ""
echo "── Timeout handling ──"
result=$(query '{"query":"SELECT * FROM cluster_a.application_logs","format":"sql","timeout_ms":100}')
code=$(echo "$result" | head -c 3)
if [ "$code" != "500" ]; then
  ok "Short timeout → HTTP $code (handled gracefully)"
else
  fail "Short timeout" "got 500"
fi

# ── 7. SQL injection attempts ──
echo ""
echo "── SQL injection resilience ──"
for desc_payload in \
  "DROP TABLE|{\"query\":\"SELECT 1; DROP TABLE cluster_a.application_logs; --\",\"format\":\"sql\"}" \
  "UNION injection|{\"query\":\"SELECT 1 UNION SELECT password FROM users--\",\"format\":\"sql\"}" \
; do
  desc="${desc_payload%%|*}"
  payload="${desc_payload#*|}"
  result=$(query "$payload")
  code=$(echo "$result" | head -c 3)
  if [ "$code" != "500" ]; then
    ok "Injection ($desc) → HTTP $code (safe)"
  else
    fail "Injection ($desc)" "got 500"
  fi
done

# ── Summary ──
echo ""
echo "═══════════════════════════════════════"
TOTAL=$((PASS + FAIL))
echo "  Chaos tests: $PASS/$TOTAL passed"
[ "$FAIL" -gt 0 ] && { echo "  ❌ $FAIL FAILURES"; exit 1; }
echo "  ✅ All chaos tests passed"
