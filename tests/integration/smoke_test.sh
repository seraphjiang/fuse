#!/usr/bin/env bash
# Live site smoke test — hits every Fuse endpoint
set -euo pipefail

FUSE="${FUSE_URL:-https://fuse.huanji.profile.aws.dev}"
PASS=0 FAIL=0

check() {
  local name="$1" expect="$2" actual="$3"
  if [ "$actual" = "$expect" ]; then
    echo "  ✅ $name ($actual)"
    PASS=$((PASS + 1))
  else
    echo "  ❌ $name (expected $expect, got $actual)"
    FAIL=$((FAIL + 1))
  fi
}

echo "🔍 Smoke testing $FUSE"
echo ""

# GET endpoints
echo "── GET endpoints ──"
for ep in health datasources stats history queries/running saved views advisor alerts; do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$FUSE/api/fuse/$ep")
  check "GET /api/fuse/$ep" "200" "$code"
done

# Datasource detail
echo ""
echo "── Schema endpoints ──"
code=$(curl -s -o /dev/null -w "%{http_code}" "$FUSE/api/fuse/datasources/cluster_a/schemas")
check "GET schemas" "200" "$code"
code=$(curl -s -o /dev/null -w "%{http_code}" "$FUSE/api/fuse/datasources/cluster_a/schemas/application_logs/fields")
check "GET fields" "200" "$code"

# POST endpoints
echo ""
echo "── POST endpoints ──"
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$FUSE/api/fuse/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT service, status FROM cluster_a.application_logs LIMIT 3","format":"sql"}')
check "POST query (SQL)" "200" "$code"

code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$FUSE/api/fuse/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"source = cluster_a.application_logs | head 3","format":"ppl"}')
check "POST query (PPL)" "200" "$code"

code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$FUSE/api/fuse/query/explain" \
  -H 'Content-Type: application/json' \
  -d '{"query":"EXPLAIN SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}')
check "POST explain" "200" "$code"

code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$FUSE/api/fuse/query/validate" \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT * FROM cluster_a.application_logs LIMIT 5","format":"sql"}')
check "POST validate" "200" "$code"

code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$FUSE/api/fuse/multi" \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1; SELECT 2","format":"sql"}')
check "POST multi" "200" "$code"

code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$FUSE/api/fuse/nl" \
  -H 'Content-Type: application/json' \
  -d '{"question":"show me errors"}')
check "POST nl" "200" "$code"

code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$FUSE/api/fuse/query/stream" \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1","format":"sql"}')
check "POST stream" "200" "$code"

code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$FUSE/api/fuse/alerts/evaluate")
check "POST alerts/evaluate" "200" "$code"

# Cross-source JOIN
echo ""
echo "── Cross-source JOIN ──"
rows=$(curl -s -X POST "$FUSE/api/fuse/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT a.service, b.service FROM cluster_a.application_logs a JOIN cluster_b.application_logs b ON a.trace_id = b.trace_id LIMIT 3","format":"sql"}' \
  | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('rows',[])))" 2>/dev/null || echo "0")
if [ "$rows" -gt 0 ] 2>/dev/null; then
  echo "  ✅ Cross-source JOIN returned $rows rows"
  PASS=$((PASS + 1))
else
  echo "  ❌ Cross-source JOIN returned 0 rows"
  FAIL=$((FAIL + 1))
fi

# Playground pages (core = must pass, recent = warn-only if 404 due to deploy lag)
echo ""
echo "── Playground pages ──"
CORE_PAGES=( "" status settings dashboard admin explore help changelog )
RECENT_PAGES=( alerts views plugins terminal federation schedules quality lineage replay cost )

for page in "${CORE_PAGES[@]}"; do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$FUSE/$page")
  check "GET /${page:-index}" "200" "$code"
done
for page in "${RECENT_PAGES[@]}"; do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$FUSE/$page")
  if [ "$code" = "200" ]; then
    echo "  ✅ GET /$page ($code)"
    PASS=$((PASS + 1))
  else
    echo "  ⚠️  GET /$page ($code) — may not be deployed yet"
  fi
done

# Summary
echo ""
echo "════════════════════════════"
echo "  ✅ Passed: $PASS"
echo "  ❌ Failed: $FAIL"
echo "════════════════════════════"
[ "$FAIL" -eq 0 ] && echo "🎉 All smoke tests passed!" || exit 1
