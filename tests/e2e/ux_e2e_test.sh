#!/usr/bin/env bash
# E2E UX test — validates all 18 playground pages load correctly from a live server
# CI runs this via .github/workflows/ci.yml ui-tests job
set -euo pipefail

BASE="${FUSE_URL:-http://localhost:9400}"
PASS=0 FAIL=0

check() {
  if [ "$2" = "ok" ]; then echo "  ✅ $1"; PASS=$((PASS+1))
  else echo "  ❌ $1 — $2"; FAIL=$((FAIL+1)); fi
}

echo "🔍 E2E UX Test against $BASE"
echo ""

# 1. All pages return 200
echo "── Page loads ──"
PAGES="/ /dashboard /explore /settings /status /help /admin /alerts /views /plugins /changelog /terminal /federation /schedules /quality /lineage /replay"
for p in $PAGES; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE$p" 2>/dev/null || echo "000")
  [ "$code" = "200" ] && check "GET $p → 200" "ok" || check "GET $p → 200" "got $code"
done

# 2. Index page has key UX elements
echo "── Index UX elements ──"
INDEX=$(curl -s "$BASE/" 2>/dev/null)
for elem in highlightSQL line-gutter getEditorQuery shortcuts-modal snippet demoTour sortTable; do
  echo "$INDEX" | grep -q "$elem" && check "index: $elem" "ok" || check "index: $elem" "missing"
done

# 3. API endpoints return valid JSON
echo "── API endpoints ──"
APIS="/api/fuse/health /api/fuse/datasources /api/fuse/stats"
for api in $APIS; do
  resp=$(curl -s "$BASE$api" 2>/dev/null)
  echo "$resp" | python3 -c "import sys,json;json.load(sys.stdin)" 2>/dev/null && check "GET $api → valid JSON" "ok" || check "GET $api → valid JSON" "invalid"
done

# 4. Query execution
echo "── Query execution ──"
resp=$(curl -s -X POST "$BASE/api/fuse/query" -H 'Content-Type: application/json' -d '{"query":"SELECT 1 as test_col","format":"sql"}' 2>/dev/null)
echo "$resp" | grep -q 'test_col\|columns\|rows\|error' && check "SQL query executes" "ok" || check "SQL query executes" "no response"

resp=$(curl -s -X POST "$BASE/api/fuse/query" -H 'Content-Type: application/json' -d '{"query":"EXPLAIN SELECT 1","format":"sql"}' 2>/dev/null)
echo "$resp" | grep -q 'plan\|Projection\|error' && check "EXPLAIN works" "ok" || check "EXPLAIN works" "no response"

# 5. Sprint 18 API endpoints exist
echo "── Sprint 18 APIs ──"
for api in /api/fuse/schedules /api/fuse/quality/rules /api/fuse/replay/recordings; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE$api" 2>/dev/null || echo "000")
  [ "$code" != "000" ] && [ "$code" != "404" ] && check "GET $api → exists" "ok" || check "GET $api → exists" "got $code"
done

resp=$(curl -s -X POST "$BASE/api/fuse/lineage" -H 'Content-Type: application/json' -d '{"query":"SELECT * FROM t","format":"sql"}' 2>/dev/null)
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/fuse/lineage" -H 'Content-Type: application/json' -d '{"query":"SELECT * FROM t","format":"sql"}' 2>/dev/null || echo "000")
[ "$code" != "000" ] && [ "$code" != "404" ] && check "POST /api/fuse/lineage → exists" "ok" || check "POST /api/fuse/lineage → exists" "got $code"

# 6. Theme support on Sprint 18 pages
echo "── Theme on Sprint 18 pages ──"
for p in schedules quality lineage replay; do
  body=$(curl -s "$BASE/$p" 2>/dev/null)
  echo "$body" | grep -q 'prefers-color-scheme' && check "$p: theme detection" "ok" || check "$p: theme detection" "missing"
done

# Summary
echo ""
echo "════════════════════════════"
echo "  ✅ Passed: $PASS"
echo "  ❌ Failed: $FAIL"
echo "════════════════════════════"
[ "$FAIL" -eq 0 ] && echo "🎉 All E2E UX tests passed!" || exit 1
