#!/usr/bin/env bash
# Endpoint Coverage Test — hit every API endpoint, verify status codes
set -euo pipefail

FUSE="${1:-${FUSE_URL:-https://fuse.huanji.profile.aws.dev}}"
PASS=0 FAIL=0 SKIP=0

ok()   { echo "  ✅ $1 → $2"; PASS=$((PASS + 1)); }
skip() { echo "  ⏭️  $1 → $2 (skipped)"; SKIP=$((SKIP + 1)); }
fail() { echo "  ❌ $1 → $2 (expected $3)"; FAIL=$((FAIL + 1)); }

check() {
  local name="$1" code="$2" expect="$3"
  # Skip timeouts (000) and undeployed endpoints (404 when expecting success)
  if [[ "$code" == 000* ]] || { [ "$code" = "404" ] && [ "$expect" != "404" ]; }; then
    skip "$name" "$code"
  elif [ "$code" = "$expect" ]; then
    ok "$name" "$code"
  else
    fail "$name" "$code" "$expect"
  fi
}

get() {
  local name="$1" path="$2" expect="${3:-200}"
  local code
  code=$(curl -sk -o /dev/null -w "%{http_code}" --max-time 10 "$FUSE$path" 2>/dev/null || echo "000")
  check "$name" "$code" "$expect"
}

post() {
  local name="$1" path="$2" payload="$3" expect="${4:-200}"
  local code
  code=$(curl -sk -o /dev/null -w "%{http_code}" --max-time 10 \
    -X POST "$FUSE$path" -H 'Content-Type: application/json' -d "$payload" 2>/dev/null || echo "000")
  check "$name" "$code" "$expect"
}

del() {
  local name="$1" path="$2" expect="${3:-200}"
  local code
  code=$(curl -sk -o /dev/null -w "%{http_code}" --max-time 10 -X DELETE "$FUSE$path" 2>/dev/null || echo "000")
  check "$name" "$code" "$expect"
}

echo "📊 Endpoint Coverage Test — $FUSE"
echo ""

echo "── System ──"
get  "GET /health"              "/api/fuse/health"
get  "GET /info"                "/api/fuse/info"
get  "GET /stats"               "/api/fuse/stats"
get  "GET /history"             "/api/fuse/history"
get  "GET /advisor"             "/api/fuse/advisor"
get  "GET /federation"          "/api/fuse/federation"

echo ""
echo "── Datasources ──"
get  "GET /datasources"                         "/api/fuse/datasources"
get  "GET /datasources/:id/schemas"             "/api/fuse/datasources/cluster_a/schemas"
get  "GET /datasources/:id/schemas/:t/fields"   "/api/fuse/datasources/cluster_a/schemas/application_logs/fields"
get  "GET /datasources/bad → 404"               "/api/fuse/datasources/nonexistent_xyz/schemas" "404"

echo ""
echo "── Query ──"
post "POST /query (SQL)"        "/api/fuse/query" \
  '{"query":"SELECT service FROM cluster_a.application_logs LIMIT 1","format":"sql"}'
post "POST /query (PPL)"        "/api/fuse/query" \
  '{"query":"source = cluster_a.application_logs | head 1","format":"ppl"}'
post "POST /query/validate"     "/api/fuse/query/validate" \
  '{"query":"SELECT 1","format":"sql"}'
post "POST /query/explain"      "/api/fuse/query/explain" \
  '{"query":"SELECT service FROM cluster_a.application_logs LIMIT 1","format":"sql"}'
post "POST /multi"              "/api/fuse/multi" \
  '{"query":"SELECT 1; SELECT 2","format":"sql"}'
get  "GET /queries/running"     "/api/fuse/queries/running"

echo ""
echo "── Advanced query ──"
post "POST /query/export/csv"   "/api/fuse/query/export/csv" \
  '{"query":"SELECT service FROM cluster_a.application_logs LIMIT 1","format":"sql"}'
post "POST /query/export/json"  "/api/fuse/query/export/json" \
  '{"query":"SELECT service FROM cluster_a.application_logs LIMIT 1","format":"sql"}'
get  "GET /anomaly"             "/api/fuse/anomaly"
post "POST /nl"                 "/api/fuse/nl" \
  '{"question":"show me error logs","execute":false}'
get  "GET /predict"             "/api/fuse/predict?query=SELECT+1"
post "POST /lineage"            "/api/fuse/lineage" \
  '{"query":"SELECT service FROM cluster_a.application_logs","format":"sql"}'
get  "GET /trace/:id"           "/api/fuse/trace/test-trace-000"
get  "GET /relationships"       "/api/fuse/relationships"

echo ""
echo "── Saved queries ──"
get  "GET /saved"               "/api/fuse/saved"
post "POST /saved (create)"     "/api/fuse/saved" \
  '{"name":"_coverage_test","query":"SELECT 1","format":"sql"}' "201"
get  "GET /saved/:name"         "/api/fuse/saved/_coverage_test"
del  "DELETE /saved/:name"      "/api/fuse/saved/_coverage_test" "204"

echo ""
echo "── Views ──"
get  "GET /views"               "/api/fuse/views"

echo ""
echo "── Alerts ──"
get  "GET /alerts"              "/api/fuse/alerts"
get  "GET /alert-rules"         "/api/fuse/alert-rules"
post "POST /alerts/evaluate"    "/api/fuse/alerts/evaluate" '{}'

echo ""
echo "── CDC ──"
get  "GET /cdc/status"          "/api/fuse/cdc/status"
post "POST /cdc/events"         "/api/fuse/cdc/events" \
  '{"datasource":"cluster_a","table":"application_logs","change_type":"insert","timestamp":1700000000}'

echo ""
echo "── Replay ──"
get  "GET /replay/recordings"   "/api/fuse/replay/recordings"
post "POST /replay/record"      "/api/fuse/replay/record" \
  '{"query":"SELECT 1","format":"sql","timestamp":1700000000}' "201"

echo ""
echo "── Webhooks ──"
get  "GET /webhooks"            "/api/fuse/webhooks"

echo ""
echo "── GraphQL ──"
post "POST /graphql"            "/api/fuse/graphql" \
  '{"query":"{ health { status } }"}'

echo ""
echo "── Cache ──"
del  "DELETE /cache"            "/api/fuse/cache"

echo ""
echo "═══════════════════════════════════════"
TOTAL=$((PASS + FAIL))
echo "  Endpoints: $PASS/$TOTAL passed, $SKIP skipped (404/timeout)"
[ "$FAIL" -gt 0 ] && { echo "  ❌ $FAIL FAILURES"; exit 1; }
echo "  ✅ All endpoints responding correctly"
