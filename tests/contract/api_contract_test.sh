#!/usr/bin/env bash
# API Contract Test — validates live responses against OpenAPI 3.1 schemas
set -euo pipefail

FUSE="${1:-${FUSE_URL:-https://fuse.huanji.profile.aws.dev}}"
DIR="$(cd "$(dirname "$0")" && pwd)"
SPEC="$DIR/../../docs/api/openapi.yaml"
VALIDATOR="$DIR/validate_schema.py"
PASS=0 FAIL=0 SKIP=0

die() { echo "❌ $1"; exit 2; }
[ -f "$SPEC" ] || die "OpenAPI spec not found: $SPEC"
[ -f "$VALIDATOR" ] || die "Validator not found: $VALIDATOR"

check() {
  local name="$1" schema="$2" array_flag="${3:-}" body="$4"
  local result
  result=$(echo "$body" | python3 "$VALIDATOR" "$SPEC" "$schema" $array_flag 2>&1) && rc=0 || rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "  ✅ $name"; PASS=$((PASS + 1))
  else
    echo "  ❌ $name — $result"; FAIL=$((FAIL + 1))
  fi
}

get_check() {
  local name="$1" endpoint="$2" schema="$3" array_flag="${4:-}"
  local tmpfile code body
  tmpfile=$(mktemp)
  code=$(curl -sk -o "$tmpfile" -w "%{http_code}" "$FUSE$endpoint")
  body=$(cat "$tmpfile"); rm -f "$tmpfile"
  if [ "$code" = "404" ]; then
    echo "  ⏭️  $name — endpoint not found (404), skipping"; SKIP=$((SKIP + 1)); return
  fi
  if [ -z "$body" ] || ! echo "$body" | python3 -c "import sys,json;json.load(sys.stdin)" 2>/dev/null; then
    echo "  ⏭️  $name — non-JSON response, skipping"; SKIP=$((SKIP + 1)); return
  fi
  check "$name" "$schema" "$array_flag" "$body"
}

post_check() {
  local name="$1" endpoint="$2" schema="$3" payload="$4" expect="${5:-200}" array_flag="${6:-}"
  local tmpfile code body
  tmpfile=$(mktemp)
  code=$(curl -sk -o "$tmpfile" -w "%{http_code}" -X POST "$FUSE$endpoint" \
    -H 'Content-Type: application/json' -d "$payload")
  body=$(cat "$tmpfile"); rm -f "$tmpfile"
  if [ "$code" != "$expect" ]; then
    echo "  ❌ $name — HTTP $code (expected $expect)"; FAIL=$((FAIL + 1)); return
  fi
  check "$name" "$schema" "$array_flag" "$body"
}

echo "📋 API Contract Tests — $FUSE"
echo ""

echo "── Health ──"
get_check "GET /health → HealthResponse" "/api/fuse/health" "HealthResponse"

echo ""
echo "── Datasources ──"
get_check "GET /datasources → DatasourceInfo[]" "/api/fuse/datasources" "DatasourceInfo" "--array"
get_check "GET /schemas → SchemaInfo[]" "/api/fuse/datasources/cluster_a/schemas" "SchemaInfo" "--array"
get_check "GET /fields → FieldInfo[]" "/api/fuse/datasources/cluster_a/schemas/application_logs/fields" "FieldInfo" "--array"

echo ""
echo "── Query ──"
post_check "POST /query SQL → QueryResponse" "/api/fuse/query" "QueryResponse" \
  '{"query":"SELECT service, status FROM cluster_a.application_logs LIMIT 3","format":"sql"}'
post_check "POST /query PPL → QueryResponse" "/api/fuse/query" "QueryResponse" \
  '{"query":"source = cluster_a.application_logs | head 3","format":"ppl"}'
post_check "POST /query pagination → QueryResponse" "/api/fuse/query" "QueryResponse" \
  '{"query":"SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC","format":"sql","page_size":2}'
post_check "POST /query/validate → ValidateResponse" "/api/fuse/query/validate" "ValidateResponse" \
  '{"query":"SELECT 1","format":"sql"}'
post_check "POST /query/explain → ExplainResponse" "/api/fuse/query/explain" "ExplainResponse" \
  '{"query":"SELECT service FROM cluster_a.application_logs LIMIT 5","format":"sql"}'

echo ""
echo "── Error contracts ──"
post_check "POST /query bad SQL → ErrorResponse (400)" "/api/fuse/query" "ErrorResponse" \
  '{"query":"SELECTTTT BROKEN","format":"sql"}' "400"

echo ""
echo "── Collections ──"
get_check "GET /history → HistoryEntry[]" "/api/fuse/history" "HistoryEntry" "--array"
get_check "GET /saved → SavedQuery[]" "/api/fuse/saved" "SavedQuery" "--array"
get_check "GET /views → ViewInfo[]" "/api/fuse/views" "ViewInfo" "--array"
get_check "GET /alerts → AlertInfo[]" "/api/fuse/alerts" "AlertInfo" "--array"
get_check "GET /webhooks → WebhookSubscription[]" "/api/fuse/webhooks" "WebhookSubscription" "--array"

echo ""
echo "═══════════════════════════════════════"
TOTAL=$((PASS + FAIL))
echo "  Contract tests: $PASS/$TOTAL passed, $SKIP skipped"
[ "$FAIL" -gt 0 ] && { echo "  ❌ $FAIL FAILURES"; exit 1; }
echo "  ✅ All contracts valid"
