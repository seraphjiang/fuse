#!/usr/bin/env bash
# E2E integration test suite — spins up Fuse + dependencies, runs tests.
# Usage: ./tests/integration/e2e.sh [--no-cleanup]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLEANUP=true
[[ "${1:-}" == "--no-cleanup" ]] && CLEANUP=false

FUSE_URL="http://localhost:9400"
PASS=0
FAIL=0
TOTAL=0

log() { echo "=== $1 ==="; }
pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo "  ✅ $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo "  ❌ $1: $2"; }

assert_status() {
  local name="$1" method="$2" url="$3" expected="$4"
  shift 4
  local status
  status=$(curl -s -o /dev/null -w "%{http_code}" -X "$method" "$url" "$@")
  if [[ "$status" == "$expected" ]]; then pass "$name"; else fail "$name" "expected $expected, got $status"; fi
}

assert_json() {
  local name="$1" method="$2" url="$3" jq_expr="$4"
  shift 4
  local body
  body=$(curl -s -X "$method" "$url" "$@")
  if echo "$body" | jq -e "$jq_expr" > /dev/null 2>&1; then pass "$name"; else fail "$name" "jq '$jq_expr' failed on: $(echo "$body" | head -c 200)"; fi
}

cleanup() {
  if $CLEANUP; then
    log "Cleanup"
    kill "$FUSE_PID" 2>/dev/null || true
    wait "$FUSE_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# Build
log "Building Fuse"
cd "$ROOT_DIR"
cargo build -p fuse-server --release 2>&1 | tail -3

# Start Fuse with test config
log "Starting Fuse"
FUSE_CONFIG="$ROOT_DIR/fuse.toml" ./target/release/fuse-server &
FUSE_PID=$!
sleep 2

# Wait for health
for i in $(seq 1 10); do
  curl -sf "$FUSE_URL/api/fuse/health" > /dev/null 2>&1 && break
  sleep 1
done
curl -sf "$FUSE_URL/api/fuse/health" > /dev/null || { echo "Fuse failed to start"; exit 1; }

log "Health & Metadata"
assert_status "health_200" GET "$FUSE_URL/api/fuse/health" 200
assert_json "health_status" GET "$FUSE_URL/api/fuse/health" '.status'
assert_status "datasources_200" GET "$FUSE_URL/api/fuse/datasources" 200
assert_json "datasources_array" GET "$FUSE_URL/api/fuse/datasources" '. | type == "array"'

log "Query — SQL"
assert_json "sql_query" POST "$FUSE_URL/api/fuse/query" '.columns' \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1 as n","format":"sql"}'

log "Query — Validation"
assert_status "validate_200" POST "$FUSE_URL/api/fuse/query/validate" 200 \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT 1","format":"sql"}'

log "Query — EXPLAIN"
assert_json "explain" POST "$FUSE_URL/api/fuse/query" '.columns' \
  -H 'Content-Type: application/json' \
  -d '{"query":"EXPLAIN SELECT 1","format":"sql"}'

log "Saved Queries"
assert_status "save_query" POST "$FUSE_URL/api/fuse/saved" 200 \
  -H 'Content-Type: application/json' \
  -d '{"name":"test_e2e","query":"SELECT 1","format":"sql"}'
assert_json "list_saved" GET "$FUSE_URL/api/fuse/saved" '. | length >= 1'
assert_status "delete_saved" DELETE "$FUSE_URL/api/fuse/saved/test_e2e" 200

log "History & Stats"
assert_status "history_200" GET "$FUSE_URL/api/fuse/history" 200
assert_status "stats_200" GET "$FUSE_URL/api/fuse/stats" 200

log "Federation"
assert_status "federation_200" GET "$FUSE_URL/api/fuse/federation" 200
assert_json "federation_topology" GET "$FUSE_URL/api/fuse/federation" '.instance_count >= 0'

log "Server Info"
assert_status "info_200" GET "$FUSE_URL/api/fuse/info" 200
assert_json "info_version" GET "$FUSE_URL/api/fuse/info" '.version'
assert_json "info_uptime" GET "$FUSE_URL/api/fuse/info" '.uptime_secs >= 0'

log "Playground"
assert_status "playground_200" GET "$FUSE_URL/" 200
assert_status "settings_200" GET "$FUSE_URL/settings" 200
assert_status "status_200" GET "$FUSE_URL/status" 200

log "Cache Management"
assert_status "cache_clear" DELETE "$FUSE_URL/api/fuse/cache" 200

log "Error Handling"
assert_status "bad_query_400" POST "$FUSE_URL/api/fuse/query" 400 \
  -H 'Content-Type: application/json' \
  -d '{"query":"","format":"sql"}'
assert_status "not_found_404" GET "$FUSE_URL/api/fuse/nonexistent" 404

echo ""
log "Results: $PASS passed, $FAIL failed, $TOTAL total"
[[ $FAIL -eq 0 ]] && echo "🎉 All E2E tests passed!" || { echo "💥 $FAIL test(s) failed"; exit 1; }
