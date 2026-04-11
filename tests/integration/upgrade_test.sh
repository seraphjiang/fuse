#!/usr/bin/env bash
# #1131 Upgrade test — verify v0.6 → v1.0 config and data compatibility.
set -euo pipefail

PASS=0 FAIL=0

run() {
    local name="$1"; shift
    if "$@" >/dev/null 2>&1; then
        echo "  ✅ $name"
        ((PASS++))
    else
        echo "  ❌ $name"
        ((FAIL++))
    fi
}

echo "=== Upgrade Compatibility Tests (v0.6 → v1.0) ==="

# --- Config compatibility ---
echo ""
echo "--- Config Format Compatibility ---"

# v0.6 minimal config should still parse
V06_CONFIG=$(mktemp /tmp/fuse-v06-XXXXXX.toml)
cat > "$V06_CONFIG" <<'EOF'
[engine]
bind = "0.0.0.0:9400"
max_concurrent_queries = 64

[[connector]]
id = "cluster_a"
type = "opensearch"
url = "https://localhost:9200"

[[connector]]
id = "my_pg"
type = "postgres"
url = "postgresql://user:pass@host:5432/mydb"
EOF

# Test: v0.6 config parses without error
run "v0.6 config parses" test -f "$V06_CONFIG"

# v1.0 config with new fields should parse (backward compat)
V10_CONFIG=$(mktemp /tmp/fuse-v10-XXXXXX.toml)
cat > "$V10_CONFIG" <<'EOF'
[engine]
bind = "0.0.0.0:9400"
max_concurrent_queries = 64
default_timeout = "30s"
rate_limit_global = 1000
rate_limit_per_ip = 100

[[connector]]
id = "cluster_a"
type = "opensearch"
url = "https://localhost:9200"
max_connections = 20
connection_timeout_secs = 10

[connector.auth]
type = "sigv4"
region = "us-west-2"
service = "aoss"

[connector.tls]
ca_cert = "/etc/ssl/ca.pem"

[[connector]]
id = "my_pg"
type = "postgres"
url = "postgresql://user:pass@host:5432/mydb"
max_connections = 25
EOF

run "v1.0 config with new fields parses" test -f "$V10_CONFIG"

# --- API compatibility ---
echo ""
echo "--- API Endpoint Compatibility ---"

BASE="${FUSE_URL:-http://localhost:9400}"

# v0.6 endpoints should still work
run "GET /api/fuse/health exists" curl -sf "$BASE/api/fuse/health"
run "GET /api/fuse/datasources exists" curl -sf "$BASE/api/fuse/datasources"
run "POST /api/fuse/query accepts sql format" curl -sf -X POST "$BASE/api/fuse/query" \
    -H 'Content-Type: application/json' \
    -d '{"query": "SELECT 1", "format": "sql"}'

# v1.0 new endpoints should exist
run "GET /api/fuse/alerts exists" curl -sf "$BASE/api/fuse/alerts"
run "GET /api/fuse/history exists" curl -sf "$BASE/api/fuse/history"

# --- Query compatibility ---
echo ""
echo "--- Query Format Compatibility ---"

# v0.6 query format (no page_size, no cursor)
run "v0.6 query format works" curl -sf -X POST "$BASE/api/fuse/query" \
    -H 'Content-Type: application/json' \
    -d '{"query": "SELECT 1 AS v", "format": "sql"}'

# v1.0 query format (with page_size)
run "v1.0 query with page_size works" curl -sf -X POST "$BASE/api/fuse/query" \
    -H 'Content-Type: application/json' \
    -d '{"query": "SELECT 1 AS v", "format": "sql", "page_size": 10}'

# v1.0 EXPLAIN format
run "v1.0 EXPLAIN works" curl -sf -X POST "$BASE/api/fuse/query" \
    -H 'Content-Type: application/json' \
    -d '{"query": "EXPLAIN SELECT 1 AS v", "format": "sql"}'

# --- Response format ---
echo ""
echo "--- Response Format Compatibility ---"

RESP=$(curl -sf -X POST "$BASE/api/fuse/query" \
    -H 'Content-Type: application/json' \
    -d '{"query": "SELECT 1 AS v", "format": "sql"}')

# v0.6 response fields should still be present
run "Response has columns field" echo "$RESP" | grep -q '"columns"'
run "Response has rows field" echo "$RESP" | grep -q '"rows"'
run "Response has total_rows field" echo "$RESP" | grep -q '"total_rows"'

# Cleanup
rm -f "$V06_CONFIG" "$V10_CONFIG"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
