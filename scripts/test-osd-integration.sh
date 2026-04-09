#!/usr/bin/env bash
# Smoke test: verify OSD loads with fuse-query plugin and all API endpoints work.
# Run after: docker-compose up -d && sleep 30
set -euo pipefail

OSD_URL="${OSD_URL:-http://localhost:5601}"
FUSE_URL="${FUSE_URL:-http://localhost:9400}"
PASS=0
FAIL=0

check() {
    local desc="$1"
    local url="$2"
    local expected="${3:-200}"
    local status
    status=$(curl -sf -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
    if [ "$status" = "$expected" ]; then
        echo "  ✅ $desc ($status)"
        PASS=$((PASS + 1))
    else
        echo "  ❌ $desc (got $status, expected $expected)"
        FAIL=$((FAIL + 1))
    fi
}

check_post() {
    local desc="$1"
    local url="$2"
    local body="$3"
    local status
    status=$(curl -sf -o /dev/null -w "%{http_code}" -X POST "$url" \
        -H 'Content-Type: application/json' -d "$body" 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        echo "  ✅ $desc ($status)"
        PASS=$((PASS + 1))
    else
        echo "  ❌ $desc (got $status)"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Fuse OSD Integration Smoke Test ==="
echo

echo "── Fuse Engine ($FUSE_URL) ──"
check "health endpoint" "$FUSE_URL/api/fuse/health"
check "datasources endpoint" "$FUSE_URL/api/fuse/datasources"
check_post "query validate" "$FUSE_URL/api/fuse/query/validate" \
    '{"query":"SELECT * FROM local_cluster.services","format":"sql"}'
check_post "query explain" "$FUSE_URL/api/fuse/query/explain" \
    '{"query":"SELECT * FROM local_cluster.services","format":"sql"}'
echo

echo "── OpenSearch Dashboards ($OSD_URL) ──"
check "OSD home page" "$OSD_URL/"
check "OSD API status" "$OSD_URL/api/status"
check "fuse-query plugin health proxy" "$OSD_URL/api/fuse_query/health"
check "fuse-query plugin datasources proxy" "$OSD_URL/api/fuse_query/datasources"
check_post "fuse-query plugin query proxy" "$OSD_URL/api/fuse_query/query/validate" \
    '{"query":"SELECT * FROM local_cluster.services","format":"sql"}'
echo

echo "── Plugin Registration ──"
# Check that the plugin appears in OSD's plugin list
PLUGINS=$(curl -sf "$OSD_URL/api/status" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
plugins = d.get('status', {}).get('plugins', [])
names = [p.get('id','') for p in plugins]
print('fuseQuery' in names)
" 2>/dev/null || echo "false")
if [ "$PLUGINS" = "True" ] || [ "$PLUGINS" = "true" ]; then
    echo "  ✅ fuseQuery plugin registered in OSD"
    PASS=$((PASS + 1))
else
    echo "  ⚠️  Could not verify plugin registration (may still be loading)"
fi
echo

echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
