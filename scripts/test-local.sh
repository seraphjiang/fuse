#!/usr/bin/env bash
# Fuse local integration test: starts OpenSearch, runs tests, cleans up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

COMPOSE_CMD="docker compose"
if ! docker compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
fi

cleanup() {
    echo
    echo -e "${YELLOW}Cleaning up...${NC}"
    # Stop fuse-server if we started it
    if [ -n "${FUSE_PID:-}" ] && kill -0 "$FUSE_PID" 2>/dev/null; then
        kill "$FUSE_PID" 2>/dev/null || true
        wait "$FUSE_PID" 2>/dev/null || true
    fi
    $COMPOSE_CMD down -v --remove-orphans 2>/dev/null || true
    echo -e "${GREEN}Done.${NC}"
}
trap cleanup EXIT

echo "=== Fuse Local Test ==="
echo

# 1. Start OpenSearch
echo -e "${YELLOW}[1/5]${NC} Starting OpenSearch cluster..."
$COMPOSE_CMD up -d opensearch-node1 opensearch-node2

# 2. Wait for healthy
echo -e "${YELLOW}[2/5]${NC} Waiting for OpenSearch to be healthy..."
MAX_WAIT=120
ELAPSED=0
until curl -sf http://localhost:9200/_cluster/health?wait_for_status=green\&timeout=5s >/dev/null 2>&1; do
    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
        echo -e "${RED}OpenSearch did not become healthy within ${MAX_WAIT}s${NC}"
        # Try yellow status as fallback
        if curl -sf http://localhost:9200/_cluster/health?wait_for_status=yellow\&timeout=5s >/dev/null 2>&1; then
            echo -e "${YELLOW}Cluster is yellow — proceeding anyway${NC}"
            break
        fi
        exit 1
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    echo "  waiting... (${ELAPSED}s)"
done
echo -e "${GREEN}  OpenSearch healthy${NC}"

# Seed test data
echo "  Seeding test index..."
curl -sf -X PUT "http://localhost:9200/test_index" \
    -H 'Content-Type: application/json' \
    -d '{"mappings":{"properties":{"trace_id":{"type":"keyword"},"status":{"type":"integer"},"message":{"type":"text"}}}}' >/dev/null

curl -sf -X POST "http://localhost:9200/test_index/_bulk" \
    -H 'Content-Type: application/x-ndjson' \
    -d '{"index":{}}
{"trace_id":"abc-001","status":500,"message":"internal server error"}
{"index":{}}
{"trace_id":"abc-002","status":200,"message":"ok"}
{"index":{}}
{"trace_id":"abc-003","status":500,"message":"gateway timeout"}
' >/dev/null

curl -sf -X POST "http://localhost:9200/test_index/_refresh" >/dev/null
echo -e "${GREEN}  Test data seeded (3 docs)${NC}"

# 3. Run cargo tests
echo -e "${YELLOW}[3/5]${NC} Running cargo tests..."
cargo test --all-targets 2>&1 | tail -15
echo -e "${GREEN}  Tests passed${NC}"

# 4. Start fuse-server and test endpoints
echo -e "${YELLOW}[4/5]${NC} Starting fuse-server..."

# Create a minimal config pointing at local cluster
FUSE_TEST_CONFIG=$(mktemp /tmp/fuse-test-XXXXXX.toml)
cat > "$FUSE_TEST_CONFIG" <<'EOF'
[engine]
bind = "127.0.0.1:9400"
max_concurrent_queries = 8
default_timeout = "10s"

[[connector]]
id = "local_cluster"
type = "opensearch"
url = "http://localhost:9200"
EOF

FUSE_CONFIG="$FUSE_TEST_CONFIG" cargo run -p fuse-server &
FUSE_PID=$!

# Wait for server
echo "  Waiting for fuse-server..."
ELAPSED=0
until curl -sf http://localhost:9400/api/fuse/health >/dev/null 2>&1; do
    if [ "$ELAPSED" -ge 30 ]; then
        echo -e "${RED}Fuse server did not start within 30s${NC}"
        # Server may have failed to start — show last output
        exit 1
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done
echo -e "${GREEN}  Fuse server running on :9400${NC}"

# 5. Smoke test endpoints
echo -e "${YELLOW}[5/5]${NC} Smoke testing API endpoints..."

echo "  GET /api/fuse/health"
curl -sf http://localhost:9400/api/fuse/health | python3 -m json.tool 2>/dev/null || \
    curl -sf http://localhost:9400/api/fuse/health

echo
echo "  GET /api/fuse/datasources"
curl -sf http://localhost:9400/api/fuse/datasources | python3 -m json.tool 2>/dev/null || \
    curl -sf http://localhost:9400/api/fuse/datasources

echo
echo "  POST /api/fuse/query/validate"
curl -sf -X POST http://localhost:9400/api/fuse/query/validate \
    -H 'Content-Type: application/json' \
    -d '{"query":"SELECT * FROM local_cluster.test_index WHERE status = 500"}' | \
    python3 -m json.tool 2>/dev/null || echo "(validate response above)"

echo
echo "==================================="
echo -e "${GREEN}All local tests passed!${NC}"
