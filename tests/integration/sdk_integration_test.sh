#!/usr/bin/env bash
# #721 SDK integration tests against live playground
set -euo pipefail

BASE="${FUSE_URL:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
PASS=0 FAIL=0

run() {
    local name="$1"; shift
    if "$@" >/dev/null 2>&1; then
        PASS=$((PASS+1)); printf "  ✅ %s\n" "$name"
    else
        FAIL=$((FAIL+1)); printf "  ❌ %s\n" "$name"
    fi
}

echo "╔═══════════════════════════════════════════════════╗"
echo "║  #721 SDK Integration Tests                      ║"
echo "║  Target: $BASE"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# ── Python SDK unit tests ──
echo "Python SDK:"
if command -v python3 &>/dev/null && [ -d "sdk/python" ]; then
    cd sdk/python
    run "Python unit tests" python3 -m pytest tests/ -q 2>&1
    cd ../..
else
    echo "  ⚠️  Python not available, skipping"
fi

# ── Python SDK live test ──
echo ""
echo "Python SDK (live):"
run "Python query via curl" bash -c "
    resp=\$(curl -sk -X POST '$BASE/api/fuse/query' \
        -H 'Content-Type: application/json' \
        -d '{\"query\":\"SELECT * FROM cluster_a.application_logs LIMIT 2\",\"format\":\"sql\"}')
    echo \"\$resp\" | python3 -c 'import sys,json; d=json.load(sys.stdin); assert len(d[\"rows\"]) == 2'
"
run "Python health via curl" bash -c "
    resp=\$(curl -sk '$BASE/api/fuse/health')
    echo \"\$resp\" | python3 -c 'import sys,json; d=json.load(sys.stdin); assert d[\"status\"] == \"healthy\"'
"
run "Python trace via curl" bash -c "
    resp=\$(curl -sk '$BASE/api/fuse/trace/tr-test')
    echo \"\$resp\" | python3 -c 'import sys,json; d=json.load(sys.stdin); assert \"datasources_searched\" in d'
"

# ── TypeScript SDK unit tests ──
echo ""
echo "TypeScript SDK:"
if command -v node &>/dev/null && [ -f "sdk/typescript/tests/test_client.mjs" ]; then
    run "TypeScript unit tests" node sdk/typescript/tests/test_client.mjs
else
    echo "  ⚠️  Node.js not available, skipping"
fi

# ── API contract tests (SDK-compatible JSON format) ──
echo ""
echo "API contract (SDK compatibility):"
run "Response has columns array" bash -c "
    curl -sk -X POST '$BASE/api/fuse/query' \
        -H 'Content-Type: application/json' \
        -d '{\"query\":\"SELECT service FROM cluster_a.application_logs LIMIT 1\",\"format\":\"sql\"}' \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); assert isinstance(d[\"columns\"], list)'
"
run "Response has rows array" bash -c "
    curl -sk -X POST '$BASE/api/fuse/query' \
        -H 'Content-Type: application/json' \
        -d '{\"query\":\"SELECT service FROM cluster_a.application_logs LIMIT 1\",\"format\":\"sql\"}' \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); assert isinstance(d[\"rows\"], list)'
"
run "Response has metadata" bash -c "
    curl -sk -X POST '$BASE/api/fuse/query' \
        -H 'Content-Type: application/json' \
        -d '{\"query\":\"SELECT service FROM cluster_a.application_logs LIMIT 1\",\"format\":\"sql\"}' \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); assert \"metadata\" in d; assert \"total_rows\" in d[\"metadata\"]'
"
run "Health response has connectors" bash -c "
    curl -sk '$BASE/api/fuse/health' \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); assert \"connectors\" in d'
"
run "Error response has error field" bash -c "
    curl -sk -X POST '$BASE/api/fuse/query' \
        -H 'Content-Type: application/json' \
        -d '{\"query\":\"SELECT * FROM nonexistent.table\",\"format\":\"sql\"}' \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); assert \"error\" in d'
"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ]
