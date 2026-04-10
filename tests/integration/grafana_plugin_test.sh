#!/usr/bin/env bash
# #722 Grafana plugin verification
set -euo pipefail

PLUGIN_DIR="grafana-plugin/fuse-datasource"
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
echo "║  #722 Grafana Plugin Verification                ║"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Structure checks
echo "Plugin structure:"
run "plugin.json exists" test -f "$PLUGIN_DIR/plugin.json"
run "plugin.json has type=datasource" bash -c "python3 -c \"import json; d=json.load(open('$PLUGIN_DIR/plugin.json')); assert d['type']=='datasource'\""
run "plugin.json has valid id" bash -c "python3 -c \"import json; d=json.load(open('$PLUGIN_DIR/plugin.json')); assert len(d['id']) > 0\""
run "package.json exists" test -f "$PLUGIN_DIR/package.json"
run "module.ts exists (entry point)" test -f "$PLUGIN_DIR/src/module.ts"
run "datasource.ts exists" test -f "$PLUGIN_DIR/src/datasource.ts"
run "query_editor.ts exists" test -f "$PLUGIN_DIR/src/query_editor.ts"
run "config_editor.ts exists" test -f "$PLUGIN_DIR/src/config_editor.ts"
run "types.ts exists" test -f "$PLUGIN_DIR/src/types.ts"

# API contract checks
echo ""
echo "API contract:"
run "datasource.ts calls /api/fuse/query" grep -q "/api/fuse/query" "$PLUGIN_DIR/src/datasource.ts"
run "datasource.ts sends format field" grep -q "format" "$PLUGIN_DIR/src/datasource.ts"
run "datasource.ts handles columns+rows" grep -q "columns" "$PLUGIN_DIR/src/datasource.ts"
run "datasource.ts supports API key" grep -q "apiKey\|api_key\|API-Key\|X-API-Key" "$PLUGIN_DIR/src/datasource.ts"
run "datasource.ts implements testDatasource" grep -q "testDatasource\|healthCheck\|health" "$PLUGIN_DIR/src/datasource.ts"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ]
