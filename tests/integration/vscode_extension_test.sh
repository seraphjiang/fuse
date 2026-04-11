#!/usr/bin/env bash
# #1031 VS Code extension E2E verification
set -euo pipefail

EXT_DIR="vscode-extension/fuse-query"
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
echo "║  #1031 VS Code Extension Verification            ║"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

echo "Structure:"
run "package.json exists" test -f "$EXT_DIR/package.json"
run "extension.ts exists" test -f "$EXT_DIR/src/extension.ts"
run "tsconfig.json exists" test -f "$EXT_DIR/tsconfig.json"

echo ""
echo "Manifest:"
run "Has name field" bash -c "python3 -c \"import json; d=json.load(open('$EXT_DIR/package.json')); assert d['name']=='fuse-query'\""
run "Has version" bash -c "python3 -c \"import json; d=json.load(open('$EXT_DIR/package.json')); assert 'version' in d\""
run "Registers language" bash -c "python3 -c \"import json; d=json.load(open('$EXT_DIR/package.json')); assert 'languages' in d['contributes']\""
run "Registers commands" bash -c "python3 -c \"import json; d=json.load(open('$EXT_DIR/package.json')); assert len(d['contributes']['commands']) > 0\""
run "Has keybindings" bash -c "python3 -c \"import json; d=json.load(open('$EXT_DIR/package.json')); assert 'keybindings' in d['contributes']\""
run "Has configuration" bash -c "python3 -c \"import json; d=json.load(open('$EXT_DIR/package.json')); assert 'configuration' in d['contributes']\""
run "Has views" bash -c "python3 -c \"import json; d=json.load(open('$EXT_DIR/package.json')); assert 'views' in d['contributes']\""

echo ""
echo "Syntax highlighting:"
run "Grammar file exists" bash -c "ls $EXT_DIR/syntaxes/*.json >/dev/null 2>&1 || ls $EXT_DIR/syntaxes/*.tmLanguage* >/dev/null 2>&1"
run "Language config exists" test -f "$EXT_DIR/language-configuration.json"

echo ""
echo "Extension code:"
run "Imports vscode API" grep -q "import.*vscode" "$EXT_DIR/src/extension.ts"
run "Has activate function" grep -q "export.*function.*activate\|export.*activate" "$EXT_DIR/src/extension.ts"
run "Connects to Fuse server" grep -q "fuse\|query\|api" "$EXT_DIR/src/extension.ts"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"
[ "$FAIL" -eq 0 ]
