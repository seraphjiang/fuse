#!/usr/bin/env bash
# Playground UI regression test — verify structure of all HTML pages
set -euo pipefail

DIR="$(cd "$(dirname "$0")/../../playground" && pwd)"
PASS=0 FAIL=0

check() {
  local name="$1" result="$2"
  if [ "$result" = "ok" ]; then
    echo "  ✅ $name"
    PASS=$((PASS + 1))
  else
    echo "  ❌ $name — $result"
    FAIL=$((FAIL + 1))
  fi
}

echo "🔍 Playground UI regression test"
echo "   Dir: $DIR"
echo ""

# 1. All expected pages exist
echo "── Page existence ──"
PAGES=(index dashboard explore settings status help admin alerts views plugins changelog terminal federation)
for p in "${PAGES[@]}"; do
  [ -f "$DIR/$p.html" ] && check "$p.html exists" "ok" || check "$p.html exists" "missing"
done

# 2. Every page has required structure
echo ""
echo "── Required structure ──"
for f in "$DIR"/*.html; do
  name=$(basename "$f")
  [ "$name" = "feedback-widget.html" ] && continue

  # DOCTYPE
  grep -q '<!DOCTYPE html>' "$f" && r="ok" || r="missing <!DOCTYPE html>"
  check "$name: DOCTYPE" "$r"

  # Viewport meta
  grep -q 'viewport' "$f" && r="ok" || r="missing viewport meta"
  check "$name: viewport" "$r"

  # Theme support
  grep -q 'fuse-theme' "$f" && r="ok" || r="missing theme persistence"
  check "$name: theme" "$r"

  # prefers-color-scheme
  grep -q 'prefers-color-scheme' "$f" && r="ok" || r="missing system preference"
  check "$name: system-pref" "$r"
done

# 3. index.html specific features
echo ""
echo "── index.html features ──"
IDX="$DIR/index.html"

grep -q 'id="editor"' "$IDX" && check "Query editor" "ok" || check "Query editor" "missing"
grep -q 'id="run-btn"' "$IDX" && check "Run button" "ok" || check "Run button" "missing"
grep -q 'id="results"' "$IDX" && check "Results area" "ok" || check "Results area" "missing"
grep -q 'downloadCSV' "$IDX" && check "CSV export" "ok" || check "CSV export" "missing"
grep -q 'downloadJSON' "$IDX" && check "JSON export" "ok" || check "JSON export" "missing"
grep -q 'copyResults' "$IDX" && check "Copy button" "ok" || check "Copy button" "missing"
grep -q 'flame-canvas' "$IDX" && check "Flame graph" "ok" || check "Flame graph" "missing"
grep -q 'dag-canvas' "$IDX" && check "DAG visualization" "ok" || check "DAG visualization" "missing"
grep -q 'cost-badge' "$IDX" && check "Cost badge" "ok" || check "Cost badge" "missing"
grep -q 'hist-search' "$IDX" && check "History search" "ok" || check "History search" "missing"
grep -q 'hist-format-filter' "$IDX" && check "History format filter" "ok" || check "History format filter" "missing"
grep -q 'hist-status-filter' "$IDX" && check "History status filter" "ok" || check "History status filter" "missing"
grep -q 'schema-panel' "$IDX" && check "Schema explorer" "ok" || check "Schema explorer" "missing"
grep -q 'ac-dropdown' "$IDX" && check "Autocomplete dropdown" "ok" || check "Autocomplete dropdown" "missing"
grep -q 'parts\[2\]' "$IDX" && check "Dot-triggered autocomplete" "ok" || check "Dot-triggered autocomplete" "missing"
grep -q 'chart-container' "$IDX" && check "Chart container" "ok" || check "Chart container" "missing"

# 4. settings.html specific
echo ""
echo "── settings.html features ──"
SET="$DIR/settings.html"
grep -q 'testConnection' "$SET" && check "Connection test button" "ok" || check "Connection test button" "missing"
grep -q 'ds-list' "$SET" && check "Datasource list" "ok" || check "Datasource list" "missing"

# 5. status.html specific
echo ""
echo "── status.html features ──"
STA="$DIR/status.html"
grep -q 'conn-grid' "$STA" && check "Connector grid" "ok" || check "Connector grid" "missing"
grep -q 'timeline-body' "$STA" && check "Health timeline" "ok" || check "Health timeline" "missing"
grep -q 'setInterval' "$STA" && check "Auto-refresh" "ok" || check "Auto-refresh" "missing"

# 6. alerts.html specific
echo ""
echo "── alerts.html features ──"
ALR="$DIR/alerts.html"
grep -q 'filter-status\|filter-search\|filterHistory\|status.*dropdown' "$ALR" && check "Alert history filter" "ok" || check "Alert history filter" "missing"

# 7. Responsive CSS
echo ""
echo "── Responsive CSS ──"
RESPONSIVE_PAGES=(index dashboard settings admin status explore)
for p in "${RESPONSIVE_PAGES[@]}"; do
  grep -q '@media' "$DIR/$p.html" && check "$p.html: @media" "ok" || check "$p.html: @media" "missing"
done

# Summary
echo ""
echo "════════════════════════════"
echo "  ✅ Passed: $PASS"
echo "  ❌ Failed: $FAIL"
echo "════════════════════════════"
[ "$FAIL" -eq 0 ] && echo "🎉 All UI regression tests passed!" || exit 1
