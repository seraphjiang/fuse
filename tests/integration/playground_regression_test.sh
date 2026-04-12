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
PAGES=(index dashboard explore settings status help admin alerts views plugins changelog terminal federation schedules quality lineage replay feedback-widget cost graphql)
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
  grep -q '@media' "$DIR/$p.html" && check "$p responsive CSS" "ok" || check "$p responsive CSS" "missing"
done
grep -q 'max-width:480px' "$DIR/index.html" && check "index: phone breakpoint" "ok" || check "index: phone breakpoint" "missing"
for p in "${RESPONSIVE_PAGES[@]}"; do
  grep -q '@media' "$DIR/$p.html" && check "$p.html: @media" "ok" || check "$p.html: @media" "missing"
done

# 8. New Sprint 18 pages
echo ""
echo "── Sprint 18 pages ──"
for page in schedules quality lineage; do
  PG="$DIR/$page.html"
  [ -f "$PG" ] && check "$page.html: exists" "ok" || { check "$page.html: exists" "missing"; continue; }
  grep -q 'nav-tab' "$PG" && check "$page.html: nav" "ok" || check "$page.html: nav" "missing"
  grep -q 'matchMedia' "$PG" && check "$page.html: system-pref" "ok" || check "$page.html: system-pref" "missing"
  grep -q '@media' "$PG" && check "$page.html: responsive" "ok" || check "$page.html: responsive" "missing"
  grep -q 'body.light' "$PG" && check "$page.html: light-mode" "ok" || check "$page.html: light-mode" "missing"
done

# 9. Demo tour & complex JOINs
echo ""
echo "── Demo & complex queries ──"
IDX="$DIR/index.html"
grep -q 'startDemo\|DEMO_STEPS' "$IDX" && check "Demo tour" "ok" || check "Demo tour" "missing"
grep -q 'demo-bar' "$IDX" && check "Demo bar UI" "ok" || check "Demo bar UI" "missing"
grep -q '3-Way JOIN\|3-way JOIN' "$IDX" && check "3-way JOIN demo" "ok" || check "3-way JOIN demo" "missing"
grep -q 'NOT IN' "$IDX" && check "Anti-join example" "ok" || check "Anti-join example" "missing"
grep -q 'EXISTS' "$IDX" && check "Correlated subquery" "ok" || check "Correlated subquery" "missing"
grep -q 'PPL.*lookup\|lookup.*REPLACE' "$IDX" && check "PPL lookup example" "ok" || check "PPL lookup example" "missing"

# 10. Playground editor features
echo ""
echo "── Editor features ──"
grep -q 'line-gutter' "$IDX" && check "Line numbers" "ok" || check "Line numbers" "missing"
grep -q 'getEditorQuery\|selectionStart' "$IDX" && check "Run selection" "ok" || check "Run selection" "missing"
grep -q 'sortTable\|sortCol' "$IDX" && check "Column sorting" "ok" || check "Column sorting" "missing"
grep -q 'shareQuery\|🔗 Share' "$IDX" && check "Query sharing" "ok" || check "Query sharing" "missing"
grep -q 'loadSaved\|saved-list' "$IDX" && check "Saved queries UI" "ok" || check "Saved queries UI" "missing"
grep -q 'shortcuts-modal' "$IDX" && check "Keyboard shortcuts" "ok" || check "Keyboard shortcuts" "missing"
grep -q 'snippet-menu\|toggleSnippets' "$IDX" && check "Query snippets" "ok" || check "Query snippets" "missing"

# UX: Editor features
echo "── Editor UX ──"
grep -q 'highlightSQL' "$DIR/index.html" && check "Syntax highlighting function" "ok" || check "Syntax highlighting function" "missing"
grep -q 'syncHighlight' "$DIR/index.html" && check "Highlight sync on input" "ok" || check "Highlight sync on input" "missing"
grep -q 'line-gutter' "$DIR/index.html" && check "Line numbers gutter" "ok" || check "Line numbers gutter" "missing"
grep -q 'getEditorQuery' "$DIR/index.html" && check "Run selected text support" "ok" || check "Run selected text support" "missing"
grep -q 'keyboard-shortcuts\|shortcuts-modal\|Keyboard Shortcuts' "$DIR/index.html" && check "Keyboard shortcuts modal" "ok" || check "Keyboard shortcuts modal" "missing"
grep -q 'savedQueries\|saved-tab\|Saved Queries\|SAVED TAB' "$DIR/index.html" && check "Saved queries UI" "ok" || check "Saved queries UI" "missing"
grep -q 'snippet' "$DIR/index.html" && check "Query snippets dropdown" "ok" || check "Query snippets dropdown" "missing"
grep -q 'shareQuery\|location\.hash' "$DIR/index.html" && check "Query sharing via URL" "ok" || check "Query sharing via URL" "missing"
grep -q 'demoTour\|demo-bar' "$DIR/index.html" && check "Demo tour" "ok" || check "Demo tour" "missing"
grep -q 'sortTable\|sortCol' "$DIR/index.html" && check "Column sorting in results" "ok" || check "Column sorting in results" "missing"
grep -q 'downloadArrow\|arrow-btn' "$DIR/index.html" && check "Arrow IPC download button" "ok" || check "Arrow IPC download button" "missing"

# UX: Cost Explorer page
echo "── Cost Explorer UX ──"
grep -q 'estimate\|Estimate Cost' "$DIR/cost.html" && check "Cost: estimate button" "ok" || check "Cost: estimate button" "missing"
grep -q 'breakdown\|connector' "$DIR/cost.html" && check "Cost: connector breakdown" "ok" || check "Cost: connector breakdown" "missing"
grep -q 'matchMedia.*prefers-color-scheme' "$DIR/cost.html" && check "Cost: system theme detection" "ok" || check "Cost: system theme detection" "missing"

# UX: Autocomplete
echo "── Autocomplete UX ──"
grep -q 'autocomplete\|suggestion' "$DIR/index.html" && check "Autocomplete present" "ok" || check "Autocomplete present" "missing"
grep -q 'SQL_KEYWORDS\|SQL_KW_SET' "$DIR/index.html" && check "SQL keyword set" "ok" || check "SQL keyword set" "missing"
grep -q 'PPL_KEYWORDS' "$DIR/index.html" && check "PPL keyword set" "ok" || check "PPL keyword set" "missing"

# UX: Sprint 18 page features
echo "── Sprint 18 page UX ──"
grep -q 'cron\|schedule' "$DIR/schedules.html" && check "Schedules: cron support" "ok" || check "Schedules: cron support" "missing"
grep -q 'pause\|resume\|toggle' "$DIR/schedules.html" && check "Schedules: pause/resume" "ok" || check "Schedules: pause/resume" "missing"
grep -q 'null.rate\|freshness\|row.count\|cardinality' "$DIR/quality.html" && check "Quality: rule types" "ok" || check "Quality: rule types" "missing"
grep -q 'evaluate\|runAll' "$DIR/quality.html" && check "Quality: evaluate all" "ok" || check "Quality: evaluate all" "missing"
grep -q 'source.*transform\|node_type\|normalizeNodes' "$DIR/lineage.html" && check "Lineage: node types" "ok" || check "Lineage: node types" "missing"
grep -q 'replay\|replayAll\|diff' "$DIR/replay.html" && check "Replay: diff detection" "ok" || check "Replay: diff detection" "missing"

# UX: Status page widgets
echo "── Status page UX ──"
grep -q 'refreshOtel\|otel-body' "$DIR/status.html" && check "Status: OTel widget" "ok" || check "Status: OTel widget" "missing"
grep -q 'adaptive.cache\|tracked.queries\|hot.queries' "$DIR/status.html" && check "Status: adaptive cache stats" "ok" || check "Status: adaptive cache stats" "missing"

# UX: Theme support on all pages
echo "── Theme UX ──"
for p in schedules quality lineage replay; do
  grep -q 'matchMedia.*prefers-color-scheme' "$DIR/$p.html" && check "$p: system theme detection" "ok" || check "$p: system theme detection" "missing"
  grep -q 'body\.light' "$DIR/$p.html" && check "$p: light mode CSS" "ok" || check "$p: light mode CSS" "missing"
done

# UX: Accessibility
echo "── Accessibility UX ──"
grep -q 'aria-label' "$DIR/index.html" && check "Index: aria-labels present" "ok" || check "Index: aria-labels present" "missing"
for p in schedules quality lineage replay; do
  grep -q '<title>' "$DIR/$p.html" && check "$p: has page title" "ok" || check "$p: has page title" "missing"
done

# Summary
echo ""
echo "════════════════════════════"
echo "  ✅ Passed: $PASS"
echo "  ❌ Failed: $FAIL"
echo "════════════════════════════"
[ "$FAIL" -eq 0 ] && echo "🎉 All UI regression tests passed!" || exit 1
