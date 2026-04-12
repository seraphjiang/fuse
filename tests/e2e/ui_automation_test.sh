#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fuse Playground UI Automation E2E Tests
# Validates all 18 playground pages: HTTP status, title, key DOM elements, nav.
#
# Usage:
#   ./tests/e2e/ui_automation_test.sh [BASE_URL]

set -euo pipefail

BASE="${1:-https://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com}"
PASS=0
FAIL=0
RESULTS=()
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# ── Helpers ──

run_test() {
    local name="$1"; shift
    local start=$(date +%s%N)
    if "$@" 2>"$TMPDIR/err"; then
        local ms=$(( ($(date +%s%N) - start) / 1000000 ))
        RESULTS+=("✅ PASS  ${ms}ms  $name")
        PASS=$((PASS + 1))
    else
        local ms=$(( ($(date +%s%N) - start) / 1000000 ))
        local err=$(cat "$TMPDIR/err" | tail -1)
        RESULTS+=("❌ FAIL  ${ms}ms  $name  ($err)")
        FAIL=$((FAIL + 1))
    fi
}

http_status() { curl -sko /dev/null --max-time 10 -w "%{http_code}" "$@" 2>/dev/null; }

fetch_page() { curl -sk --max-time 10 "$BASE/$1" 2>/dev/null; }

# Assert page returns 200, contains expected <title>, and has key DOM element ids.
# Usage: assert_page <path> <expected_title_substr> <id1> [id2 ...]
assert_page() {
    local path="$1"; shift
    local title="$1"; shift
    local ids=("$@")
    local file="$TMPDIR/$(echo "$path" | tr '/' '_')"

    fetch_page "$path" > "$file"

    # HTTP 200
    local s=$(http_status "$BASE/$path")
    [ "$s" = "200" ] || { echo "HTTP $s for $path" >&2; return 1; }

    # Title
    grep -qi "$title" "$file" || { echo "title '$title' not found in $path" >&2; return 1; }

    # Key DOM ids
    for eid in "${ids[@]}"; do
        grep -q "id=\"$eid\"" "$file" || { echo "id='$eid' not found in $path" >&2; return 1; }
    done
}

# Assert page has nav-tabs navigation
assert_nav() {
    local path="$1"
    local file="$TMPDIR/$(echo "$path" | tr '/' '_')"
    grep -q 'nav-tab' "$file" || { echo "nav-tabs missing in $path" >&2; return 1; }
}

# ── Page Tests ──

# 1. Index (Query Playground)
test_index() { assert_page "index.html" "Fuse Query Playground" "onboarding"; }
test_index_nav() { assert_nav "index.html"; }

# 2. Dashboard
test_dashboard() { assert_page "dashboard.html" "Fuse Dashboard" "dashboard-list" "time-range" "refresh-interval"; }
test_dashboard_nav() { assert_nav "dashboard.html"; }

# 3. Explore
test_explore() { assert_page "explore.html" "Fuse Explore" "editor" "format"; }
test_explore_nav() { assert_nav "explore.html"; }

# 4. Settings
test_settings() { assert_page "settings.html" "Fuse Settings" "ds-list" "ds-form"; }
test_settings_nav() { assert_nav "settings.html"; }

# 5. Help
test_help() { assert_page "help.html" "Fuse Help" "s-sql" "s-ppl" "s-shortcuts"; }
test_help_nav() { assert_nav "help.html"; }

# 6. Admin
test_admin() { assert_page "admin.html" "Fuse Admin" "s-tenants" "tenant-list"; }
test_admin_nav() { assert_nav "admin.html"; }

# 7. Changelog
test_changelog() { assert_page "changelog.html" "Fuse Changelog" "content"; }
test_changelog_nav() { assert_nav "changelog.html"; }

# 8. Feedback Widget (embeddable, no <title>)
test_feedback_widget() {
    local s=$(http_status "$BASE/feedback-widget.html")
    [ "$s" = "200" ] || { echo "HTTP $s" >&2; return 1; }
    local file="$TMPDIR/feedback-widget.html"
    fetch_page "feedback-widget.html" > "$file"
    grep -q 'id="fb-btn"' "$file" || { echo "fb-btn missing" >&2; return 1; }
    grep -q 'id="fb-panel"' "$file" || { echo "fb-panel missing" >&2; return 1; }
    grep -q 'id="fb-desc"' "$file" || { echo "fb-desc missing" >&2; return 1; }
}

# 9. Views
test_views() { assert_page "views.html" "Materialized Views" "view-list" "create-modal" "mv-name" "mv-query"; }
test_views_nav() { assert_nav "views.html"; }

# 10. Plugins
test_plugins() { assert_page "plugins.html" "Plugins" "upload-area" "drop-zone" "plugin-list"; }
test_plugins_nav() { assert_nav "plugins.html"; }

# 11. Terminal
test_terminal() { assert_page "terminal.html" "Terminal" "terminal" "cmd-input" "autocomplete"; }
test_terminal_nav() { assert_nav "terminal.html"; }

# 12. Alerts
test_alerts() { assert_page "alerts.html" "Alerts" "stat-rules" "stat-firing" "active-list"; }
test_alerts_nav() { assert_nav "alerts.html"; }

# 13. Federation
test_federation() { assert_page "federation.html" "Federation" "stat-instances" "stat-datasources" "topo-canvas" "instance-list"; }
test_federation_nav() { assert_nav "federation.html"; }

# 14. Schedules
test_schedules() { assert_page "schedules.html" "Scheduled Queries" "sched-body" "modal"; }
test_schedules_nav() { assert_nav "schedules.html"; }

# 15. Quality
test_quality() { assert_page "quality.html" "Data Quality" "summary" "rules-body"; }
test_quality_nav() { assert_nav "quality.html"; }

# 16. Lineage
test_lineage() { assert_page "lineage.html" "Query Lineage" "lineage-graph" "format"; }
test_lineage_nav() { assert_nav "lineage.html"; }

# 17. Replay
test_replay() { assert_page "replay.html" "Query Replay" "summary" "s-total"; }
test_replay_nav() { assert_nav "replay.html"; }

# 18. Status
test_status() { assert_page "status.html" "Fuse Status" "last-update" "summary-grid" "sys-status" "sys-version"; }
test_status_nav() { assert_nav "status.html"; }

# ── Cross-page: nav link consistency ──

test_nav_links() {
    # Every page with nav-tabs should link to at least index.html and explore.html
    local fail=0
    for page in index dashboard explore settings help admin changelog views plugins terminal alerts federation schedules quality lineage replay status; do
        local file="$TMPDIR/${page}.html"
        fetch_page "${page}.html" > "$file"
        if grep -q 'nav-tab' "$file"; then
            grep -q 'index.html\|href="/"' "$file" || { echo "${page}.html missing link to index" >&2; fail=1; }
        fi
    done
    [ "$fail" -eq 0 ]
}

# ── Cross-page: no broken static assets ──

test_no_500_pages() {
    local fail=0
    for page in index dashboard explore settings help admin changelog feedback-widget views plugins terminal alerts federation schedules quality lineage replay status; do
        local s=$(http_status "$BASE/${page}.html")
        if [ "${s:0:1}" = "5" ]; then
            echo "${page}.html returned $s" >&2
            fail=1
        fi
    done
    [ "$fail" -eq 0 ]
}

# ── 404 for non-existent page ──

test_404_missing_page() {
    local s=$(http_status "$BASE/nonexistent-page-xyz.html")
    [ "$s" = "404" ] || [ "$s" = "200" ]  # Some servers serve index for unknown routes
}

# ── Run ──

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Fuse Playground UI Automation E2E Tests          ║"
echo "║  Target: $BASE"
echo "║  Time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "╚═══════════════════════════════════════════════════╝"
echo ""

# Page content + DOM validation (18 pages)
run_test "01 Index: serves, title, onboarding"          test_index
run_test "01 Index: nav-tabs present"                   test_index_nav
run_test "02 Dashboard: serves, title, DOM"             test_dashboard
run_test "02 Dashboard: nav-tabs present"               test_dashboard_nav
run_test "03 Explore: serves, title, editor"            test_explore
run_test "03 Explore: nav-tabs present"                 test_explore_nav
run_test "04 Settings: serves, title, ds-form"          test_settings
run_test "04 Settings: nav-tabs present"                test_settings_nav
run_test "05 Help: serves, title, sections"             test_help
run_test "05 Help: nav-tabs present"                    test_help_nav
run_test "06 Admin: serves, title, tenants"             test_admin
run_test "06 Admin: nav-tabs present"                   test_admin_nav
run_test "07 Changelog: serves, title, content"         test_changelog
run_test "07 Changelog: nav-tabs present"               test_changelog_nav
run_test "08 Feedback Widget: serves, DOM elements"     test_feedback_widget
run_test "09 Views: serves, title, create modal"        test_views
run_test "09 Views: nav-tabs present"                   test_views_nav
run_test "10 Plugins: serves, title, upload area"       test_plugins
run_test "10 Plugins: nav-tabs present"                 test_plugins_nav
run_test "11 Terminal: serves, title, cmd-input"        test_terminal
run_test "11 Terminal: nav-tabs present"                test_terminal_nav
run_test "12 Alerts: serves, title, stats"              test_alerts
run_test "12 Alerts: nav-tabs present"                  test_alerts_nav
run_test "13 Federation: serves, title, topology"       test_federation
run_test "13 Federation: nav-tabs present"              test_federation_nav
run_test "14 Schedules: serves, title"                  test_schedules
run_test "14 Schedules: nav-tabs present"               test_schedules_nav
run_test "15 Quality: serves, title"                    test_quality
run_test "15 Quality: nav-tabs present"                 test_quality_nav
run_test "16 Lineage: serves, title"                    test_lineage
run_test "16 Lineage: nav-tabs present"                 test_lineage_nav
run_test "17 Replay: serves, title"                     test_replay
run_test "17 Replay: nav-tabs present"                  test_replay_nav
run_test "18 Status: serves, title, sys info"           test_status
run_test "18 Status: nav-tabs present"                  test_status_nav

# Cross-page checks
run_test "Nav links: all pages link to index"           test_nav_links
run_test "No 5xx on any page"                           test_no_500_pages
run_test "Missing page returns 404 or fallback"         test_404_missing_page

# ── Summary ──

echo ""
echo "═══════════════════════════════════════════════════"
echo "  RESULTS"
echo "═══════════════════════════════════════════════════"
for r in "${RESULTS[@]}"; do
    echo "  $r"
done
echo ""
echo "  Total: $((PASS + FAIL))  |  Pass: $PASS  |  Fail: $FAIL"
echo "═══════════════════════════════════════════════════"

[ "$FAIL" -eq 0 ]
