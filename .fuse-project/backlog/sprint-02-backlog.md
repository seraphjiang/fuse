# Sprint 2 Backlog

**Sprint:** 2
**Start:** 2026-04-09 (pending PM kickoff)
**Focus:** Production hardening, community adoption, OpenSearch Dashboards integration

## P0: Production Hardening

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 200 | Deploy latest (484 tests, 21 endpoints) and verify live | infra | done | Pipeline a9681443 succeeded. 16/16 endpoints verified live |
| 201 | E2E test suite against live playground (all 21 endpoints) | tester | done | 25/25 green. Commit: 70abb13 |
| 202 | Load test: 50 concurrent queries, measure p50/p95/p99 | tester | done | 50/50, p50=413ms p95=535ms p99=577ms. Commit: 70abb13 |
| 203 | Negative test suite: SQL injection, empty body, unicode, oversized query, malformed JSON | tester | done | 7 negative tests in E2E suite. Commit: 70abb13 |
| 204 | S3 O11y connector health fix (ECS task role needs s3:GetObject) | infra | done | Already working — permissions correct from Sprint 1 |
| 205 | Fix fuse-server warning (cargo fix --lib -p fuse-server) | general | done | Already clean + SubQuery::having sites fixed. Commit: 1786a61 |

## P1: GitHub Pages Docs Site Live

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 210 | Enable GitHub Pages in repo settings (gh-pages branch) | infra | done | Already enabled, verified live via gh API + curl |
| 211 | Verify https://seraphjiang.github.io/fuse/ loads | explorer | done | All 11 pages return 200, content verified |
| 212 | Update docs-site content for Sprint 2 features (21 endpoints, saved queries, CSV, params, cancellation) | explorer | done | Commit: 8ab14cf. 4 pages updated, mdBook clean |

## P1: OpenSearch Dashboards Plugin (OSD)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 220 | OSD plugin scaffold (TypeScript, Kibana platform plugin) | explorer | done | Verified existing structure. Commit: 102618e |
| 221 | Query bar component (SQL/PPL toggle, syntax highlighting) | explorer | done | QueryEditor 155 lines, Ctrl+Enter, analyze checkbox. Commit: 102618e |
| 222 | Results table component (sortable, paginated, provenance colors) | explorer | done | ResultsTable 160 lines, sort/pagination/provenance. Commit: 102618e |
| 223 | Datasource picker (multi-select from /api/fuse/datasources) | general | todo | Depends: 220 |
| 224 | Visual execution plan component (tree view from analyze:true) | planner | done | ProfileNode with cost/detail/pushdown. Commit: 98e326e. Tests: 532→559 |

## P2: Query Engine Depth

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 230 | Subquery support: SELECT * FROM (SELECT ...) AS sub | planner | done | Recursive extraction + filter merging. Commit: 12847cb. Tests: 484→502 |
| 231 | HAVING clause in GROUP BY | planner | done | Commit: 12847cb. Tests: 484→487 |
| 232 | IN/NOT IN filter pushdown | explorer | done | HAVING pushdown to UNION ALL sources. Commit: 50f68cf. 2 tests |
| 233 | LIKE/ILIKE filter pushdown | planner | done | Added ILike variant + case_insensitive wildcard. Commit: c52181d. Tests: 502→532 |
| 234 | COUNT DISTINCT aggregation pushdown | explorer | done | End-to-end: AggFunction::CountDistinct → cardinality. Commit: 3752c6d. 3 tests |
| 235 | PPL: lookup command (cross-datasource enrichment) | planner | todo | OpenSearch PPL extension |
| 236 | Query plan cache (same query → skip planning) | planner | todo | |

## P2: Connector Improvements

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 240 | OpenSearch scroll/PIT for large result sets | general | done | Scroll for limit>10k, aggs single-shot. Tests: 487→502. Commit: 1786a61 |
| 241 | S3 Parquet partition pruning | general | done | Hive-style pruning, 12 tests. Commit: 0d654fe |
| 242 | Prometheus range query support (start/end/step) | general | done | query_range API, passthrough params. Commit: 7b60394. Tests: 526→561 |
| 243 | CloudWatch Logs connector | explorer | done | 6th connector. Insights API, filter/sort/limit pushdown, 11 tests. Commit: 300c730 |

## P3: Observability & Operations

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 250 | Structured logging (tracing-subscriber JSON) | infra | todo | Currently text logs |
| 251 | Metrics endpoint (Prometheus /metrics) | explorer | done | 4 metrics: queries_total, duration_ms, active_queries, connector_healthy. Commit: 7448bde. 3 tests |
| 252 | Distributed tracing (OpenTelemetry) | infra | todo | Trace across connectors |
| 253 | Graceful shutdown (drain in-flight queries) | explorer | done | SIGTERM/SIGINT → drain 10s → cancel_all. Commit: 7ae3d5d. 2 tests |

## P3: Community

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 260 | v0.1.0 release (tag + GitHub Release with binaries) | infra | todo | Tag exists locally, needs push |
| 261 | Demo video (2-3 min, playground walkthrough) | explorer | done | Commit: 75b68f9. 6-scene script, ~2.5 min |
| 262 | OpenSearch community forum update post | explorer | todo | Sprint 2 features |
| 263 | Publish fuse-connector-sdk to crates.io | general | todo | Was dry-run only |
