# Sprint 4 Backlog

**Sprint:** 4
**Start:** 2026-04-10T05:30Z
**Focus:** Cross-datasource analytics depth, visualization platform, new connectors, docs catch-up

## P0: Cross-Datasource Query Capabilities

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 400 | CTEs (WITH clause) | planner | done | Parse → execute inner → MemoryConnector → main SELECT. Cross-datasource. Commit: f36e421 |
| 401 | Time-windowed JOINs | planner | done | Hash join + post-join time-window filter. BETWEEN + INTERVAL support. Tests: 537→543 |
| 402 | HAVING clause on cross-source GROUP BY | planner | done | Post-reaggregation HAVING filter. Commit: 08d4b5f. Tests: 543→551 |
| 403 | Nested/JSON field access (dot notation) | general | done | get_nested() across OS, ES, MongoDB, ClickHouse. Dot-path traversal |
| 404 | DISTINCT / COUNT DISTINCT across sources | planner | todo | Deduplicated counts across federated UNION ALL. Cardinality analysis |
| 405 | FULL OUTER JOIN | planner | done | Tracks matched build rows, emits unmatched with NULLs. Commit: 2984515. Tests: 551→572 |

## P1: Compute Depth — Time Series & Aggregation

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 410 | Temporal bucketing (time_bucket, date_trunc for grouping) | planner | todo | time_bucket('5m', timestamp) for time-series aggregation across sources |
| 411 | Top-N / Bottom-N with pushdown | planner | todo | ORDER BY count DESC LIMIT 10 pushed to connectors. Top error-producing services |
| 412 | Approximate aggregations (HyperLogLog, t-digest) | planner | todo | Pushdown to OpenSearch native approximate aggs for COUNT DISTINCT, percentiles |
| 413 | Recursive CTEs | planner | todo | Trace dependency chains: find all services in call graph from service X |

## P1: Search & Observability

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 420 | Full-text search syntax | planner | todo | WHERE message CONTAINS 'OutOfMemory' or MATCH(). Bridge SQL + OpenSearch full-text |
| 421 | Cross-source trace reconstruction | explorer | done | GET /api/fuse/trace/{trace_id}, parallel fan-out, unified timeline. Commit: 3f5b498 |
| 422 | Anomaly detection primitives | planner | todo | Moving avg, stddev, z-score across time-bucketed data. Statistical outlier detection |
| 423 | Saved queries / virtual views | planner | todo | CREATE VIEW unified_logs AS SELECT ... UNION ALL ... Virtual tables over cross-source queries |

## P2: Connectors & Infrastructure (Carried from Sprint 3)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 322 | OFFSET pushdown to connectors | planner | todo | Carried from S3. Skip rows at source, not post-fetch |
| 323 | Pagination across UNION ALL (global cursor) | planner | todo | Carried from S3. Maintain per-source cursors, merge in order |
| 324 | Large result streaming with backpressure | explorer | done | Bounded channel (4 slots), client batch_size, row buffering. 8 new tests |
| 325 | OpenSearch search_after for deep pagination | general | done | Carried from S3. Replace from+size with search_after for >10k results |
| 326 | S3 connector: paginated Parquet reading | general | done | Carried from S3. Read row groups incrementally, not full file |
| 337 | Nested field access (JSON dot notation) | general | done | Carried from S3. Merged into #403 |

## P1: Visualization & Dashboard Platform

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 440 | Chart library integration (Apache ECharts) | frontend | done | ECharts 5 via CDN, dark theme, responsive. Commit: fb92edc |
| 441 | Auto-visualization: detect column types → suggest chart | frontend | done | time→line, category→bar, numbers→pie/scatter. Commit: fb92edc |
| 442 | Core chart types (8): line, bar, stacked bar, pie, area, scatter, table, histogram | frontend | done | All 8 types + selector dropdown + auto-bins. Commit: 7775c88 |
| 443 | Log-specific charts (4): heatmap, timeline/Gantt, flame chart, Sankey | frontend | done | All 4 types in playground + dashboard, auto-detect columns |
| 444 | Dashboard builder: grid layout, save/load JSON, time range picker | frontend | done | 12-col grid, panel CRUD, localStorage + JSON export, auto-refresh. Commit: 3f5b498 |
| 445 | Dashboard templates: pre-built for error analysis, trace correlation, capacity | frontend | done | 3 templates (16 panels), one-click load via Templates button |
| 446 | Dashboard sharing: URL sharing, export PNG/PDF/CSV | frontend | done | Base64 URL sharing, PNG 2x retina, CSV re-query, print/PDF. Commit: 5a41f6b |
| 447 | Variables/parameters: dropdown filters that update all panels | explorer | todo | Service picker, time range, environment selector |
| 448 | Drill-down: click chart element → filter to that value | explorer | todo | Click bar → filter all panels to that service/status |
| 449 | One-stop-shop: expand playground into /playground + /dashboards + /explore | infra | todo | Single URL, single auth, tabbed navigation |

## P1: New Connectors (DB-Engines Top 50)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 450 | MongoDB connector | general | done | BSON filter pushdown, projection, limit, schema from doc sample. Commit: 2649e4b |
| 451 | InfluxDB connector | general | done | v1 InfluxQL + v2 Flux, version detection, CSV parsing. Commit: 2649e4b |
| 452 | ClickHouse connector | general | done | HTTP JSONEachRow, full SQL pushdown, uniq(). Commit: 2649e4b |

## P2: Docs & Community

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 430 | Update docs-site for Sprint 3+4 features | docs | done | Architecture page, SQL/PPL reference, all features documented. Commit: 01d1d49 |
| 431 | Update OpenAPI spec | docs | done | v0.4.0: 19 paths, 22 schemas. Trace, saved queries, cursor pagination. Commit: 9341ece |
| 432 | Demo video v2 (cross-datasource scenarios) | docs | done | 8-scene script, ~4 min, exact runnable queries. Commit: 0e4a349 |
| 433 | Architecture doc: federated query execution model | docs | done | Full query flow, connector trait, pushdown table, crate map. Commit: aa74eef |
