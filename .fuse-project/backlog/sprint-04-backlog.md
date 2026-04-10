# Sprint 4 Backlog

**Sprint:** 4
**Start:** TBD
**Focus:** Cross-datasource analytics depth, log analysis primitives, search capabilities, observability correlation

## P0: Cross-Datasource Query Capabilities

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 400 | CTEs (WITH clause) | planner | todo | WITH errors AS (SELECT ... FROM os) SELECT * FROM errors JOIN ddb. Multi-step analysis pipelines |
| 401 | Time-windowed JOINs | planner | todo | JOIN ON key AND timestamp BETWEEN t1 AND t2. Critical for log correlation within time windows |
| 402 | HAVING clause on cross-source GROUP BY | planner | todo | Filter aggregated results after federated re-aggregation |
| 403 | Nested/JSON field access (dot notation) | general | todo | SELECT metadata.region, tags.env FROM opensearch. Carried from S3 #337 |
| 404 | DISTINCT / COUNT DISTINCT across sources | planner | todo | Deduplicated counts across federated UNION ALL. Cardinality analysis |
| 405 | FULL OUTER JOIN | planner | todo | Currently only Inner/Left/Semi/Anti. Show all logs + metrics matched or not |

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
| 421 | Cross-source trace reconstruction | explorer | todo | Given trace_id, auto-query all datasources, reconstruct full trace timeline |
| 422 | Anomaly detection primitives | planner | todo | Moving avg, stddev, z-score across time-bucketed data. Statistical outlier detection |
| 423 | Saved queries / virtual views | planner | todo | CREATE VIEW unified_logs AS SELECT ... UNION ALL ... Virtual tables over cross-source queries |

## P2: Connectors & Infrastructure (Carried from Sprint 3)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 322 | OFFSET pushdown to connectors | planner | todo | Carried from S3. Skip rows at source, not post-fetch |
| 323 | Pagination across UNION ALL (global cursor) | planner | todo | Carried from S3. Maintain per-source cursors, merge in order |
| 324 | Large result streaming with backpressure | explorer | todo | Carried from S3. SSE with flow control, client-driven page size |
| 325 | OpenSearch search_after for deep pagination | general | todo | Carried from S3. Replace from+size with search_after for >10k results |
| 326 | S3 connector: paginated Parquet reading | general | todo | Carried from S3. Read row groups incrementally, not full file |
| 337 | Nested field access (JSON dot notation) | general | todo | Carried from S3. Merged into #403 |

## P1: Visualization & Dashboard Platform

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 440 | Chart library integration (Apache ECharts) | explorer | todo | Apache 2.0, CSP-safe, zero deps, 62k+ stars. Embed in playground |
| 441 | Auto-visualization: detect column types → suggest chart | explorer | todo | timestamps→line, categories→bar, numbers→gauge. Smart defaults |
| 442 | Core chart types (8): line, bar, stacked bar, pie, area, scatter, table, histogram | explorer | todo | Standard analytics charts for log/metrics analysis |
| 443 | Log-specific charts (4): heatmap, timeline/Gantt, flame chart, Sankey | explorer | todo | Error density, trace spans, execution plan viz, request flow |
| 444 | Dashboard builder: grid layout, save/load JSON, time range picker | explorer | todo | Drag-and-drop panels, global time filter, auto-refresh |
| 445 | Dashboard templates: pre-built for error analysis, trace correlation, capacity | explorer | todo | One-click dashboards for common o11y use cases |
| 446 | Dashboard sharing: URL sharing, export PNG/PDF/CSV | explorer | todo | Share dashboards via link, export for reports |
| 447 | Variables/parameters: dropdown filters that update all panels | explorer | todo | Service picker, time range, environment selector |
| 448 | Drill-down: click chart element → filter to that value | explorer | todo | Click bar → filter all panels to that service/status |
| 449 | One-stop-shop: expand playground into /playground + /dashboards + /explore | infra | todo | Single URL, single auth, tabbed navigation |

## P1: New Connectors (DB-Engines Top 50)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 450 | MongoDB connector | general | todo | #5 globally. Document store, huge in log pipelines. Aggregation pipeline pushdown |
| 451 | InfluxDB connector | general | todo | #28. Time-series DB, critical for metrics/o11y. InfluxQL/Flux query pushdown |
| 452 | ClickHouse connector | general | todo | #30. Fast columnar analytics, popular for logs. Native SQL pushdown |

## P2: Docs & Community

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 430 | Update docs-site for Sprint 3+4 features | explorer | todo | Connectors, pagination, aggregation, cross-source docs |
| 431 | Update OpenAPI spec | explorer | todo | Cursor pagination params, new query features, CTE support |
| 432 | Demo video v2 (cross-datasource scenarios) | explorer | todo | Showcase JOIN/UNION/lookup/CTE across sources |
| 433 | Architecture doc: federated query execution model | explorer | todo | How Fuse plans, distributes, and merges cross-source queries |
