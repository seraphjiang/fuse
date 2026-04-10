# Sprint 3 Backlog

**Sprint:** 3
**Start:** 2026-04-10
**Focus:** New datasources, cross-datasource demo scenarios, pagination/sorting/limit depth, advanced aggregation & compute

## P0: New Datasources (Demo-Ready)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 300 | DynamoDB connector | general | done | Scan/Query, full filter pushdown, 18 tests. Commit: a89c037 |
| 301 | PostgreSQL/MySQL connector (JDBC-style) | general | todo | SQL passthrough, schema discovery, connection pooling |
| 302 | Elasticsearch connector (distinct from OpenSearch) | general | todo | REST API, version-aware (ES 7.x/8.x), Query DSL pushdown |
| 303 | Redis connector (key-value + sorted sets) | explorer | done | 8th connector. SCAN, hash+string types, 8 tests. Commit: afd98f8 |
| 304 | CSV/JSON file connector (local or S3) | explorer | done | 7th connector. Auto-detect format, schema inference, 13 tests. Commit: c97bfea |
| 305 | Deploy new connectors to playground with sample data | infra | todo | Depends: 300-304. Add to fuse.toml, seed demo data |

## P0: Cross-Datasource Demo Scenarios

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 310 | Demo: JOIN OpenSearch logs + DynamoDB user profiles | planner | done | 5 demo queries: JOIN, GROUP BY, PPL lookup, correlated subquery, anti-join. Commit: 99c8732 |
| 311 | Demo: UNION ALL across OpenSearch + CloudWatch + S3 logs | planner | done | 3-source UNION ALL, trace correlation, error agg. Commit: 1f8498c. 2 tests |
| 312 | Demo: Aggregate Prometheus metrics + OpenSearch events | planner | done | JOIN + UNION ALL correlating metrics + errors. Commit: 802d3f5 |
| 313 | Demo: PPL lookup across datasources (logs → user enrichment) | planner | done | 4 lookup demo queries. Commit: dfca414 |
| 314 | Demo data seeding (consistent timestamps, correlated events) | infra | done | 50 users, 200 logs, 200 S3 rows, 50 CW events. Shared user_ids + trace_ids |
| 315 | Playground UI: demo scenario selector (pre-built queries) | explorer | done | 6 scenarios, auto-execute on click. Commit: 6d85d72 |

## P1: Pagination, Sorting & Limit Depth

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 320 | Server-side cursor pagination (keyset-based) | planner | done | cursor + page_size on request, next_cursor in response. Commit: 0965c50. 3 tests |
| 321 | Multi-column ORDER BY with mixed ASC/DESC | planner | done | Vec<(String, bool)> parsing. Commit: 39ab5c5 |
| 322 | OFFSET pushdown to connectors | planner | todo | Skip rows at source, not post-fetch |
| 323 | Pagination across UNION ALL (global cursor) | planner | todo | Maintain per-source cursors, merge in order |
| 324 | Large result streaming with backpressure | explorer | todo | SSE with flow control, client-driven page size |
| 325 | OpenSearch search_after for deep pagination | general | todo | Replace from+size with search_after for >10k results |
| 326 | S3 connector: paginated Parquet reading | general | todo | Read row groups incrementally, not full file |

## P1: Advanced Aggregation & Compute

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 330 | Window functions (ROW_NUMBER, RANK, LAG, LEAD) | planner | todo | Post-fetch compute, DataFusion integration |
| 331 | PERCENTILE / PERCENTILE_APPROX aggregation | planner | todo | OpenSearch → percentiles agg, others → post-compute |
| 332 | Computed columns (expressions in SELECT) | planner | done | Pass-through SQL expressions. Commit: 0631c5b |
| 333 | CASE WHEN expressions | planner | done | Via passthrough + partial WHERE translation. Commit: f14f517. 3 tests |
| 334 | Date/time functions (DATE_TRUNC, DATE_DIFF, NOW()) | planner | done | Via computed column passthrough. Commit: f14f517. 3 tests |
| 335 | String functions (UPPER, LOWER, SUBSTRING, TRIM, REGEXP) | planner | done | Via #332 passthrough. Already tested |
| 336 | Math functions (ROUND, CEIL, FLOOR, ABS, MOD) | planner | done | Via #332 passthrough. Already tested |
| 337 | Nested field access (JSON dot notation) | general | todo | SELECT metadata.region FROM ... for nested OpenSearch docs |
| 338 | UNION (deduplicated) vs UNION ALL | planner | done | Plain UNION now deduplicates. Commit: e61512e. 2 tests |

## P2: Cross-Datasource Query Improvements

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 340 | Hash join optimization (build side selection) | planner | done | Smaller table as build side, profile annotated. Commit: ee30ee7 |
| 341 | Semi-join / anti-join (EXISTS, NOT EXISTS) | planner | done | Semi + Anti JoinType variants. Commit: 4fed418. 2 tests |
| 342 | Correlated subqueries | planner | done | WHERE col IN (SELECT ... FROM other_source). Commit: c7382e6. Tests: 487→495 |
| 343 | Cross-datasource GROUP BY (federated aggregation) | planner | done | reaggregate_batches for post-merge GROUP BY. COUNT/SUM merge correctly |
| 344 | Query cost estimator (pre-execution) | planner | done | estimated_rows + estimated_cost on scan/parent nodes. Commit: 7d25e71. Tests: 470→477 |

## P2: Testing & Quality

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 350 | Cross-datasource integration test suite | tester | todo | JOIN, UNION, lookup across real connectors |
| 351 | Pagination E2E tests (cursor, offset, deep pages) | tester | done | 5 cursor tests, roundtrip verified. Commit: bdd2cff. Tests: 599→609 |
| 352 | Aggregation correctness tests (window, percentile, computed) | tester | done | 10 tests verifying #321/#332-336. Commit: 0c0082e. Tests: 589→599 |
| 353 | New connector E2E tests (DynamoDB, PostgreSQL, etc.) | tester | done | Conformance framework + MockConnector. Commit: 2306031. 5 tests |
| 354 | Performance regression suite (compare vs Sprint 2 baseline) | tester | done | p50=90ms p95=148ms, no regression. Commit: 2306031 |

## P3: Playground & Docs

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 360 | Update docs-site for Sprint 3 features | explorer | todo | New connectors, pagination, aggregation docs |
| 361 | Update OpenAPI spec for new endpoints/params | explorer | todo | Cursor pagination params, new query features |
| 362 | Deploy Sprint 3 to playground | infra | todo | Full feature set live |
| 363 | Demo video v2 (cross-datasource scenarios) | explorer | todo | Showcase JOIN/UNION/lookup across sources |
