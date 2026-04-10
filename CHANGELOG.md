# Changelog

All notable changes to Fuse are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

## [0.5.0] — 2026-04-10

### Added

**Docs & Community**
- Connector development guide: end-to-end tutorial with FilterExpr translation, SDK testing, MockConnector
- Dashboard user guide: 12 chart types, auto-detection, variables, drill-down, templates, save/share/export
- Performance tuning guide: pushdown tables, JOIN optimization, caching TTLs, server config, cost estimator
- Migration guide: OpenSearch Query DSL → Fuse SQL/PPL translation tables, dashboard migration
- API reference guide: all 19 endpoints with curl examples and request/response JSON
- CONTRIBUTING.md rewrite: dev setup, naming conventions, commit format, PR process, chart/connector contribution
- Docs-site: 5 new pages with organized navigation (User Guide, API, Developer sections)

### Tests
- 983 tests (up from 904 in v0.4.0), 0 failures

## [0.4.0] — 2026-04-10

### Added

**Connectors (3 new → 14 total)**
- MongoDB connector — BSON filter pushdown, projection, limit, connection pooling
- InfluxDB connector — InfluxDB 1.x (InfluxQL) and 2.x (Flux), auto-version detection
- ClickHouse connector — full SQL pushdown via HTTP interface, JSONEachRow streaming

**Docs & Community**
- Architecture doc: federated query execution model (SQL→parse→plan→fan-out→merge→response)
- Docs-site: architecture page, updated SQL/PPL reference, all 14 connectors
- OpenAPI spec v0.4.0: cursor pagination, trace reconstruction, saved queries, running queries (19 paths, 22 schemas)
- Demo video script v2: 8-scene walkthrough covering JOINs, UNION ALL, CTEs, trace, dashboards, pagination
- README: updated to 14 connectors, all Sprint 3 features, 900+ tests

### Tests
- 904 tests (up from 660 in v0.3.0), 0 failures

---

## [0.3.0] — 2026-04-10

### Added

**Connectors (7 new → 11 total)**
- DynamoDB connector — Scan/Query, full filter pushdown, projection
- PostgreSQL connector — full SQL pushdown via sqlx PgPool
- MySQL connector — full SQL pushdown via sqlx MySqlPool
- Elasticsearch connector — ES 7.x/8.x, API key + Basic auth, Query DSL pushdown
- Redis connector — SCAN, hash + string types
- CSV/JSON connector — auto-detect format, schema inference
- CloudWatch connector — CloudWatch Logs Insights, time range + filter pattern pushdown

**Cross-Datasource Query**
- Cross-datasource JOIN: hash join with build-side selection (smaller table as build)
- Semi-join (EXISTS) and anti-join (NOT EXISTS)
- Correlated subqueries: `WHERE col IN (SELECT ... FROM other_source)`
- Cross-datasource GROUP BY with federated re-aggregation
- UNION (deduplicated) vs UNION ALL
- Hash join optimization: build-side selection based on cost estimation

**Pagination & Sorting**
- Server-side cursor pagination (keyset-based): `page_size`, `cursor`, `next_cursor`
- Multi-column ORDER BY with mixed ASC/DESC
- OpenSearch `search_after` for deep pagination (>10k results)
- S3 Parquet row-group pagination with early limit termination

**Advanced Compute**
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD
- PERCENTILE / PERCENTILE_APPROX aggregation
- Computed columns (expressions in SELECT)
- CASE WHEN expressions
- Date/time functions: DATE_TRUNC, DATE_DIFF, NOW()
- String functions: UPPER, LOWER, SUBSTRING, TRIM, REGEXP
- Math functions: ROUND, CEIL, FLOOR, ABS, MOD
- Nested/JSON field access (dot notation) in OpenSearch + Elasticsearch

**Server**
- Trace reconstruction endpoint: `GET /api/fuse/trace/{trace_id}` — fan-out to all datasources
- Query cost estimator: pre-execution `estimated_rows` and `estimated_cost` per plan node

**Playground**
- Demo scenario selector: 6 pre-built cross-datasource queries
- Demo data seeding: 50 users, 200 logs, 200 S3 rows, 50 CloudWatch events

### Tests
- 660 tests (up from 586 in v0.2.0), 0 failures
- Cross-datasource integration test suite (JOIN, UNION ALL, correlated subquery, 3-source, GROUP BY)
- Pagination E2E tests (cursor roundtrip, offset, deep pages)
- Aggregation correctness tests (window, percentile, computed columns)
- New connector conformance framework + MockConnector
- Performance regression suite (p50=90ms, p95=148ms, no regression vs v0.2.0)

### Added

**Query Engine**
- HAVING clause pushdown in UNION ALL queries
- BETWEEN filter support in SQL→SubQuery translation
- IN/NOT IN and ILIKE filter pushdown
- COUNT(DISTINCT) pushdown end-to-end
- PPL `lookup` command for cross-datasource enrichment
- PPL `top` and `rare` commands
- PPL `eval` and `rename` commands
- Type coercion in cross-source UNION
- Global ORDER BY for UNION ALL results
- Query plan cache — skip re-parsing for repeated queries

**Server**
- Data provenance — `_datasource` column and `datasource_stats` in responses
- EXPLAIN ANALYZE — execution profiling with per-stage timing
- Query history + rate limiting (configurable per-IP)
- Saved queries — CRUD for named query templates
- Query cancellation — cancel running queries by ID
- Per-query timeout with configurable `timeout_ms`
- CSV result format via `result_format` field
- Parameterized queries — named parameter binding
- SELECT DISTINCT + OFFSET support
- Partial failure resilience in UNION ALL queries
- Enhanced query validation — table existence check
- Query stats endpoint — aggregated metrics from history
- Graceful shutdown — drain in-flight queries
- Prometheus `/metrics` endpoint
- Structured JSON logging (`FUSE_LOG_FORMAT=json`)

**Connectors**
- OpenSearch scroll pagination for large result sets
- S3 Hive partition pruning for Parquet files
- Prometheus range query — `start`/`end`/`step` in QueryRequest

**Playground UI**
- Visual execution plan tree viewer
- Data provenance display
- Query history tab
- Download CSV button
- GitHub and Docs links in header
- Tabbed docs site

**Infrastructure**
- GitHub Pages docs site (mdBook → gh-pages)
- GitHub Actions release workflow (v* tags → binary artifacts)
- ECS auto-scaling (1-3 tasks, 70% CPU target)
- `.dockerignore` for faster Docker builds
- Deployment & operations runbook (`docs/DEPLOYMENT.md`)

**OSD Plugin**
- Query bar with syntax highlighting
- Results table with sort, pagination, provenance

### Fixed
- AND/OR partial filter translation correctness
- String literal safety for all SQL clause parsers
- SQL source parsing and param binding correctness
- Removed `unwrap()` from all handler execution paths
- OpenSearch u64/usize type mismatch in scroll limit check

### Tests
- 586 tests (up from 347 in Sprint 1), 0 failures
- COUNT DISTINCT, HAVING, ILIKE, IN/NOT IN verification
- S3 partition pruning verification
- Execution plan visualization tests
- Load test + negative E2E suite (25 E2E, 50-query load)
- SDK MockConnector + smoke_test coverage

---

## [0.1.0] — 2026-04-09

### Added

**Core engine**
- `FederatedConnector` trait — uniform interface for all datasource connectors
- `ConnectorRegistry` — thread-safe runtime connector registration
- `SubQuery` — structured query representation with filter, projection, aggregation, sort, limit
- `ConnectorCapabilities` — per-connector push-down declaration
- `QueryCache` — TTL-based result cache (OpenSearch: 30s, S3: 5min, Prometheus: 1min)
- DataFusion federation planner via `datafusion-federation` 0.5
- PPL parser with multi-source fan-out (`source = ds1.t, ds2.t | ...`)
- SQL→SubQuery translation using `sqlparser-rs` 0.61
- Cross-type hash JOIN executor
- Result merger: union, global sort, dedup, schema alignment
- Cost-based push-down optimizer
- Spark/EMR delegation interface (stub)

**Connectors**
- `fuse-connector-opensearch` — SigV4 auth, filter/projection/aggregation/sort/limit push-down, scroll streaming, AOSS compatibility
- `fuse-connector-s3` — Parquet reader, S3 Select push-down
- `fuse-connector-prometheus` — PromQL translation, range and instant queries
- `fuse-connector-s3-o11y` — gzipped NDJSON log reader, schema discovery

**Server**
- 8 REST endpoints: `POST /query`, `POST /query/stream` (SSE), `POST /query/validate`, `POST /query/explain`, `GET /datasources`, `GET /datasources/:id/schemas`, `GET /datasources/:id/schemas/:table/fields`, `GET /health`
- `GET /api/fuse/alerts` + `POST /api/fuse/alerts/evaluate` — alert rule evaluation
- Embedded playground UI at `/`
- Multi-datasource federation: UNION ALL fan-out + cross-datasource JOIN
- PPL multi-source routing to UNION ALL executor

**Security & operations**
- RBAC + field-level security (`PolicyEngine`, `ResultFilter`)
- Connector protocol versioning (`ConnectorVersion`, `VersionNegotiator`)
- Alerting integration (`AlertEvaluator`, `NotificationDispatcher` — log, webhook, Slack)
- Materialized view registry

**SDK & tooling**
- `fuse-connector-sdk` — `MockConnector`, assertion helpers, smoke test harness
- `publish-sdk.sh` — crates.io publish script
- Multi-stage Dockerfile (cargo-chef layer caching, ~5 min builds)
- `docker-compose.yml` — OpenSearch + fuse-server + OSD with plugin
- `Dockerfile.osd` — OSD 2.17 with fuse-query plugin pre-installed
- CI: `.github/workflows/ci.yml` (build + test on every push)

**OSD plugin** (`osd-plugin/fuse-query/`)
- Query editor with SQL/PPL toggle
- Results table with column headers
- Datasource selector
- Health indicator
- Visual join builder
- Server-side proxy routes (reads `FUSE_ENGINE_URL` from env)

**Docs**
- `docs/guides/getting-started.md` — playground quick-start with copy-pasteable examples
- `docs/guides/writing-a-connector.md` — connector implementation guide
- `docs/api/openapi.yaml` — OpenAPI 3.1 spec
- `docs/rfcs/RFC-001-fuse-integration.md` — upstream OSD integration RFC
- `docs/community/forum-post.md` — OpenSearch forum post
- `docs/blog/introducing-fuse.md` — blog post draft

**Tests**
- 256 unit and integration tests, 0 failures
- Coverage: all public connector methods, SQL/PPL parsing, push-down pipeline, federation routing, REST API, alerting, security, caching

**Playground**
- Live at https://fuse.huanji.profile.aws.dev (Amazon VPN required)
- Two OpenSearch Serverless clusters (`cluster_a`, `cluster_b`) with demo data
- Full CI/CD: CodeCommit → CodeBuild → ECR → ECS Fargate → ALB

---

[0.5.0]: https://github.com/seraphjiang/fuse/releases/tag/v0.5.0
[0.4.0]: https://github.com/seraphjiang/fuse/releases/tag/v0.4.0
[0.3.0]: https://github.com/seraphjiang/fuse/releases/tag/v0.3.0
[0.2.0]: https://github.com/seraphjiang/fuse/releases/tag/v0.2.0
[0.1.0]: https://github.com/seraphjiang/fuse/releases/tag/v0.1.0
