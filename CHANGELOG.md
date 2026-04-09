# Changelog

All notable changes to Fuse are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased] — Sprint 2

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
- 582 tests (up from 347 in Sprint 1), 0 failures
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

## [Unreleased]

- End-to-end playground query verification (#060)
- Federated demo data seeding (#073)
- Cache test coverage (#090)

[0.1.0]: https://github.com/seraphjiang/fuse/releases/tag/v0.1.0
