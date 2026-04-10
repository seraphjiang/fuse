# Roadmap

Fuse is actively developed. This page tracks what's shipped, what's in progress, and what's planned.

Last updated: April 2026

## ✅ Shipped

### v0.5.0 — Production Hardening (April 2026)

- API key authentication with viewer/editor/admin roles
- Rate limiting (global + per-IP)
- Query timeout with per-query override
- Query cancellation by ID
- Result caching with per-connector TTL
- Saved queries / virtual views
- Recursive CTEs
- RIGHT JOIN
- Top-N pushdown to connectors
- Anomaly detection primitives (moving avg, stddev, z-score)
- Parallel connector health checks
- UNION ALL cursor pagination (global cursor)
- OSD plugin packaging
- Full documentation suite (architecture, API, connector dev, dashboard, performance, migration, security)
- 980+ tests, load tested at 100 concurrent / p99 <100ms

### v0.4.0 — Visualization Platform (April 2026)

- 3 new connectors: MongoDB, InfluxDB, ClickHouse (14 total)
- Dashboard platform with 12 chart types (line, bar, stacked bar, pie, area, scatter, histogram, table, heatmap, timeline, flame chart, Sankey)
- Auto-visualization (detect column types → suggest chart)
- Dashboard templates (Error Analysis, Trace Correlation, Capacity)
- Variables, drill-down, auto-refresh, time range picker
- Dashboard save/load/share/export (PNG, CSV, PDF)
- Architecture documentation

### v0.3.0 — Cross-Datasource Analytics (April 2026)

- 7 new connectors: DynamoDB, PostgreSQL, MySQL, Elasticsearch, Redis, CSV/JSON, CloudWatch (11 total)
- Cross-datasource JOINs with build-side selection
- Semi-join (EXISTS) and anti-join (NOT EXISTS)
- Correlated subqueries
- Cross-datasource GROUP BY with federated re-aggregation
- UNION (deduplicated) vs UNION ALL
- Server-side cursor pagination
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- CASE WHEN, computed columns, date/time/string/math functions
- Cost estimator with pre-execution estimates
- Trace reconstruction endpoint
- Demo scenario selector in playground

### v0.2.0 — Query Engine Depth (April 2026)

- PPL lookup, top, rare, eval, rename commands
- HAVING, BETWEEN, IN/NOT IN, ILIKE pushdown
- Data provenance (_datasource column)
- EXPLAIN ANALYZE with execution profiling
- Query history, saved queries, query cancellation
- Rate limiting, CSV export, parameterized queries
- Partial failure resilience in UNION ALL
- OpenSearch scroll pagination, S3 Hive partition pruning

### v0.1.0 — Foundation (April 2026)

- FederatedConnector trait and connector registry
- 4 connectors: OpenSearch, S3 Parquet, S3 O11y, Prometheus
- SQL and PPL parsing with DataFusion federation
- Cross-datasource hash JOIN and UNION ALL
- Cost-based pushdown optimizer
- REST API (8 endpoints), embedded playground UI
- Query cache, alerting, materialized views
- OSD plugin with query editor and results table

## 🔨 In Progress — Sprint 6

### Security Fixes
- InfluxDB injection vulnerability fix (quote escaping in InfluxQL/Flux)
- Prometheus injection vulnerability fix (PromQL label value escaping)

### New Connectors
- Amazon Redshift (SQL pushdown, IAM auth)
- DuckDB (in-process analytics, Arrow-native)
- SQLite (file-based, embedded)

### AI Integration
- Natural language to SQL (LLM-powered, schema-aware)
- Auto-suggest queries based on datasource schema
- Intelligent query optimization from history analysis

### Multi-Tenancy
- Per-tenant datasource visibility and query isolation
- Query governor (max rows, max time, max memory per tenant)
- Audit logging (identity, query, duration, result count)

## 🗺️ Planned

### Ecosystem & Integration
- **Python SDK** — `pip install fuse-client` with `query()`, `stream()`, `trace()` methods
- **Jupyter notebook integration** — `%fuse` magic command with DataFrame output
- **Grafana datasource plugin** — use Fuse as a Grafana datasource

### Advanced Connectors
- Apache Kafka (streaming, consume topics by key/timestamp)
- Amazon Timestream (time-series, InfluxQL-compatible)
- Snowflake (OAuth/key-pair auth, SQL pushdown)

### Quality & Operations
- Chaos testing (connector failures, network partitions)
- Performance regression CI (automated benchmarks per commit)
- Accessibility audit (WCAG 2.1 AA)
- Connector integration tests against live services

### Approximate Aggregations
- HyperLogLog for COUNT DISTINCT pushdown to OpenSearch
- t-digest for percentile pushdown

## 💡 Community Requests

Have a feature request? [Open a GitHub issue](https://github.com/seraphjiang/fuse/issues) or contribute directly — see [CONTRIBUTING.md](https://github.com/seraphjiang/fuse/blob/main/CONTRIBUTING.md).

Most-requested:
- Snowflake connector
- Grafana integration
- Natural language queries
- Write-back support (INSERT INTO from federated SELECT)
