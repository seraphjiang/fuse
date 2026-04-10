# Fuse v0.5.0: Dashboards, 14 Connectors, and Production Hardening

*April 2026 · Fuse Team*

We're excited to announce Fuse v0.5.0 — the biggest release yet. This sprint focused on three themes: a full visualization platform, production hardening, and comprehensive documentation for community contributors.

## What's New

### 14 Connectors

Fuse now connects to 14 datasource types. Sprint 4 added MongoDB, InfluxDB, and ClickHouse alongside the existing OpenSearch, Elasticsearch, PostgreSQL, MySQL, DynamoDB, S3 (Parquet), S3 O11y (NDJSON), Prometheus, CloudWatch, Redis, and CSV/JSON.

Every connector supports the same `SubQuery` interface — write SQL or PPL, and Fuse translates to the native query language automatically.

**Try it:**
```bash
# List all connected datasources
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources
```

*[Screenshot: Playground datasource selector showing all 14 connector types with health indicators]*

### Visualization Platform

The new `/dashboard` page is a full analytics platform built on Apache ECharts. Create panels, pick chart types, and build dashboards over federated queries.

**12 chart types:**
- Standard: line, bar, stacked bar, pie, area, scatter, histogram, table
- Observability: heatmap, timeline/Gantt, flame chart, Sankey

Fuse auto-detects the best chart type from your data — timestamps become line charts, categories become bars, proportions become pies.

*[Screenshot: Error Analysis dashboard with 5 panels — bar chart of errors by service, pie chart of status codes, cross-cluster error timeline, error message table, and service×status heatmap]*

**Try it:**
```bash
# Open the dashboard
open https://fuse.huanji.profile.aws.dev/dashboard
# Click "Templates" → "Error Analysis" for a pre-built dashboard
```

**Key features:**
- **Variables** — dropdown filters that update all panels (e.g., service picker)
- **Drill-down** — click a bar to filter the entire dashboard to that value
- **Auto-refresh** — 10s, 30s, 1m, 5m intervals with live pulse indicator
- **Time range** — global time window (5m to 7d) applied to all panels
- **Save/share/export** — local storage, shareable URLs, PNG/CSV/PDF export
- **3 templates** — Error Analysis, Trace Correlation, Capacity & Performance

*[Screenshot: Trace Correlation dashboard showing Sankey diagram of service-to-service request flow across clusters]*

### Cross-Datasource Queries

The query engine now supports the full spectrum of federated operations:

**JOIN OpenSearch logs with DynamoDB user profiles:**
```sql
SELECT l.service, l.status, u.name, u.team
FROM prod.application_logs l
JOIN dynamodb.users u ON l.user_id = u.user_id
WHERE l.status >= 500
```

**UNION ALL across 3 sources with provenance:**
```sql
SELECT _datasource, service, message
FROM prod.application_logs
UNION ALL SELECT _datasource, service, message
FROM cloudwatch.lambda_logs
UNION ALL SELECT _datasource, service, message
FROM s3_o11y.logs
ORDER BY timestamp DESC LIMIT 50
```

**CTE for multi-step analysis:**
```sql
WITH heavy_users AS (
    SELECT user_id, count(*) as errors
    FROM prod.application_logs WHERE status >= 500
    GROUP BY user_id HAVING count(*) > 3
)
SELECT u.name, u.team, h.errors
FROM heavy_users h JOIN dynamodb.users u ON h.user_id = u.user_id
```

**Trace reconstruction across all datasources:**
```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/trace/trace-001
```

*[Screenshot: Playground showing a cross-datasource JOIN result with OpenSearch log columns alongside DynamoDB user profile columns, _datasource provenance bar at bottom]*

### Production Hardening

**API key authentication** — protect your Fuse instance with `x-api-key` header or `Authorization: Bearer` token. Keys are configured in `fuse.toml` with viewer/editor/admin roles.

**Rate limiting** — configurable per-IP and global request limits. Returns 429 when exceeded.

**Query timeout** — per-query `timeout_ms` with server-wide default. Long-running queries are cancelled automatically.

**Query cancellation** — cancel any running query by ID via `DELETE /api/fuse/query/{id}/cancel`.

**Partial failure resilience** — UNION ALL queries continue when one datasource fails, returning partial results with error details.

### 980+ Tests

The test suite grew from 586 (v0.2.0) to 983 tests across unit, integration, E2E, and performance regression suites. Zero failures.

## Documentation

This release includes a complete documentation overhaul:

- **[Architecture](./architecture.md)** — how federated query execution works, from SQL parse to merged response
- **[Connector Development Guide](./connector-development.md)** — build your own connector in 30 minutes
- **[Dashboard Guide](./dashboard-guide.md)** — panels, charts, variables, drill-down, templates
- **[Performance Tuning](./performance-tuning.md)** — pushdown optimization, caching, server config
- **[Migration Guide](./migration-guide.md)** — move from direct OpenSearch queries to Fuse
- **[API Reference](./api-reference-guide.md)** — every endpoint with curl examples
- **[Contributing](./contributing.md)** — dev setup, code style, PR process

## Try It

**Live playground:** [https://fuse.huanji.profile.aws.dev](https://fuse.huanji.profile.aws.dev) (Amazon VPN)

**Dashboards:** [https://fuse.huanji.profile.aws.dev/dashboard](https://fuse.huanji.profile.aws.dev/dashboard)

**From source:**
```bash
git clone https://github.com/seraphjiang/fuse
cd fuse
cargo build --release
./target/release/fuse-server --config fuse.toml
# Open http://localhost:9400
```

## What's Next

Sprint 6 planning is underway. On the roadmap:
- Recursive CTEs for dependency chain tracing
- Approximate aggregations (HyperLogLog, t-digest) with OpenSearch pushdown
- Saved queries as virtual views (`CREATE VIEW`)
- New connectors: Amazon Redshift, DuckDB, SQLite
- OSD plugin packaging for npm

We welcome contributions — see [CONTRIBUTING.md](https://github.com/seraphjiang/fuse/blob/main/CONTRIBUTING.md) to get started.
