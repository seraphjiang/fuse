# Fuse v1.0.0: General Availability

*April 2026 · Fuse Team*

Fuse v1.0.0 is generally available. After 11 sprints, 8 agents, and 1,100+ tests, Fuse is production-ready for federated query workloads.

## What is Fuse?

Fuse is a federated query engine that lets you write one SQL or PPL query across multiple datasources — OpenSearch, PostgreSQL, DynamoDB, S3, Prometheus, and more — and get merged results. Built on Apache DataFusion, it runs as a single binary with sub-100ms query latency.

→ [What is Fuse?](./what-is-fuse.md) for the full overview.

## What's New Since v0.6.0

### Horizontal Scaling

Run multiple stateless Fuse instances behind a load balancer with Redis-backed cache and tenant registry. Hot-reload tenant configs without restarts.

```yaml
# Docker Compose: 3 instances behind nginx
services:
  fuse-1: { build: ., volumes: [./fuse.stateless.toml:/etc/fuse/fuse.toml:ro] }
  fuse-2: { build: ., volumes: [./fuse.stateless.toml:/etc/fuse/fuse.toml:ro] }
  fuse-3: { build: ., volumes: [./fuse.stateless.toml:/etc/fuse/fuse.toml:ro] }
  nginx:  { image: nginx:alpine, ports: ["9400:9400"] }
  redis:  { image: redis:7-alpine }
```

### Materialized Views

Pre-compute expensive federated queries on a schedule:

```toml
[[view]]
name = "error_summary"
query = "SELECT service, count(*) as errors FROM cluster_a.logs WHERE status >= 500 GROUP BY service"
refresh_secs = 300
```

```sql
SELECT * FROM view.error_summary  -- sub-millisecond, served from cache
```

### WASM Plugin System

Build custom connectors as WebAssembly plugins. Implement the `FederatedConnectorPlugin` trait, compile to `wasm32-wasi`, drop into `plugins/`, and query.

### Federation Architecture

Chain Fuse instances in hub-and-spoke topologies for multi-region deployments. A hub routes queries to regional spokes, each managing local datasources.

### CLI

Full command-line interface: `fuse query`, `fuse explain`, `fuse health`, `fuse views`, `fuse config check`.

### VS Code Extension

Schema-aware autocomplete, inline query execution, EXPLAIN visualization, and datasource explorer.

## GA Feature Set

| Category | Features |
|----------|----------|
| Connectors | 17+ types: OpenSearch, Elasticsearch, PostgreSQL, MySQL, DynamoDB, S3, Prometheus, CloudWatch, Redis, CSV/JSON, MongoDB, InfluxDB, ClickHouse, DuckDB, Redshift, Kafka, Timestream |
| Query | SQL + PPL, cross-datasource JOINs, UNION ALL, window functions, recursive CTEs, cursor pagination |
| Enterprise | Multi-tenancy, API key auth (3 roles), rate limiting, query governor, audit logging |
| AI | NL→SQL, query advisor, anomaly detection, auto-suggest |
| Visualization | 12 chart types, dashboards, templates, variables, drill-down, auto-refresh |
| Scaling | Stateless mode, Redis cache, horizontal scaling, materialized views |
| Extensibility | WASM plugin system, hub-spoke federation |
| SDKs | Python (PyPI), TypeScript (npm), Grafana plugin, VS Code extension, CLI |
| Docs | 35-page docs site with guides for every feature |
| Testing | 1,100+ tests (unit, integration, E2E, chaos, performance regression) |

## Migrating from v0.6.0

v1.0.0 is backward-compatible with v0.6.0 configs. No breaking changes.

**New optional config sections:**

```toml
# Stateless mode (optional — omit to keep single-instance behavior)
[engine]
mode = "stateless"

[redis]
url = "redis://localhost:6379"

# Materialized views (optional)
[[view]]
name = "my_view"
query = "SELECT ..."
refresh_secs = 300

# WASM plugins (optional — just add plugins/ directory)
```

**New endpoints:**

| Endpoint | Description |
|----------|-------------|
| `GET /api/fuse/views` | List materialized views |
| `POST /api/fuse/views/{name}/refresh` | Trigger view refresh |
| `GET /api/fuse/plugins` | List loaded WASM plugins |
| `GET /api/fuse/completions` | Auto-suggest queries |

All existing endpoints are unchanged. See [API Stability Guarantee](./api-stability.md).

## Migrating from Direct Queries

If you're currently querying datasources directly:

1. Install Fuse: `cargo install fuse-server` or use the Docker image
2. Add datasources to `fuse.toml`
3. Replace native queries with SQL: `SELECT * FROM datasource.table WHERE ...`
4. Add cross-datasource JOINs that weren't possible before

→ [Migration Guide](./migration-guide.md) for OpenSearch Query DSL → Fuse SQL translation.

## Try It

**Playground:** [https://fuse.huanji.profile.aws.dev](https://fuse.huanji.profile.aws.dev)

**Docker:**
```bash
docker run -p 9400:9400 -v ./fuse.toml:/etc/fuse/fuse.toml ghcr.io/seraphjiang/fuse:1.0.0
```

**From source:**
```bash
git clone https://github.com/seraphjiang/fuse && cd fuse
cargo build --release
./target/release/fuse-server --config fuse.toml
```

**SDKs:**
```bash
pip install fuse-client        # Python
npm install fuse-client        # TypeScript
```

## What's Next

Fuse v1.0.0 is the foundation. Planned for v1.x:
- More connectors (Snowflake, Cassandra, SQLite)
- Streaming queries (Kafka consumer groups)
- Query result caching with invalidation
- Distributed query execution
- Terraform provider

See the [Roadmap](./roadmap.md).

## Thank You

Fuse was built by 8 agents across 11 sprints. Thank you to the community for feedback, bug reports, and contributions. We're just getting started.

→ [Contributing](./contributing.md) · [Community](./community-guide.md) · [GitHub](https://github.com/seraphjiang/fuse)
