# Fuse 🔗

**Cross-Datasource Federated Query Engine for OpenSearch Dashboards**

Fuse is a standalone query engine that federates queries across multiple heterogeneous datasources — OpenSearch clusters, S3 data lakes, Prometheus, and more — from a single query interface in OpenSearch Dashboards.

Built on [Apache DataFusion](https://datafusion.apache.org/) and [datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation) for SQL planning, optimization, and push-down to remote engines.

> 🚧 **Status: Incubation / Proposal** — See the [full proposal](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705)

## Prerequisites

- **Rust** stable toolchain (1.85+) — install via [rustup](https://rustup.rs/)
- **OpenSSL dev headers** — `apt install libssl-dev pkg-config` (Debian/Ubuntu) or `yum install openssl-devel` (RHEL/AL2)
- **Docker & Docker Compose** (optional, for local dev environment)

## Build

```bash
cargo build --release
```

## Run

1. Copy and edit the sample config:

```bash
cp fuse.toml my-fuse.toml
# Edit my-fuse.toml with your datasource URLs and credentials
```

2. Start the server:

```bash
FUSE_CONFIG=my-fuse.toml cargo run -p fuse-server
```

The server binds to `0.0.0.0:9400` by default (configurable in `fuse.toml`).

## Docker

Spin up a full dev environment with two OpenSearch nodes and Dashboards:

```bash
docker-compose up
```

| Service | URL |
|---------|-----|
| Fuse API | http://localhost:9400 |
| OpenSearch | http://localhost:9200 |
| Dashboards | http://localhost:5601 |

## API Examples

### Health Check

```bash
curl http://localhost:9400/api/fuse/health
```

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "connectors": {
    "prod_cluster": { "status": "healthy", "latency_ms": 12 }
  }
}
```

### List Datasources

```bash
curl http://localhost:9400/api/fuse/datasources
```

### Query (SQL)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT * FROM prod_cluster.logs WHERE status = 500",
    "format": "sql"
  }'
```

### Query (PPL)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "source = cluster_a.logs, cluster_b.logs | where status >= 500 | stats count() by service",
    "format": "ppl"
  }'
```

### Explain Plan

```bash
curl -X POST http://localhost:9400/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM prod_cluster.logs", "format": "sql"}'
```

### Validate Query

```bash
curl -X POST http://localhost:9400/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "source = prod_cluster.logs | head 10", "format": "ppl"}'
```

### Browse Schemas

```bash
# List tables for a datasource
curl http://localhost:9400/api/fuse/datasources/prod_cluster/schemas

# Get fields for a specific table
curl http://localhost:9400/api/fuse/datasources/prod_cluster/schemas/logs/fields
```

## Query Languages

### SQL

Standard SQL routed through DataFusion's optimizer with federation push-down:

```sql
SELECT service, count(*) AS errors
FROM prod_cluster.logs
WHERE status = 500 AND @timestamp > '2025-01-01'
GROUP BY service
ORDER BY errors DESC
LIMIT 20
```

### PPL (Piped Processing Language)

OpenSearch PPL syntax with multi-source federation:

```
source = cluster_a.logs, cluster_b.logs
| where status >= 500
| stats count() by service
| sort - count
| head 20
```

Supported PPL commands: `where`, `stats` (with `by`), `sort`, `head`, `fields`, `dedup`.

PPL queries are translated to SQL internally and executed through the same DataFusion federation pipeline.

## Configuration

See [`fuse.toml`](fuse.toml) for the full configuration reference. Key sections:

```toml
[engine]
bind = "0.0.0.0:9400"
max_concurrent_queries = 64
default_timeout = "30s"

[[connector]]
id = "prod_cluster"
type = "opensearch"
url = "https://opensearch-prod.example.com:9200"

[connector.auth]
type = "basic"
username = "admin"
password_env = "FUSE_PROD_PASSWORD"
```

Auth types: `basic`, `sigv4`, `bearer`.

## Project Structure

```
fuse/
├── Cargo.toml                  # Workspace root
├── fuse.toml                   # Sample configuration
├── Dockerfile                  # Multi-stage build
├── docker-compose.yml          # Dev environment (OpenSearch + Dashboards)
├── crates/
│   ├── fuse-core/              # Connector traits, registry, config, errors
│   ├── fuse-engine/            # DataFusion federation, PPL parser, result merger
│   ├── fuse-connectors/
│   │   └── opensearch/         # OpenSearch connector implementation
│   └── fuse-server/            # REST API (axum)
├── docs/
│   └── design/                 # Design documents
├── tests/integration/          # Integration tests
└── .fuse-project/              # Project management (backlog, ADRs, sprints)
```

## Architecture

```
┌─────────────────────────────────┐
│  OpenSearch Dashboards (UI)     │
│  Query Bar / Dashboard Panels   │
└──────────────┬──────────────────┘
               │ REST API (:9400)
┌──────────────▼──────────────────┐
│  Fuse Server (axum)             │
│                                 │
│  PPL Parser → SQL Translation   │
│         ↓                       │
│  DataFusion SessionContext      │
│  + FederationOptimizerRule      │
│         ↓                       │
│  FuseExecutor (SQLExecutor)     │
│    ┌────┼────────┐              │
│    ▼    ▼        ▼              │
│   OS   S3    Prometheus         │
│         ↓                       │
│  Result Merger (align + dedup)  │
└─────────────────────────────────┘
```

## Roadmap

| Phase | Focus | Timeline |
|-------|-------|----------|
| 1 | Same-type federation (OS ↔ OS) | 8 weeks |
| 2 | Cross-type federation (OS ↔ S3) + JOINs | 8 weeks |
| 3 | Prometheus connector + Connector SDK | 6 weeks |
| 4 | Caching, materialized views, RBAC | 8 weeks |

## Contributing

See [`.fuse-project/`](.fuse-project/) for backlog, architecture decisions (ADRs), sprint plans, and project requirements.

- 📋 [Proposal & Design Doc](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705)
- 📐 [Connector Interface Design](docs/design/connector-interface-and-query-parser.md)
- 💬 Open an issue to discuss

## License

This project is licensed under the Apache License 2.0 — see [LICENSE](LICENSE) for details.
