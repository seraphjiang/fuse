# Fuse 🔗

**Cross-Datasource Federated Query Engine for OpenSearch Dashboards**

Fuse federates queries across multiple OpenSearch clusters, S3 data lakes, and Prometheus from a single SQL or PPL query. Built on [Apache DataFusion](https://datafusion.apache.org/) and [datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation).

🎮 **[Live Playground](https://fuse.huanji.profile.aws.dev)** (Amazon VPN) · 📖 **[Proposal](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705)** · 📐 **[Connector Guide](docs/guides/writing-a-connector.md)**

## Architecture

```
┌─────────────────────────────────────────────┐
│  OpenSearch Dashboards                      │
│  Query Bar (SQL + PPL) / Dashboard Panels   │
└──────────────────┬──────────────────────────┘
                   │ REST API (:9400)
┌──────────────────▼──────────────────────────┐
│  Fuse Server (axum)                         │
│                                             │
│  PPL Parser ──→ SQL Translation             │
│                    ↓                        │
│  DataFusion SessionContext                  │
│  + FederationOptimizerRule                  │
│  + Cost-Based Join Planner                  │
│       ┌────────┼────────┬──────────┐        │
│       ▼        ▼        ▼          ▼        │
│   OpenSearch  S3/NDJSON  S3/Parquet  Prom   │
│   (SigV4)    (gzip)     (col prune) (PromQL)│
│       └────────┼────────┴──────────┘        │
│            Result Merger                    │
│       (align, dedup, sort, limit)           │
│                                             │
│  Query Cache (per-connector TTL)            │
│  Materialized Views (scheduled refresh)     │
└─────────────────────────────────────────────┘
```

## Quick Start

```bash
git clone https://github.com/seraphjiang/fuse
cd fuse

# Option 1: Local with Docker
docker compose up -d          # Start OpenSearch cluster
cargo run -p fuse-server      # Start Fuse on :9400
open http://localhost:9400/   # Playground UI

# Option 2: Just build and test
cargo build --release
cargo test --all-targets
```

### Prerequisites

- Rust stable (1.85+) — `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- OpenSSL dev — `apt install libssl-dev pkg-config` or `yum install openssl-devel`
- Docker (optional, for local OpenSearch)

## Query Examples

### Multi-cluster error analysis (SQL)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT service, count(*) as errors FROM cluster_a.application_logs WHERE status >= 500 GROUP BY service ORDER BY errors DESC",
    "format": "sql"
  }'
```

### Cross-cluster search (PPL)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "source = cluster_a.application_logs, cluster_b.application_logs | where status >= 500 | stats count() by service | sort - count()",
    "format": "ppl"
  }'
```

### Cross-source JOIN (OpenSearch + S3)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT l.trace_id, l.service, s.level, s.message FROM cluster_a.application_logs l JOIN s3_o11y.fuse_logs s ON l.trace_id = s.trace_id WHERE l.status >= 500",
    "format": "sql"
  }'
```

### Federated UNION ALL

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT service, status, message FROM cluster_a.application_logs UNION ALL SELECT service, status, message FROM cluster_b.application_logs LIMIT 20",
    "format": "sql"
  }'
```

### Other endpoints

```bash
curl http://localhost:9400/api/fuse/health              # Health + connector status
curl http://localhost:9400/api/fuse/datasources          # List connectors
curl http://localhost:9400/api/fuse/datasources/cluster_a/schemas        # Tables
curl http://localhost:9400/api/fuse/datasources/cluster_a/schemas/application_logs/fields  # Fields
```

## Connectors

| Connector | Type | Auth | Push-down |
|-----------|------|------|-----------|
| OpenSearch | `opensearch` | Basic, SigV4 (AOSS) | Filter, projection, aggregation, sort, limit |
| S3/Parquet | `s3` | SigV4 (IAM) | Projection (column pruning), limit |
| S3 O11y | `s3-o11y` | SigV4 (IAM) | Projection, limit |
| Prometheus | `prometheus` | Bearer token | Time range, label filters |

### Configuration

```toml
[engine]
bind = "0.0.0.0:9400"
max_concurrent_queries = 64

[[connector]]
id = "cluster_a"
type = "opensearch"
url = "https://your-cluster.us-west-2.aoss.amazonaws.com"

[connector.auth]
type = "sigv4"
region = "us-west-2"
service = "aoss"

[[connector]]
id = "s3_o11y"
type = "s3-o11y"
bucket = "your-log-bucket"
prefix = "logs/"
region = "us-west-1"
```

### Build Your Own

Implement the `FederatedConnector` trait (~8 methods) and register a factory. The fastest path:

1. Copy `crates/fuse-connectors/example/` — a minimal working connector with inline comments
2. Follow the [connector authoring guide](docs/guides/writing-a-connector.md)
3. Use `fuse-connector-sdk` for mock testing utilities

## Project Structure

```
fuse/
├── crates/
│   ├── fuse-core/              # Connector traits, registry, config, errors, alerting, RBAC
│   ├── fuse-engine/            # DataFusion federation, PPL parser, JOINs, caching, materialized views
│   ├── fuse-connectors/
│   │   ├── opensearch/         # OpenSearch connector (SigV4, Query DSL pushdown)
│   │   ├── s3/                 # S3/Parquet connector
│   │   ├── s3-o11y/            # S3 O11y connector (gzipped NDJSON)
│   │   └── prometheus/         # Prometheus connector
│   ├── fuse-connector-sdk/     # Connector SDK for third-party development
│   ├── fuse-connectors/example/ # Minimal working connector template
│   └── fuse-server/            # REST API (axum) + embedded playground
├── playground/                 # Query playground UI (vanilla HTML/JS/CSS)
├── docs/
│   ├── api/openapi.yaml        # OpenAPI 3.1 spec
│   ├── guides/                 # Connector authoring guide
│   ├── rfcs/                   # Integration RFCs
│   └── blog/                   # Blog posts
├── scripts/                    # setup-dev.sh, test-local.sh
├── fuse.toml                   # Sample configuration
├── Dockerfile                  # Multi-stage Rust build
└── docker-compose.yml          # Dev environment (OpenSearch + Dashboards)
```

## Dev Scripts

```bash
./scripts/setup-dev.sh    # Check prerequisites, verify build
./scripts/test-local.sh   # Docker + OpenSearch + cargo test + API smoke test
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for DCO sign-off, code style, and PR checklist. Connector contributions welcome — follow the [connector guide](docs/guides/writing-a-connector.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
