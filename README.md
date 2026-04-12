# Fuse 🔗

[![CI](https://github.com/seraphjiang/fuse/actions/workflows/ci.yml/badge.svg)](https://github.com/seraphjiang/fuse/actions/workflows/ci.yml)

**Federated Query Engine — Query Any Datasource with SQL or PPL**

Fuse federates queries across 25 connectors from a single SQL or PPL query. Join OpenSearch logs with DynamoDB profiles, union CloudWatch and S3 events, correlate Prometheus metrics with Elasticsearch alerts — all in one statement. Built on [Apache DataFusion](https://datafusion.apache.org/) and [datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation).

🎮 **[Live Playground](https://fuse.huanji.profile.aws.dev)** (Amazon VPN) · 📖 **[Docs Site](https://seraphjiang.github.io/fuse/)** · 📖 **[Proposal](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705)** · 📐 **[Connector Guide](docs/guides/writing-a-connector.md)**

## Highlights

- **25 connectors** — OpenSearch, Elasticsearch, PostgreSQL, MySQL, DynamoDB, S3 (Parquet), S3 O11y (NDJSON), Prometheus, CloudWatch, Redis, CSV/JSON, MongoDB, InfluxDB, ClickHouse, Kafka, Athena, Timestream, Snowflake, BigQuery, Cassandra, DuckDB, Arrow Flight, Spark, Delta Lake, Iceberg
- **Cross-datasource JOINs** — hash join with build-side selection, semi-join, anti-join, correlated subqueries
- **Federated UNION / UNION ALL** — combine results from any mix of sources, with deduplication
- **Window functions** — ROW_NUMBER, RANK, LAG, LEAD over federated results
- **Cross-datasource GROUP BY** — federated re-aggregation (COUNT, SUM merge correctly)
- **Cursor pagination** — keyset-based server-side cursors for efficient paging
- **Cost estimator** — pre-execution estimated_rows and estimated_cost per plan node
- **PPL support** — pipe-delimited query language with `lookup` for cross-source enrichment
- **EXPLAIN / EXPLAIN ANALYZE** — inspect federated query plans and execution stats
- **Prepared statements** — PREPARE/EXECUTE with positional parameter binding, SQL injection prevention
- **Fuse-to-Fuse federation** — chain Fuse instances for multi-region or tiered deployments
- **GraphQL API** — schema introspection, query execution, saved queries, views
- **Scheduled queries** — cron-based scheduling with execution history and alerting
- **Data quality rules** — null rate, cardinality, freshness, row count checks
- **Query cost estimation ($)** — real dollar estimates (Athena $/TB, DDB $/RCU, S3 $/request)
- **Arrow IPC format** — zero-copy binary results for Python/Rust SDK consumers
- **Webhook subscriptions** — event-driven notifications on query result conditions
- **Query replay** — record queries, replay against staging, diff results
- **Query lineage** — data flow graph across connectors (source → transform → sink)
- **Adaptive caching** — auto-cache repeat patterns with per-datasource TTL
- **Multi-tenant SaaS mode** — tenant isolation, usage metering, rate limiting
- **OpenTelemetry collector** — ingest OTLP traces/metrics/logs, query with SQL
- **Query compilation** — skip re-parsing for hot query patterns
- **2700+ tests** — unit, integration, E2E, UI regression, and performance suite

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Client (Playground UI / curl / OpenSearch Dashboards)      │
└──────────────────────┬──────────────────────────────────────┘
                       │ REST API (:9400)
┌──────────────────────▼──────────────────────────────────────┐
│  Fuse Server (axum)                                         │
│                                                             │
│  SQL / PPL Parser ──→ Logical Plan                          │
│                          ↓                                  │
│  DataFusion SessionContext                                  │
│  + FederationOptimizerRule                                  │
│  + Cost-Based Join Planner (build-side selection)           │
│       ┌──────┬──────┬──────┬──────┬──────┬──────┐           │
│       ▼      ▼      ▼      ▼      ▼      ▼      ▼          │
│     OS    ES   PG/MySQL DDB  S3  Prom  CW  Redis ...       │
│     Mongo  InfluxDB  ClickHouse  CSV/JSON                   │
│       └──────┴──────┴──────┴──────┴──────┴──────┘           │
│            Result Merger + Re-Aggregator                    │
│       (align, dedup, sort, limit, GROUP BY merge)           │
│                                                             │
│  Cursor Pagination · Query Cache · Cost Estimator           │
└─────────────────────────────────────────────────────────────┘
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

### Cross-datasource JOIN (OpenSearch + DynamoDB)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT l.trace_id, l.service, u.name, u.plan FROM cluster_a.application_logs l JOIN dynamodb.user_profiles u ON l.user_id = u.user_id WHERE l.status >= 500",
    "format": "sql"
  }'
```

### Federated UNION ALL (3 sources)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT source, service, message FROM cluster_a.application_logs UNION ALL SELECT source, service, message FROM cloudwatch.events UNION ALL SELECT source, service, message FROM s3_o11y.logs LIMIT 50",
    "format": "sql"
  }'
```

### Correlated subquery (anti-join pattern)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT * FROM cluster_a.application_logs WHERE user_id NOT IN (SELECT user_id FROM dynamodb.user_profiles WHERE plan = '\''enterprise'\'')",
    "format": "sql"
  }'
```

### PPL lookup (cross-source enrichment)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "source = cluster_a.application_logs | where status >= 500 | lookup dynamodb.user_profiles user_id AS user_id REPLACE name, plan | stats count() by plan",
    "format": "ppl"
  }'
```

### Cursor pagination

```bash
# First page
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "format": "sql", "page_size": 20}'

# Next page (use next_cursor from previous response)
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "format": "sql", "page_size": 20, "cursor": "<next_cursor>"}'
```

### EXPLAIN (query plan + cost estimate)

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "EXPLAIN SELECT l.service, count(*) FROM cluster_a.application_logs l JOIN dynamodb.user_profiles u ON l.user_id = u.user_id GROUP BY l.service", "format": "sql"}'
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
| OpenSearch | `opensearch` | Basic, SigV4 (AOSS) | Filter, projection, aggregation, sort, limit, search_after |
| Elasticsearch | `elasticsearch` | Basic, API key | Filter, projection, aggregation, sort, limit |
| PostgreSQL | `postgres` | Password | Full SQL pushdown |
| MySQL | `mysql` | Password | Full SQL pushdown |
| DynamoDB | `dynamodb` | SigV4 (IAM) | Filter (Scan/Query), projection, limit |
| S3 (Parquet) | `s3` | SigV4 (IAM) | Column pruning, row-group pagination, limit |
| S3 O11y (NDJSON) | `s3-o11y` | SigV4 (IAM) | Projection, limit |
| Prometheus | `prometheus` | Bearer token | Time range, label filters |
| CloudWatch | `cloudwatch` | SigV4 (IAM) | Log group, time range, filter pattern |
| Redis | `redis` | Password | Key pattern (SCAN), hash/string types |
| CSV/JSON | `csv-json` | None (local/S3) | Schema inference, auto-detect format |
| MongoDB | `mongodb` | Connection string | Filter pushdown (BSON), projection, limit |
| InfluxDB | `influxdb` | Token (v2) / Basic (v1) | InfluxQL WHERE pushdown, time range |
| ClickHouse | `clickhouse` | Basic (HTTP) | Full SQL pushdown (native SQL) |
| Kafka | `kafka` | None / SASL | Topic consume, key/timestamp filter, JSON extraction |
| Athena | `athena` | SigV4 (IAM) | Full SQL pushdown, Glue catalog |
| Timestream | `timestream` | SigV4 (IAM) | Full SQL pushdown, time-series types |
| Snowflake | `snowflake` | Bearer token | Full SQL pushdown, async polling |
| BigQuery | `bigquery` | Service account | Full SQL pushdown, Jobs API |
| Fuse | `fuse` | Bearer token | Full query forwarding (federation) |
| DuckDB | `duckdb` | None (local file) | Full SQL pushdown (native SQL) |
| Arrow Flight | `arrow-flight` | Bearer token (optional) | Flight SQL pushdown, ticket-based streaming |
| Spark | `spark` | Bearer token / Kerberos | Spark SQL pushdown, Thrift/HTTP |
| Delta Lake | `delta-lake` | SigV4 (S3) / None (local) | Predicate pushdown, time travel, partition pruning |
| Iceberg | `iceberg` | SigV4 (S3) / None (local) | Predicate pushdown, snapshot queries, partition pruning |

### Configuration (fuse.toml)

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
id = "my_ddb"
type = "dynamodb"
region = "us-west-2"
table_names = ["user_profiles", "orders"]

[[connector]]
id = "my_pg"
type = "postgres"
url = "postgresql://user:pass@host:5432/mydb"

[[connector]]
id = "my_mongo"
type = "mongodb"
url = "mongodb://host:27017/mydb"

[[connector]]
id = "my_clickhouse"
type = "clickhouse"
url = "http://host:8123"
database = "default"
```

### Build Your Own

Implement the `FederatedConnector` trait (~8 methods) and register a factory:

1. Copy `crates/fuse-connectors/example/` — a minimal working connector with inline comments
2. Follow the [connector authoring guide](docs/guides/writing-a-connector.md)
3. Use `fuse-connector-sdk` for mock testing utilities

## Project Structure

```
fuse/
├── crates/
│   ├── fuse-core/              # Connector traits, registry, config, errors
│   ├── fuse-engine/            # DataFusion federation, PPL parser, JOINs, caching
│   ├── fuse-connectors/
│   │   ├── opensearch/         # OpenSearch (SigV4, Query DSL pushdown)
│   │   ├── elasticsearch/      # Elasticsearch 7.x/8.x
│   │   ├── postgres/           # PostgreSQL + MySQL (sqlx)
│   │   ├── dynamodb/           # DynamoDB (Scan/Query)
│   │   ├── s3/                 # S3 Parquet (column pruning)
│   │   ├── s3-o11y/            # S3 NDJSON (gzipped)
│   │   ├── prometheus/         # Prometheus (PromQL)
│   │   ├── cloudwatch/         # CloudWatch Logs
│   │   ├── redis/              # Redis (SCAN, hash/string)
│   │   ├── csv-json/           # CSV/JSON (local or S3)
│   │   ├── mongodb/            # MongoDB (BSON filter pushdown)
│   │   ├── influxdb/           # InfluxDB 1.x/2.x
│   │   ├── clickhouse/         # ClickHouse (HTTP, full SQL pushdown)
│   │   ├── kafka/              # Kafka (rskafka, JSON extraction)
│   │   ├── athena/             # Amazon Athena (Glue catalog)
│   │   ├── timestream/         # Amazon Timestream (time-series)
│   │   ├── snowflake/          # Snowflake (SQL API, async polling)
│   │   ├── bigquery/           # Google BigQuery (Jobs API)
│   │   ├── cassandra/          # Apache Cassandra (CQL, scylla driver)
│   │   ├── duckdb/             # DuckDB (embedded SQL)
│   │   ├── arrow-flight/       # Arrow Flight (zero-copy streaming)
│   │   ├── fuse/               # Fuse-to-Fuse (federation)
│   │   └── example/            # Minimal connector template
│   ├── fuse-connector-sdk/     # SDK for third-party connectors
│   └── fuse-server/            # REST API (axum) + embedded playground
├── playground/                 # Query playground UI (vanilla HTML/JS/CSS)
├── docs/
│   ├── api/openapi.yaml        # OpenAPI 3.1 spec
│   ├── guides/                 # Connector authoring, getting started
│   ├── rfcs/                   # Integration RFCs
│   └── blog/                   # Blog posts
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
