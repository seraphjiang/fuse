# Fuse — Federated Query Engine

Fuse is a federated query engine that lets you query any datasource with SQL or PPL. Join OpenSearch logs with DynamoDB profiles, union CloudWatch and S3 events, correlate Prometheus metrics with Elasticsearch alerts — all in one statement. Built on [Apache DataFusion](https://datafusion.apache.org/) and [datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation).

**[🎮 Live Playground](https://fuse.huanji.profile.aws.dev)** *(Amazon VPN required)*

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
│  + Cost-Based Join Planner (build-side selection)           │
│       ┌──────┬──────┬──────┬──────┬──────┬──────┐           │
│       ▼      ▼      ▼      ▼      ▼      ▼      ▼          │
│     OS    ES   PG/MySQL DDB  S3  Prom  CW  Redis ...       │
│     Mongo  InfluxDB  ClickHouse  CSV/JSON                   │
│       └──────┴──────┴──────┴──────┴──────┴──────┘           │
│            Result Merger + Re-Aggregator                    │
│  Cursor Pagination · Query Cache · Cost Estimator           │
└─────────────────────────────────────────────────────────────┘
```

See [Architecture](./architecture.md) for the full query execution model.

## Key Features

- **14 connectors** — OpenSearch, Elasticsearch, PostgreSQL, MySQL, DynamoDB, S3 (Parquet), S3 O11y (NDJSON), Prometheus, CloudWatch, Redis, CSV/JSON, MongoDB, InfluxDB, ClickHouse
- **Cross-datasource JOINs** — hash join with build-side selection, semi-join (EXISTS), anti-join (NOT EXISTS)
- **Correlated subqueries** — `WHERE col IN (SELECT ... FROM other_source)`
- **Federated UNION / UNION ALL** — combine any mix of sources, with deduplication
- **Window functions** — ROW_NUMBER, RANK, LAG, LEAD over federated results
- **Cross-datasource GROUP BY** — federated re-aggregation across sources
- **Cursor pagination** — keyset-based server-side cursors for efficient paging
- **Cost estimator** — pre-execution cost estimates per plan node
- **SQL + PPL** — full support for both query languages, including PPL `lookup`
- **EXPLAIN / ANALYZE** — inspect federated query plans and execution stats
- **Data provenance** — `_datasource` column shows where each row came from
- **Caching** — TTL-based query result cache per connector type
- **Materialized views** — pre-computed cross-datasource views with scheduled refresh
- **Saved queries** — named query templates with parameter binding
- **Query cancellation** — cancel long-running queries by ID
- **Partial failure resilience** — UNION ALL continues if one datasource fails
- **845+ tests** — unit, integration, E2E, and performance regression suite

## Quick Start

```bash
git clone https://github.com/seraphjiang/fuse.git
cd fuse
cargo build --release
./target/release/fuse-server --config fuse.toml
```

Open `http://localhost:9400` for the playground UI. See [Getting Started](./getting-started.md) for full setup instructions.

## Connectors

| Connector | Type | Push-down |
|-----------|------|-----------|
| OpenSearch | `opensearch` | Filter, projection, aggregation, sort, limit, search_after |
| Elasticsearch | `elasticsearch` | Filter, projection, aggregation, sort, limit |
| PostgreSQL | `postgres` | Full SQL pushdown |
| MySQL | `mysql` | Full SQL pushdown |
| DynamoDB | `dynamodb` | Filter (Scan/Query), projection, limit |
| S3 (Parquet) | `s3` | Column pruning, row-group pagination, limit |
| S3 O11y (NDJSON) | `s3-o11y` | Projection, limit |
| Prometheus | `prometheus` | Time range, label filters |
| CloudWatch | `cloudwatch` | Log group, time range, filter pattern |
| Redis | `redis` | Key pattern (SCAN), hash/string types |
| CSV/JSON | `csv-json` | Schema inference, auto-detect format |
| MongoDB | `mongodb` | Filter (BSON), projection, limit |
| InfluxDB | `influxdb` | InfluxQL WHERE, time range |
| ClickHouse | `clickhouse` | Full SQL pushdown |

See [Connectors](./connectors.md) for configuration details.
