# What is Fuse?

Fuse is a federated query engine. It lets you write one SQL or PPL query that runs across multiple datasources — OpenSearch, PostgreSQL, DynamoDB, S3, Prometheus, and more — and returns merged results.

## The Problem

Modern applications spread data across many systems:

- **Logs** in OpenSearch or CloudWatch
- **User profiles** in DynamoDB or PostgreSQL
- **Metrics** in Prometheus or InfluxDB
- **Analytics** in S3 Parquet or ClickHouse
- **Cache** in Redis

To answer "which users are causing the most errors?", you'd need to:
1. Query OpenSearch for error logs
2. Query DynamoDB for user profiles
3. Export both to CSV
4. Join them in a spreadsheet or Python script
5. Repeat every time the data changes

Fuse eliminates steps 2–5.

## How It Works

```
You write:
  SELECT l.service, u.name, count(*) as errors
  FROM opensearch.logs l
  JOIN dynamodb.users u ON l.user_id = u.user_id
  WHERE l.status >= 500
  GROUP BY l.service, u.name

Fuse does:
  1. Parse → identify datasources (OpenSearch + DynamoDB)
  2. Plan → build SubQuery per source, decide pushdown
  3. Fan out → query both in parallel
  4. Merge → hash join on user_id, re-aggregate GROUP BY
  5. Return → unified result set with _datasource provenance
```

Fuse translates your SQL into each datasource's native language — Query DSL for OpenSearch, FilterExpression for DynamoDB, SQL for PostgreSQL, BSON for MongoDB — so filters and projections are pushed down for performance.

## Key Features

- **15 connectors** — OpenSearch, Elasticsearch, PostgreSQL, MySQL, DynamoDB, S3, Prometheus, CloudWatch, Redis, CSV/JSON, MongoDB, InfluxDB, ClickHouse, DuckDB, S3 O11y
- **Cross-datasource JOINs** — hash join, semi-join, anti-join, correlated subqueries
- **UNION ALL** — combine any mix of sources with automatic schema alignment
- **SQL + PPL** — standard SQL and OpenSearch's pipe-delimited language
- **Dashboard platform** — 12 chart types, variables, drill-down, templates
- **Cursor pagination** — efficient paging through large result sets
- **AI-powered** — natural language → SQL, query advisor
- **Enterprise** — multi-tenancy, API key auth, rate limiting, audit logging
- **SDKs** — Python and TypeScript (zero dependencies)
- **Grafana plugin** — use Fuse as a Grafana datasource
- **1,000+ tests** — unit, integration, E2E, performance regression

## Quick Start

```bash
git clone https://github.com/seraphjiang/fuse && cd fuse
cargo build --release
./target/release/fuse-server --config fuse.toml
# Open http://localhost:9400
```

Or try the [live playground](https://fuse.huanji.profile.aws.dev) (Amazon VPN).

→ [Full Getting Started Guide](./getting-started.md)

## How Fuse Compares

| | Fuse | Trino/Presto | Direct Queries |
|---|------|-------------|----------------|
| **Setup** | Single binary, TOML config | JVM cluster, coordinator + workers | N/A |
| **Latency** | Sub-100ms (p99) | Seconds (query planning overhead) | Varies |
| **Connectors** | 15 (observability-focused) | 40+ (warehouse-focused) | 1 per tool |
| **Query language** | SQL + PPL | SQL (ANSI) | Native per datasource |
| **Pushdown** | Filter, projection, agg, sort, limit | Filter, projection, limit | Full native |
| **JOINs** | Cross-datasource hash join | Cross-datasource | Within single datasource |
| **Built-in UI** | Playground + dashboards + charts | None (use external tools) | Varies |
| **Target use case** | Observability, log analysis, operational analytics | Data warehouse federation | Single-source queries |
| **Resource footprint** | ~50MB binary, single process | Multi-GB JVM, multi-node | N/A |

**Choose Fuse when:**
- You need sub-second queries across observability data (logs, metrics, traces)
- You want a single binary with zero JVM overhead
- You need built-in dashboards and visualization
- Your datasources are OpenSearch, DynamoDB, Prometheus, CloudWatch, S3

**Choose Trino/Presto when:**
- You need warehouse-scale analytics (petabytes)
- You need ANSI SQL compliance for complex analytical queries
- Your datasources are primarily Hive, Iceberg, Delta Lake, Redshift

**Choose direct queries when:**
- You only query one datasource
- You need the full native query language (e.g., OpenSearch Query DSL aggregations)
- Latency is not a concern

## Learn More

- [Architecture](./architecture.md) — how federated query execution works
- [Connectors](./connectors.md) — all 15 datasource types
- [SQL Reference](./sql-reference.md) — JOINs, UNION, window functions, CTEs
- [Dashboard Guide](./dashboard-guide.md) — visualization platform
- [Contributing](./contributing.md) — get involved
