# Introducing Fuse: Federated Queries Across OpenSearch, S3, and Beyond

If you run OpenSearch, you probably run more than one cluster. Maybe a
production cluster for logs, a staging cluster for testing, an S3 data lake
for long-term analytics, and Prometheus for infrastructure metrics. Each has
its own query interface, its own schema, its own way of doing things.

When a question spans two of these systems — "show me the 500 errors from
both prod and staging clusters, sorted by timestamp" — you're stuck writing
glue code. Export from one, import to another, merge in a script, hope the
schemas align. It works, but it doesn't scale.

**Fuse is a federated query engine that lets you query all of them from one
place.**

## How It Works

Fuse sits between OpenSearch Dashboards and your datasources. You write one
query — SQL or PPL — and Fuse figures out which parts go where, pushes
filters and aggregations down to each source, and merges the results.

```
┌──────────────────────────────┐
│  OpenSearch Dashboards       │
│  (Query Bar / Panels)        │
└──────────┬───────────────────┘
           │ REST API
┌──────────▼───────────────────┐
│  Fuse Server                 │
│                              │
│  SQL / PPL Parser            │
│        ↓                     │
│  DataFusion Query Planner    │
│  + Federation Optimizer      │
│     ┌──┼──────┐              │
│     ▼  ▼      ▼              │
│    OS  S3  Prometheus        │
│        ↓                     │
│  Result Merger               │
└──────────────────────────────┘
```

Under the hood, Fuse uses [Apache DataFusion](https://datafusion.apache.org/)
for SQL planning and [datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation)
to route table scans to the right connector. Each connector translates the
query plan into native operations — OpenSearch Query DSL, S3 object reads,
PromQL — so the heavy lifting happens at the source.

Authentication is handled per-connector. OpenSearch connectors support SigV4
(including Amazon OpenSearch Serverless) and basic auth. S3 connectors use
IAM roles. No credentials are stored in config files — only environment
variable references.

## What You Can Do Today

### Multi-Cluster Search

Query across two OpenSearch clusters in one statement:

```sql
SELECT service, count(*) as errors
FROM cluster_a.application_logs
WHERE status >= 500
GROUP BY service
ORDER BY errors DESC
```

Or with PPL:

```
source = cluster_a.application_logs, cluster_b.application_logs
| where status >= 500
| stats count() by service
| sort - count()
| head 20
```

Fuse fans the query out to both clusters in parallel, pushes the filter and
aggregation down, and merges the results with a global sort and limit.

### Cross-Source Federation

Join OpenSearch logs with S3 data — the first real cross-type federation:

```sql
SELECT l.trace_id, l.service, l.status, s.level, s.message
FROM cluster_a.application_logs l
JOIN s3_o11y.logs s ON l.trace_id = s.trace_id
WHERE l.status >= 500
```

DataFusion's optimizer uses semi-join pushdown: it extracts matching keys
from the smaller side and pushes an IN-filter to the larger side, avoiding
full table scans.

### Query Result Caching

Fuse caches results per-connector with configurable TTLs — OpenSearch at 30s,
S3 at 5 minutes. Repeated queries hit the cache instead of the datasource.
Materialized views take this further: define a query that refreshes on a
schedule, and read pre-computed results instantly.

## Try It

A live playground is running at **https://fuse.huanji.profile.aws.dev**
(Amazon VPN required). Click "🎲 Feeling Lucky" to run a random query
against real OpenSearch Serverless clusters.

Or run locally:

```bash
git clone https://github.com/seraphjiang/fuse
cd fuse
docker compose up -d
cargo run -p fuse-server
# Open http://localhost:9400/
```

Hit the API directly:

```bash
# Health check
curl http://localhost:9400/api/fuse/health

# Federated query
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT service, status, message FROM cluster_a.application_logs WHERE status >= 500 LIMIT 20"}'

# Explore schemas
curl http://localhost:9400/api/fuse/datasources/cluster_a/schemas
```

## Build a Connector

Fuse is designed to be extended. Every datasource is a Rust crate that
implements the `FederatedConnector` trait — about 8 methods covering health
checks, schema discovery, and query execution. The engine handles planning,
optimization, and result merging.

We ship four connectors today: OpenSearch (with SigV4/AOSS support),
S3/Parquet, S3 O11y (gzipped NDJSON), and Prometheus. The
[connector authoring guide](https://github.com/seraphjiang/fuse/blob/main/docs/guides/writing-a-connector.md)
walks through building a new one from scratch.

We'd love community connectors for PostgreSQL, MySQL, ClickHouse, MongoDB,
DynamoDB, Kafka — anything with a query interface.

## What's Next

| Phase | Focus | Status |
|-------|-------|--------|
| 1 | Same-type federation (OS ↔ OS) | ✅ Shipped |
| 2 | Cross-type federation (OS ↔ S3) + JOINs | ✅ Shipped |
| 3 | Prometheus connector + Connector SDK | ✅ Shipped |
| 4 | Caching, materialized views, RBAC | ✅ Shipped |
| Next | OSD plugin integration, visual join builder | In progress |

## Get Involved

- 🔗 [GitHub](https://github.com/seraphjiang/fuse) — star, fork, contribute
- 🎮 [Live Playground](https://fuse.huanji.profile.aws.dev) — try it now (Amazon VPN)
- 📖 [Proposal](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705) — the original design discussion
- 📐 [Connector Guide](https://github.com/seraphjiang/fuse/blob/main/docs/guides/writing-a-connector.md) — build your own
- 🐛 [Issues](https://github.com/seraphjiang/fuse/issues) — report bugs, request features, propose connectors

Fuse is Apache 2.0 licensed. We welcome contributions of all kinds — code,
docs, connector ideas, feedback. Open an issue or submit a PR to get started.
