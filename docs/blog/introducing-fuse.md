# Introducing Fuse: Federated Queries Across OpenSearch, S3, and Prometheus

*Draft — for OpenSearch community blog*

---

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

## What Fuse Does

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
query plan into native operations — OpenSearch Query DSL, S3 Select, PromQL —
so the heavy lifting happens at the source.

## Three Things You Can Do Today

### 1. Multi-Cluster Search

Query across two OpenSearch clusters in one statement:

```sql
SELECT trace_id, status, message
FROM prod_cluster.logs
WHERE status >= 500 AND @timestamp > '2025-01-01'
ORDER BY @timestamp DESC
LIMIT 50
```

Or with PPL:

```
source = prod_cluster.logs, staging_cluster.logs
| where status >= 500
| sort - @timestamp
| head 50
```

Fuse fans the query out to both clusters, pushes the filter and sort down,
and merges the results with a global sort and limit.

### 2. Cross-Source JOINs (Phase 2)

Join OpenSearch logs with S3 Parquet data:

```sql
SELECT l.trace_id, l.message, o.customer_id
FROM opensearch_prod.logs l
JOIN s3_lake.orders o ON l.order_id = o.id
WHERE l.status = 500
```

DataFusion's optimizer decides whether to use a hash join, push a semi-join
filter down, or delegate to Spark for large datasets.

### 3. Metrics Correlation (Phase 3)

Correlate application errors with infrastructure metrics:

```sql
SELECT l.service, l.error_count, p.cpu_p99
FROM opensearch_prod.error_summary l
JOIN prometheus.container_cpu p ON l.service = p.service
WHERE l.error_count > 100
```

## Try It

```bash
git clone https://github.com/seraphjiang/fuse
cd fuse

# Start OpenSearch + Fuse
docker compose up -d
cargo run -p fuse-server

# Open the playground
open http://localhost:9400/
```

Or hit the API directly:

```bash
# Health check
curl http://localhost:9400/api/fuse/health

# Run a query
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM local_cluster.test_index LIMIT 10"}'

# Explore schemas
curl http://localhost:9400/api/fuse/datasources/local_cluster/schemas
```

## Build a Connector

Fuse is designed to be extended. Every datasource is a Rust crate that
implements the `FederatedConnector` trait — about 8 methods covering health
checks, schema discovery, and query execution. The engine handles planning,
optimization, and result merging.

The [connector authoring guide](docs/guides/writing-a-connector.md) walks
through the full process using the OpenSearch connector as a reference.

We'd love connectors for PostgreSQL, MySQL, ClickHouse, MongoDB, Kafka,
DynamoDB — anything with a query interface.

## What's Next

| Phase | Focus | Status |
|-------|-------|--------|
| 1 | Same-type federation (OS ↔ OS) | ✅ Working |
| 2 | Cross-type federation (OS ↔ S3) + JOINs | In progress |
| 3 | Prometheus connector + Connector SDK | Planned |
| 4 | Caching, materialized views, RBAC | Planned |

## Get Involved

- 🔗 [GitHub](https://github.com/seraphjiang/fuse) — star, fork, contribute
- 📖 [Proposal](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705) — the original design discussion
- 🐛 [Issues](https://github.com/seraphjiang/fuse/issues) — report bugs, request features, propose connectors
- 📐 [Connector Guide](docs/guides/writing-a-connector.md) — build your own

Fuse is Apache 2.0 licensed. We welcome contributions of all kinds — code,
docs, connector ideas, feedback. Open an issue or submit a PR to get started.
