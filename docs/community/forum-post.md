# OpenSearch Community Forum Post — Fuse Federated Query Engine

**Target:** [OpenSearch Forum — Feature Proposals](https://forum.opensearch.org/c/feature-proposals/)

---

**Title:** [RFC] Fuse — Federated Query Engine for Cross-Datasource Queries in OpenSearch Dashboards

**Body:**

Hi OpenSearch community,

We've been building **Fuse**, a standalone federated query engine that lets you query across multiple OpenSearch clusters, S3 data lakes, and other datasources from a single query in OpenSearch Dashboards.

## The Problem

If you run multiple OpenSearch clusters — say one for production logs and another for staging, or separate clusters per region — querying across them today means writing glue code: export from one, import to another, merge manually. The same applies when your data spans OpenSearch and S3 (e.g., hot/warm/cold tiering).

## What Fuse Does

Fuse sits between Dashboards and your datasources. You write one SQL or PPL query, and Fuse:

1. Parses the query and identifies which datasources are referenced
2. Uses Apache DataFusion + datafusion-federation to plan and optimize
3. Pushes filters, projections, and aggregations down to each source
4. Merges results with global sort/limit/dedup

```
Dashboards → Fuse Server (REST API)
                ↓
            DataFusion Planner
            ↓         ↓         ↓
         OS Cluster  S3 Lake  Prometheus
                ↓
            Result Merger
```

## What's Working Today

- **Same-type federation**: Query across 2+ OpenSearch clusters (including AOSS with SigV4)
- **Cross-type federation**: Query OpenSearch + S3 (gzipped NDJSON) with JOINs
- **SQL and PPL support**: Both query languages, translated through DataFusion
- **7 REST API endpoints**: query, explain, validate, health, datasources, schemas, fields
- **Query result caching**: Per-connector TTL (OS: 30s, S3: 5min)
- **Materialized views**: Pre-computed query results with scheduled refresh
- **RBAC**: Role-based access control and field-level security
- **Live playground**: Web UI with clickable examples and "🎲 Feeling Lucky" button
- **4 connectors**: OpenSearch (SigV4/AOSS), S3/Parquet, S3 O11y (NDJSON), Prometheus

### Example: Multi-cluster error analysis (SQL)

```sql
SELECT service, count(*) as errors
FROM cluster_a.application_logs
WHERE status >= 500
GROUP BY service
ORDER BY errors DESC
```

### Example: Cross-cluster search (PPL)

```
source = cluster_a.application_logs, cluster_b.application_logs
| where status >= 500
| stats count() by service
| sort - count()
| head 20
```

### Example: Cross-source JOIN (OpenSearch + S3)

```sql
SELECT l.trace_id, l.service, l.status, s.level, s.message
FROM cluster_a.application_logs l
JOIN s3_o11y.fuse_logs s ON l.trace_id = s.trace_id
WHERE l.status >= 500
```

### Example: Federated UNION ALL

```sql
SELECT service, status, message
FROM cluster_a.application_logs
UNION ALL
SELECT service, status, message
FROM cluster_b.application_logs
LIMIT 20
```

## Architecture

Built in Rust with:
- **Apache DataFusion** for SQL planning and optimization
- **datafusion-federation** for routing table scans to connectors
- **axum** for the REST API server
- **Pluggable connectors**: OpenSearch, S3/NDJSON, Prometheus (stub), with a Connector SDK

Each connector implements a `FederatedConnector` trait (~8 methods) covering health checks, schema discovery, and query execution. The engine handles planning, push-down optimization, and result merging.

## Connector SDK

We've published a [connector authoring guide](https://github.com/seraphjiang/fuse/blob/main/docs/guides/writing-a-connector.md) that walks through building a new connector from scratch. We'd love community connectors for PostgreSQL, MySQL, ClickHouse, MongoDB, DynamoDB, Kafka, and more.

## Try It

```bash
git clone https://github.com/seraphjiang/fuse
cd fuse
docker compose up -d
cargo run -p fuse-server
# Open http://localhost:9400/
```

## Links

- **GitHub**: https://github.com/seraphjiang/fuse
- **Original proposal**: https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705
- **Connector guide**: https://github.com/seraphjiang/fuse/blob/main/docs/guides/writing-a-connector.md
- **OpenAPI spec**: https://github.com/seraphjiang/fuse/blob/main/docs/api/openapi.yaml
- **Blog draft**: https://github.com/seraphjiang/fuse/blob/main/docs/blog/introducing-fuse.md

## Feedback Wanted

1. **Use cases**: What cross-datasource queries would you run?
2. **Connectors**: Which datasources should we prioritize?
3. **OSD integration**: How should this surface in Dashboards? (We have an RFC for a query bar plugin)
4. **PPL extensions**: What multi-source PPL syntax feels natural?

This is Apache 2.0 licensed. We welcome contributions — code, docs, connector ideas, or just feedback on the approach.

Thanks for reading. Looking forward to the discussion.

---

**Tags:** `feature-proposal`, `federation`, `query-engine`, `dashboards`, `ppl`, `sql`
