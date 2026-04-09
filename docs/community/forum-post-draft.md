# [RFC/Proposal] Fuse: Cross-Datasource Federated Query Engine for OpenSearch Dashboards

**Category:** Plugins & Integrations
**Tags:** federation, multi-datasource, PPL, SQL, S3, Prometheus

---

Hi OpenSearch community 👋

I wanted to share a project I've been building called **Fuse** — a standalone federated query engine that lets you run a single query across multiple OpenSearch clusters, S3 data lakes, and Prometheus from within OpenSearch Dashboards.

## The Problem

OpenSearch Dashboards supports multiple data sources since v2.4, but each is an isolated silo. You can't:

- Run one query across 3 OpenSearch clusters and merge results
- JOIN OpenSearch logs with S3 archived data
- Build a dashboard panel that blends live metrics (Prometheus) with log data (OpenSearch)

## What Fuse Does

Fuse is a **separate Rust service** (not embedded in OSD's Node.js process) that sits between Dashboards and your datasources:

```
OSD Query Bar → Fuse Engine → [OpenSearch A, OpenSearch B, S3, Prometheus]
                                        ↓
                               Merged Results → OSD
```

**Example queries:**

```sql
-- Search across 3 clusters
SELECT * FROM cluster_a.logs, cluster_b.logs WHERE status = 500

-- Correlate OpenSearch logs with S3 archived audit trail
SELECT l.trace_id, a.user_id
FROM os_prod.logs AS l
JOIN s3_archive.audit AS a ON l.trace_id = a.trace_id
WHERE l.@timestamp > now() - 24h
```

```ppl
-- PPL multi-source
source = cluster_a.logs, cluster_b.logs
| where status >= 500
| stats count() by service
```

## Architecture

- **Query Parser**: extends PPL/SQL with `datasource.table` qualified references
- **DataFusion-based planner**: uses datafusion-federation for push-down optimization
- **Connector interface**: pluggable trait — OpenSearch, S3/Parquet, Prometheus connectors built-in
- **Result merger**: union, global sort/limit, hash-join across connector types
- **OSD Plugin**: TypeScript/React plugin that adds a federated query bar to Dashboards

## Current Status

The project is open source at https://github.com/seraphjiang/fuse and has a live playground at https://fuse.huanji.profile.aws.dev (Amazon VPN).

**What's working:**
- ✅ OpenSearch connector (full push-down: filter, projection, aggregation, sort, limit)
- ✅ S3/Parquet connector (column projection, S3 Select filter push-down)
- ✅ Prometheus connector (PromQL translation, range queries, rate/irate)
- ✅ DataFusion federation planner with SQL/PPL parsing
- ✅ REST API (`/api/fuse/query`, `/api/fuse/datasources`, `/api/fuse/health`)
- ✅ OSD plugin (query editor, results table, datasource selector, health indicator)
- ✅ Connector SDK for community-built connectors (`fuse-connector-sdk` on crates.io)
- ✅ Field-level security (deny/mask fields per datasource/role)
- ✅ Query result caching (TTL per connector type)
- ✅ Alerting (condition-based alerts on federated query results)

## Relationship to Existing Work

This complements rather than replaces existing components:

| Component | Relationship |
|-----------|-------------|
| Multi-datasource plugin | Extends — reuses datasource saved objects and auth |
| Federated PPL engine (sql#561) | Complements — adds UI-layer federation + more connectors |
| opensearch-spark | Integrates — can delegate heavy JOINs to Spark |
| Cross-cluster search | Supersedes for UI — no pre-config needed |

## Looking For

1. **Feedback on the connector interface** — is the `FederatedConnector` trait the right abstraction?
2. **Interest in contributing connectors** — CloudWatch Logs, DynamoDB, JDBC (PostgreSQL/MySQL)?
3. **Thoughts on OSD plugin integration** — should this be a standalone plugin or integrated into the existing query workbench?
4. **Performance testing** — anyone with multi-cluster setups willing to test?

Full proposal: https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705

Happy to answer questions or discuss the design!

— Huan
