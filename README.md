# Fuse 🔗

**Cross-Datasource Federated Query Engine for OpenSearch Dashboards**

Fuse is a standalone query engine that federates queries across multiple heterogeneous datasources — OpenSearch clusters, S3 data lakes, Prometheus, and more — from a single query interface in OpenSearch Dashboards.

> 🚧 **Status: Incubation / Proposal** — See the [full proposal](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705)

## Problem

OpenSearch Dashboards supports multiple data sources, but each is an isolated silo. You can't:

- Run a single query across multiple OpenSearch clusters and merge results
- JOIN data between OpenSearch and S3/Prometheus/JDBC sources
- Build dashboards that blend hot (OpenSearch) and cold (S3) data

## What Fuse Does

```sql
-- Search across 3 clusters
SEARCH logs FROM cluster_a, cluster_b, cluster_c WHERE status=500

-- Correlate OpenSearch logs with S3 archived data
SELECT l.trace_id, l.message, a.user_id
FROM os_prod.logs AS l
JOIN s3_archive.audit AS a ON l.trace_id = a.trace_id
WHERE l.@timestamp > now() - 24h

-- Mix metrics and logs
source = os_prod.app_logs
| where level = 'ERROR'
| stats count() by service_name
| join prometheus_prod.cpu_usage on service_name = exported_job
```

## Architecture

```
┌─────────────────────────────────┐
│  OpenSearch Dashboards (UI)     │
│  Query Bar / Dashboard Panels   │
└──────────────┬──────────────────┘
               │ API
┌──────────────▼──────────────────┐
│  Fuse - Federated Query Engine  │
│                                 │
│  Parser → Planner → Optimizer   │
│              │                  │
│    ┌─────────┼─────────┐       │
│    ▼         ▼         ▼       │
│  OpenSearch  S3/Glue  Prometheus│
│  Connector  Connector Connector │
│              │                  │
│         Result Merger           │
└─────────────────────────────────┘
```

Fuse runs as a **separate service** (not embedded in OSD's Node.js process) for performance, security, and independent scalability.

## Roadmap

| Phase | Focus | Timeline |
|-------|-------|----------|
| 1 | Same-type federation (OS ↔ OS) | 8 weeks |
| 2 | Cross-type federation (OS ↔ S3) + JOINs | 8 weeks |
| 3 | Prometheus connector + Connector SDK | 6 weeks |
| 4 | Caching, materialized views, alerting, RBAC | 8 weeks |

## Contributing

This project is in early incubation. Contributions, ideas, and feedback are welcome!

- 📋 [Proposal & Design Doc](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705)
- 💬 Open an issue to discuss

## License

This project is licensed under the Apache License 2.0 — see [LICENSE](LICENSE) for details.
