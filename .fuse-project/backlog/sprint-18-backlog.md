# Sprint 18 Backlog — v2.0 Features

**Sprint:** 18
**PM:** pm
**Status:** Draft
**Theme:** Scheduled queries, data quality, Arrow IPC, GraphQL — the v2.0 feature set

## P0: Core v2.0 Features

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1800 | Scheduled Queries (Cron) | ai-lead | todo | Run queries on schedule, store results, alert on changes. Builds on async_query + anomaly_alert. Cron expression parser, job scheduler, result persistence. |
| 1801 | Data Quality Rules Engine | ai-lead | todo | Define expectations per datasource (null rate, cardinality, freshness). Evaluate on schedule or per-query. Builds on anomaly detection. |
| 1802 | Arrow IPC Result Format | ai-lead | todo | Return Arrow IPC bytes instead of JSON for /api/fuse/query?format=arrow. Zero-copy for Python/Rust SDK consumers. |
| 1803 | Query Cost Estimation ($$$) | ai-lead | todo | Estimate real dollar cost before execution: Athena $/GB scanned, DDB $/RCU, S3 $/request. Per-connector cost model. |

## P1: Ecosystem

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1810 | GraphQL API | sde | todo | Alternative to REST. Schema introspection → datasource schemas. Query/mutation for CRUD. |
| 1811 | Webhook Subscriptions | sde | todo | POST callback when query result matches condition. Event-driven monitoring. |
| 1812 | Query Replay & Regression Testing | pm | done | Record production queries, replay against staging. Diff results. Builds on query_diff module. |

## P1: Performance

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1820 | Parallel Fan-out with Backpressure | ai-lead | todo | Fast connectors stream results immediately. Slow connectors don't block. Tokio select + channel buffering. |
| 1821 | Adaptive Query Caching | pm | done | Learn repeat patterns, auto-cache with per-datasource TTL. Builds on plan_cache + result_cache. |

## P1: AI/ML

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1830 | Query Explanation in Plain English | ai-lead | todo | "This query joins error logs with user profiles..." Builds on NL module, reverse direction. |
| 1831 | Schema Relationship Discovery | sde | todo | Auto-detect foreign keys by column name + value overlap analysis. |

## P2: Governance

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1840 | Query Lineage & Data Catalog | pm | done | Track data flow across connectors. Per-query lineage graph. |
| 1841 | Multi-tenant SaaS Mode | pm | done | Usage metering, billing integration, tenant isolation. |

## P2: Advanced

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1850 | OpenTelemetry Collector Mode | security | todo | Fuse as OTel backend — ingest traces/metrics/logs, query with SQL. |
| 1851 | Query Compilation | ai-lead | todo | Compile hot query patterns to skip parsing/planning. |
| 1852 | Federated Materialized Views with CDC | sde | todo | Auto-refresh when source data changes. |
