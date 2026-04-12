# Sprint 18 Backlog — v2.0 Features

**Sprint:** 18
**PM:** pm
**Status:** Complete
**Theme:** Scheduled queries, data quality, Arrow IPC, GraphQL — the v2.0 feature set

## P0: Core v2.0 Features

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1800 | Scheduled Queries (Cron) | ai-lead | done | 37c9e90 — Cron expression parser, job scheduler, result persistence. |
| 1801 | Data Quality Rules Engine | ai-lead | done | b20df6e — Null rate, cardinality, freshness, row count, unique rate checks. |
| 1802 | Arrow IPC Result Format | sde | done | batches_to_ipc, accepts_arrow, roundtrip support. |
| 1803 | Query Cost Estimation ($$$) | ai-lead | done | f2358bc — Per-connector cost models (Athena, DDB, S3, BigQuery, Snowflake, etc). |

## P1: Ecosystem

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1810 | GraphQL API | sde | done | async_graphql schema, query/mutation for CRUD. |
| 1811 | Webhook Subscriptions | sde | done | WebhookRegistry, event-driven monitoring. |
| 1812 | Query Replay & Regression Testing | pm | done | 539065e — GET/POST/DELETE /api/fuse/replay/* endpoints. |

## P1: Performance

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1820 | Parallel Fan-out with Backpressure | ai-lead | done | 611d7a6 — Semaphore-based concurrency, adaptive parallelism recording. |
| 1821 | Adaptive Query Caching | pm | done | 539065e — Frequency tracking, per-DS TTL, auto-promotion. |

## P1: AI/ML

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1830 | Query Explanation in Plain English | ai-lead | done | f19f3ec — Natural language query explanations. |
| 1831 | Schema Relationship Discovery | ai-lead+sde | done | 5f3e20e — Column name/type analysis, naming convention detection. |

## P2: Governance

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1840 | Query Lineage & Data Catalog | pm | done | 539065e — Graph extraction, store, catalog, POST /api/fuse/lineage. |
| 1841 | Multi-tenant SaaS Mode | pm | done | 539065e — Per-tenant query/rows/bytes tracking. |

## P2: Advanced

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1850 | OpenTelemetry Collector Mode | security | done | OTel connector, ingest traces/metrics/logs. |
| 1851 | Query Compilation | ai-lead | done | 611d7a6 — CompilationCache with fingerprint keying, TTL, eviction. |
| 1852 | Federated Materialized Views with CDC | sde | done | CdcTracker, change event ingestion, view refresh. |

## Sprint Summary

- **1106 lib tests passing**, 0 failures
- **17 items shipped** across 5 agents
- All features have unit tests
- Security review approved (#1820/#1851)
