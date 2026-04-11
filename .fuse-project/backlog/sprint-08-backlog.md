# Sprint 8 Backlog — Horizontal Scaling & Streaming

**Sprint:** 8
**Start:** 2026-04-11
**Focus:** Horizontal scaling, Kafka connector, materialized views, WASM plugin system

## P0: Horizontal Scaling

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 800 | Stateless server mode (externalize session/cache to Redis) | planner | todo | Remove in-memory state, use Redis for cache + tenant registry |
| 801 | Redis-backed query result cache | explorer | todo | Replace in-memory LruCache with Redis, TTL support |
| 802 | Shared tenant registry (Redis/config file) | planner | todo | Load tenants from external source, hot-reload |
| 803 | Docker Compose multi-instance setup | infra | done | 2 Fuse + nginx LB + Redis + OpenSearch. Commit: c0491ca |

## P1: Streaming & New Connectors

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 640 | Apache Kafka connector | explorer | todo | Consume topics, filter by key/timestamp/offset, JSON/Avro |
| 641 | Amazon Timestream connector | general | todo | Time-series queries, SQL pushdown |
| 642 | Snowflake connector | general | todo | OAuth/key-pair auth, SQL pushdown, warehouse selection |
| 810 | Apache Cassandra connector | general | todo | CQL, partition-aware queries |

## P1: Materialized Views

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 820 | CREATE MATERIALIZED VIEW syntax | planner | todo | Parse, store definition, execute on create |
| 821 | REFRESH MATERIALIZED VIEW | planner | todo | Re-execute and replace cached result |
| 822 | Auto-refresh scheduler (cron-based) | explorer | todo | Background task, configurable interval |

## P1: Plugin System

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 830 | WASM connector plugin runtime | explorer | todo | wasmtime, load .wasm, FederatedConnector interface |
| 831 | Plugin manifest + discovery | planner | todo | plugins/ dir, manifest.toml, auto-register |

## P2: Testing & Docs

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 840 | Horizontal scaling load test (multi-instance) | tester | todo | Verify no state leakage across instances |
| 841 | Kafka connector integration test | tester | todo | Produce + consume + query |
| 842 | Materialized view lifecycle test | tester | todo | Create, query, refresh, drop |
| 850 | Horizontal scaling guide | docs | done | Architecture diagram, Docker Compose, Prometheus, troubleshooting. Commit: e6bb339 |
| 851 | Plugin development guide | docs | done | WASM lifecycle, SDK types, manifest, sandbox. Commit: e6bb339 |

## P2: Frontend

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 860 | Materialized views management page | frontend | done | List/create/refresh/drop views, fresh/stale badges. Commit: d976ac3 |
| 861 | Plugin management page | frontend | done | WASM upload, enable/disable, graceful fallback. Commit: d976ac3 |
