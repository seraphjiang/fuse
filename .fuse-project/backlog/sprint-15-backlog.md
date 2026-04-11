# Sprint 15 Backlog — AI/ML, Ecosystem & Production Polish

**Sprint:** 15
**PM:** pm
**Status:** ✅ COMPLETE
**Theme:** AI-powered query experience, ecosystem SDKs, production hardening

## P0: AI/ML Integration

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1500 | Natural language to SQL (LLM-powered) | — | todo | Accept natural language, generate SQL via LLM, return query + results. Bedrock/OpenAI backend. |
| 1501 | Auto-suggest queries from schema | — | todo | Given a datasource + table, suggest useful queries (top N, aggregations, recent data) |
| 1502 | Query optimization advisor | — | todo | Analyze slow queries, suggest index hints, pushdown improvements, join reordering |

## P0: Ecosystem SDKs

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1510 | Go SDK client | — | todo | Deferred from Sprint 14. Full API surface: query, explain, async, health, datasources |
| 1511 | Python SDK client | — | todo | pip-installable, sync + async, pandas DataFrame integration |
| 1512 | Grafana datasource plugin | — | todo | Query Fuse from Grafana dashboards, variable support |

## P1: Production Hardening

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1520 | Multi-tenancy: tenant isolation | — | todo | Per-tenant datasource visibility, query limits, resource quotas |
| 1521 | Query governor: max rows/time/memory | — | todo | Configurable per-tenant limits, kill long-running queries |
| 1522 | Health dashboard in playground | — | todo | Real-time connector status, p50/p95/p99 latency, error rates, throughput |
| 1523 | Graceful shutdown with drain | — | todo | Drain in-flight queries before shutdown, configurable timeout |

## P1: Advanced Query

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1530 | Materialized views with refresh | — | todo | CREATE MATERIALIZED VIEW, scheduled refresh, incremental update |
| 1531 | Query plan visualization in playground | — | todo | Interactive tree/flame graph for EXPLAIN ANALYZE in web UI |
| 1532 | Cost-based federation routing | — | todo | Route queries to cheapest/fastest Fuse instance based on data locality |

## P2: Developer Experience

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1540 | VS Code extension | — | todo | SQL/PPL syntax highlighting, inline results, schema browser |
| 1541 | WASM plugin system | — | todo | Load custom connectors as WASM modules at runtime |
| 1542 | E2E test suite with docker-compose | — | todo | Spin up full stack (Fuse + OS + PG + DDB), run integration tests |

## P2: Cleanup & Tech Debt

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1550 | Fix api_test.rs compile errors | — | todo | 19 errors from cross-agent changes, needs reconciliation |
| 1551 | Cargo clippy clean | — | todo | Zero warnings across all crates |
| 1552 | README connector count sync | — | todo | Verify README matches actual connector count (22) |
