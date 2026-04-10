# Sprint 5 Backlog

**Sprint:** 5
**Start:** 2026-04-10
**Focus:** Production hardening, remaining compute depth, deploy all features live, new connectors, OSD plugin packaging

## P0: Production Hardening & Deploy

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 500 | Deploy Sprint 4 to playground (all 14 connectors + viz platform) | infra | todo | Full feature set live including dashboards |
| 501 | Auth/RBAC: API key authentication | explorer | done | x-api-key + Bearer, Role enum, public path bypass, 401 JSON. Commit: 50669df |
| 502 | Rate limiting per API key | explorer | done | PerKeyLimiter, 200 req/min default, global→IP→key chain. Commit: 84e6f33 |
| 503 | Query timeout enforcement | planner | todo | Cancel long-running queries after configurable timeout |
| 504 | Connection pooling for all connectors | general | todo | Reuse connections, health checks, pool size config |
| 505 | Graceful error handling for connector failures | planner | todo | Partial results when one connector fails in UNION ALL |

## P0: Carried from Sprint 4

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 411 | Top-N / Bottom-N with pushdown | planner | todo | ORDER BY count DESC LIMIT 10 pushed to connectors |
| 412 | Approximate aggregations (HyperLogLog, t-digest) | planner | todo | Pushdown to OpenSearch native approximate aggs |
| 423 | Saved queries / virtual views | planner | todo | CREATE VIEW unified_logs AS SELECT ... Virtual tables |
| 323 | Pagination across UNION ALL (global cursor) | explorer | done | Per-source cursor encoding, backward compatible. Commit: 9ae3506 |

## P1: New Connectors

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 510 | Amazon Redshift connector | general | todo | AWS native warehouse. SQL pushdown, IAM auth |
| 511 | DuckDB connector (local compute engine) | general | todo | In-process analytics, Arrow-native. Local data analysis |
| 512 | SQLite connector | general | todo | File-based, embedded. Good for local/edge deployments |

## P1: OSD Plugin & Integration

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 520 | OSD plugin packaging (npm publish-ready) | explorer | todo | package.json, build scripts, version compat matrix |
| 521 | OSD plugin: saved dashboards integration | frontend | done | Save/load/delete/export/import, localStorage. Commit: f64903a |
| 522 | OSD plugin: datasource picker UI | frontend | done | Two-pane browser, capability badges, schema preview, insert to query. Commit: e94b827 |
| 523 | OSD plugin: query history with replay | frontend | done | Recent + favorites, one-click replay, copy. Commit: e94b827 |

## P1: Compute Depth

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 413 | Recursive CTEs | planner | todo | Trace dependency chains across services |
| 422 | Anomaly detection primitives | planner | todo | Moving avg, stddev, z-score across time-bucketed data |
| 530 | Query result caching with TTL | planner | todo | Cache frequent queries, invalidate on TTL or schema change |
| 531 | Parallel connector health checks | infra | todo | Concurrent health pings, timeout per connector |
| 532 | RIGHT JOIN support | planner | todo | Complete the join type matrix |

## P2: Testing & Quality

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 540 | E2E test suite for visualization platform | tester | todo | Test chart rendering, dashboard save/load, export |
| 541 | Load testing: concurrent query stress test | tester | todo | 50+ concurrent queries, measure p50/p95/p99 latency |
| 542 | Connector integration tests against real services | tester | todo | Test against real PostgreSQL, Redis, DynamoDB (not mocks) |
| 543 | Security audit: SQL injection across all connectors | tester | todo | Fuzz test all filter pushdown paths |

## P2: Docs & Community

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 550 | Connector development guide (how to add a new connector) | docs | done | 6-step tutorial with checklist and reference table. Commit: 01b8f49 |
| 551 | Dashboard user guide with screenshots | docs | done | 12 chart types, auto-detection, variables, drill-down, templates, export. Commit: 8fea9d1 |
| 552 | Performance tuning guide | docs | done | 6 sections: pushdown, JOINs, caching, config, cost estimator, monitoring. Commit: 93277e7 |
| 553 | GitHub release v0.5.0 | infra | todo | Tag, release notes, binary artifacts |
