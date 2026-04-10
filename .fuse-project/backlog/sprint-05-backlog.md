# Sprint 5 Backlog

**Sprint:** 5
**Start:** 2026-04-10
**Focus:** Production hardening, remaining compute depth, deploy all features live, new connectors, OSD plugin packaging

## P0: Production Hardening & Deploy

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 500 | Deploy Sprint 4 to playground (all 14 connectors + viz platform) | infra | done | /playground + /dashboard + /explore live. 6 connectors healthy. Pipeline ec1cce12 |
| 501 | Auth/RBAC: API key authentication | explorer | done | x-api-key + Bearer, Role enum, public path bypass, 401 JSON. Commit: 50669df |
| 502 | Rate limiting per API key | explorer | done | PerKeyLimiter, 200 req/min default, global→IP→key chain. Commit: 84e6f33 |
| 503 | Query timeout enforcement | planner | done | Per-connector 25s timeout on UNION ALL + JOIN tasks. Commit: 5ebf648 |
| 504 | Connection pooling for all connectors | general | done | bb8 pool for Redis, pool config helpers, SDK-pooled connectors noted. Commit: 09052fc |
| 505 | Graceful error handling for connector failures | planner | done | Already implemented — partial_errors + merge successful results |

## P0: Carried from Sprint 4

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 411 | Top-N / Bottom-N with pushdown | planner | done | ORDER BY + LIMIT pushed to sources, min(base, existing). Commit: 62b87c0 |
| 412 | Approximate aggregations (HyperLogLog, t-digest) | planner | todo | Pushdown to OpenSearch native approximate aggs |
| 423 | Saved queries / virtual views | planner | done | CREATE VIEW via SQL + REST API, MaterializedViewRegistry. Commit: ded5492 |
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
| 520 | OSD plugin packaging (npm publish-ready) | explorer | done | package.json, peer deps, barrel exports, README. Commit: 3d52a01 |
| 521 | OSD plugin: saved dashboards integration | frontend | done | Save/load/delete/export/import, localStorage. Commit: f64903a |
| 522 | OSD plugin: datasource picker UI | frontend | done | Two-pane browser, capability badges, schema preview, insert to query. Commit: e94b827 |
| 523 | OSD plugin: query history with replay | frontend | done | Recent + favorites, one-click replay, copy. Commit: e94b827 |

## P1: Compute Depth

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 413 | Recursive CTEs | explorer | done | WITH RECURSIVE, base+recursive step, 100 iteration safety. Commit: c307ec9 |
| 422 | Anomaly detection primitives | explorer | done | moving_avg, stddev, z_scores, detect_anomalies, annotate_batch. Commit: 5dfa8f3 |
| 530 | Query result caching with TTL | planner | done | 60s TTL, 500 entries, format:query key. Commit: fccc544 |
| 531 | Parallel connector health checks | infra | done | 5s per-connector timeout, unhealthy on hang. Commit: 4d55226 |
| 532 | RIGHT JOIN support | planner | done | Swap sides + delegate to LEFT. 6 join types complete. Commit: cfda8ea |

## P2: Testing & Quality

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 540 | E2E test suite for visualization platform | tester | done | 6 E2E tests: playground, dashboard, explore, chart format, saved queries. Commit: fec077f |
| 541 | Load testing: concurrent query stress test | tester | done | 100 concurrent, p99 <100ms, 0% errors. Commit: 2563560 |
| 542 | Connector integration tests against real services | tester | todo | Test against real PostgreSQL, Redis, DynamoDB (not mocks) |
| 543 | Security audit: SQL injection across all connectors | tester | done | 6 fuzz tests × 13 payloads. 2 vulns found (InfluxDB, Prometheus). Commit: 16f4aff |

## P2: Docs & Community

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 550 | Connector development guide (how to add a new connector) | docs | done | 6-step tutorial with checklist and reference table. Commit: 01b8f49 |
| 551 | Dashboard user guide with screenshots | docs | done | 12 chart types, auto-detection, variables, drill-down, templates, export. Commit: 8fea9d1 |
| 552 | Performance tuning guide | docs | done | 6 sections: pushdown, JOINs, caching, config, cost estimator, monitoring. Commit: 93277e7 |
| 553 | GitHub release v0.5.0 | infra | done | Tag pushed to GitHub + CodeCommit. release.yml triggered |
