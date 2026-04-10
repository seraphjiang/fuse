# Sprint 7 Backlog

**Sprint:** 7
**Start:** 2026-04-10
**Focus:** Remaining connectors, final deploy, polish, community readiness

## P0: Connectors (Carried — Reassigned)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 510 | Amazon Redshift connector | general | todo | SQL pushdown, IAM auth |
| 511 | DuckDB connector (local compute engine) | general | todo | In-process, Arrow-native |
| 512 | SQLite connector | general | todo | File-based, embedded |

## P1: Advanced Connectors

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 640 | Apache Kafka connector (streaming) | general | todo | Consume topics, filter by key/timestamp |
| 641 | Amazon Timestream connector | general | todo | Time-series, SQL pushdown |
| 642 | Snowflake connector | general | todo | OAuth/key-pair auth, SQL pushdown |

## P1: Final Deploy & Release

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 700 | Deploy Sprint 6 features to playground | infra | todo | Enterprise stack, AI features, SDKs |
| 701 | GitHub release v0.6.0 | infra | todo | Tag, release notes |
| 702 | Publish docs-site updates | infra | todo | mdbook build + GitHub Pages |
| 703 | Publish Python SDK to PyPI | explorer | done | fuse-client@0.5.0, .whl + .tar.gz, py.typed. Commit: 063aca6 |
| 704 | Publish TypeScript SDK to npm | explorer | done | @fuse-query/client@0.5.0, dual CJS/ESM. Ready for npm publish |

## P1: Polish & Integration

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 710 | Wire multi-tenancy into query handler | planner | todo | Connect #620 TenantRegistry to actual query execution |
| 711 | Wire audit logging into all endpoints | planner | todo | Connect #622 AuditLog to middleware |
| 712 | Wire query governor into execution | planner | todo | Connect #621 QueryGovernor to query handler |
| 713 | Anomaly detection in playground UI | frontend | done | z-score markers, moving avg overlay, toggle. Commit: 9f9dc72 |
| 714 | Query advisor in playground UI | frontend | done | Color-coded recommendations, clickable queries. Commit: 9f9dc72 |

## P2: Testing

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 720 | Verify enterprise stack end-to-end | tester | done | 13 tests: multi-tenancy, governor, auth, audit, rate limit, isolation. Commit: 9d379e1 |
| 721 | SDK integration tests (Python + TypeScript) | tester | todo | Test SDKs against live playground |
| 722 | Grafana plugin verification | tester | todo | Test Grafana plugin against live Fuse |

## P2: Docs

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 730 | Release notes v0.6.0 blog post | docs | done | Enterprise features, AI, SDKs, by-the-numbers. Commit: 027ae76 |
| 731 | SDK comparison guide (Python vs TypeScript) | docs | done | Comparison table, use cases, API parity, side-by-side examples. Commit: 565997a |
