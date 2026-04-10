# Sprint 6 Backlog

**Sprint:** 6
**Start:** TBD
**Focus:** AI integration, multi-tenancy, remaining connectors, community readiness

## P0: Carried from Sprint 5

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 412 | Approximate aggregations (HyperLogLog, t-digest) | explorer | done | Shipped in Sprint 5 (commit e4b25bf) |
| 510 | Amazon Redshift connector | general | todo | AWS native warehouse. SQL pushdown, IAM auth |
| 511 | DuckDB connector (local compute engine) | general | todo | In-process analytics, Arrow-native |
| 512 | SQLite connector | general | todo | File-based, embedded |
| 542 | Connector integration tests against real services | tester | done | 21/21 green against live playground. Commit: bd7f382 |

## P0: Security Fix

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 600 | Fix InfluxDB injection vulnerability | general | todo | Add quote escaping to InfluxQL + Flux filter paths |
| 601 | Fix Prometheus injection vulnerability | general | todo | Escape single quotes in PromQL label values |

## P1: AI Integration

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 610 | Natural language to SQL (LLM-powered) | planner | done | POST /api/fuse/nl, provider-agnostic, rule-based fallback, execute mode. Commit: f79ed1f |
| 611 | Auto-suggest queries based on schema | frontend | done | Contextual suggestions based on column types. Commit: 6f19619 |
| 612 | Intelligent query optimization (learn from history) | planner | done | GET /api/fuse/advisor, 4 categories: missing_limit, high_error, cache_opportunity, missing_filter. Commit: 64c9db6 |

## P1: Multi-Tenancy & Enterprise

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 620 | Multi-tenant isolation | explorer | done | TenantConfig, TenantRegistry, datasource filtering, secure default. Commit: 2026634 |
| 621 | Query governor (max rows, max time, max memory) | planner | done | Per-tenant max_rows, max_time_ms, max_result_bytes. Commit: a534b51 |
| 622 | Audit logging (who queried what, when) | explorer | done | AuditEntry, 8 action types, bounded FIFO, identity tracking. Commit: 9f1e42e |

## P1: Ecosystem & Integration

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 630 | Python SDK client | explorer | done | FuseClient, auto-paginate, trace, zero deps, Python >=3.8. 9 tests |
| 631 | Grafana datasource plugin | frontend | done | plugin.json, config/query editors, auto field types, health check. Commit: 6f19619 |
| 632 | Jupyter notebook integration | docs | done | SDK setup, DataFrame helpers, visualization, pagination, trace. Commit: 986b92d |

## P2: Advanced Connectors

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 640 | Apache Kafka connector (streaming) | general | todo | Consume topics, filter by key/timestamp |
| 641 | Amazon Timestream connector | general | todo | Time-series, InfluxQL-compatible |
| 642 | Snowflake connector | general | todo | OAuth/key-pair auth, SQL pushdown |

## P2: Testing & Quality

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 650 | Chaos testing (connector failures, network partitions) | tester | todo | Verify graceful degradation under failure |
| 651 | Performance regression CI (automated benchmark on every commit) | tester | todo | Catch perf regressions before deploy |
| 652 | Accessibility audit (WCAG 2.1 AA) | frontend | done | ARIA landmarks, skip-to-content, focus-visible, aria-live, role=alert. Commit: a572da1 |

## P2: Docs & Community

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 660 | Video recording of demo script | docs | done | v3 script, 9 scenes, ~4:30, covers all v0.5.0 features. Commit: 92f932e |
| 661 | Community Discord/Slack setup | docs | done | Community guide, templates, SUMMARY.md 25 pages in 6 sections. Commit: 4f28364 |
| 662 | Roadmap page on docs-site | docs | done | Shipped/In Progress/Planned/Community sections. Commit: 05a46d9 |
