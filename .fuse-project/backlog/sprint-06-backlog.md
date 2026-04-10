# Sprint 6 Backlog

**Sprint:** 6
**Start:** TBD
**Focus:** AI integration, multi-tenancy, remaining connectors, community readiness

## P0: Carried from Sprint 5

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 412 | Approximate aggregations (HyperLogLog, t-digest) | explorer | todo | Pushdown to OpenSearch native approximate aggs |
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
| 610 | Natural language to SQL (LLM-powered) | planner | todo | Schema-aware NL→SQL translation via Bedrock/OpenAI |
| 611 | Auto-suggest queries based on schema | frontend | todo | Suggest interesting queries when user selects a datasource |
| 612 | Intelligent query optimization (learn from history) | planner | todo | Analyze past queries to suggest index/pushdown improvements |

## P1: Multi-Tenancy & Enterprise

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 620 | Multi-tenant isolation | explorer | done | TenantConfig, TenantRegistry, datasource filtering, secure default. Commit: 2026634 |
| 621 | Query governor (max rows, max time, max memory) | planner | todo | Per-tenant resource limits |
| 622 | Audit logging (who queried what, when) | explorer | done | AuditEntry, 8 action types, bounded FIFO, identity tracking. Commit: 9f1e42e |

## P1: Ecosystem & Integration

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 630 | Python SDK client | explorer | todo | pip install fuse-client, query(), stream(), trace() |
| 631 | Grafana datasource plugin | frontend | todo | Fuse as a Grafana datasource for existing Grafana users |
| 632 | Jupyter notebook integration | docs | todo | %fuse magic command, DataFrame output |

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
| 652 | Accessibility audit (WCAG 2.1 AA) | frontend | todo | Screen reader, keyboard nav, contrast ratios |

## P2: Docs & Community

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 660 | Video recording of demo script | docs | done | v3 script, 9 scenes, ~4:30, covers all v0.5.0 features. Commit: 92f932e |
| 661 | Community Discord/Slack setup | docs | todo | Community channel for users and contributors |
| 662 | Roadmap page on docs-site | docs | done | Shipped/In Progress/Planned/Community sections. Commit: 05a46d9 |
