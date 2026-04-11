# Sprint 16 Backlog — Final Polish, Testing & Roadmap Cleanup

**Sprint:** 16
**PM:** pm
**Status:** Active
**Theme:** Ship remaining backlog, live site testing, production readiness

## P0: Live Site Testing

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1600 | End-to-end live site smoke test | fee | todo | Hit every endpoint on fuse.huanji.profile.aws.dev — query, explain, health, datasources, schemas, fields, history, trace, federation, stats, saved queries, async query. Report failures. |
| 1601 | Cross-datasource JOIN test on live site | sde | todo | Test OS+DDB join, OS+S3 union, 3-way union. Verify results correctness. |
| 1602 | Playground UI regression test | fee | todo | Test all playground pages: query editor, explore, federation, settings, status, alerts, dashboard, terminal. Screenshot any broken UI. |
| 1603 | Load test: 50 concurrent queries | pm | todo | Run bench script against live site. Report p50/p95/p99 latency, error rate, throughput. |

## P0: Remaining Sprint 15 Items

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1501 | Auto-suggest queries from schema | ai-lead | todo | Given datasource+table, suggest useful queries (top N, aggregations, recent) |
| 1551 | Cargo clippy clean | security | todo | Zero warnings across all crates |
| 1552 | README connector count sync | fee | todo | Verify README matches actual 22 connectors |

## P1: Roadmap — AI/ML

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1610 | Anomaly detection alerts | ai-lead | todo | Continuous monitoring: detect unusual patterns in query results, trigger alerts |
| 1611 | Query auto-complete in playground | fee | todo | Schema-aware SQL/PPL autocomplete in query editor |

## P1: Roadmap — Production

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1620 | TLS/mTLS for connector connections | security | todo | Configurable TLS for PG, MySQL, MongoDB, ClickHouse connectors |
| 1621 | Health dashboard metrics export (Prometheus) | pm | todo | /metrics endpoint with query latency histograms, connector status, cache hit rates |

## P1: Roadmap — Ecosystem

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1630 | Jupyter notebook integration | sde | todo | %fuse magic command, DataFrame output, cell-level query execution |
| 1631 | REST API SDK — JavaScript/TypeScript | sde | todo | npm package, typed client, async/await |

## P2: Tech Debt

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1640 | Fix 4 remaining api_test failures | ai-lead | todo | test_connector_error, test_governor_limits, test_tenant_forbidden, test_timeout |
| 1641 | Squash commits for clean history | pm | todo | 50+ commits on main — squash into logical feature groups before any upstream push |
