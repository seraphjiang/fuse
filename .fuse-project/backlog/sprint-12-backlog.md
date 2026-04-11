# Sprint 12 Backlog — Query Intelligence & Security Hardening

**Sprint:** 12
**Start:** 2026-04-11
**PM:** pm
**Focus:** Materialized view syntax, EXPLAIN ANALYZE, security hardening, query visualization

## P0: Materialized Views (sde)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 820 | CREATE MATERIALIZED VIEW syntax | sde | done | Parse, store, execute on create. Commit: fd58fcb |
| 821 | REFRESH MATERIALIZED VIEW | sde | done | Re-execute and replace cached result. Commit: fd58fcb |

## P0: Query Intelligence (ai-lead)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 900 | EXPLAIN ANALYZE with execution stats | ai-lead | done | execution_profile, per-node timing/rows/bytes, 5 new tests. Commit: d871977 |
| 903 | Prepared statements with parameter binding | ai-lead | done | PREPARE/EXECUTE, positional $N binding, safe escaping, 19 tests. Commit: 54981ad |
| 940 | EXPLAIN ANALYZE accuracy tests | fee | done | 6 tests validating profile accuracy. Commit: 1578611 |

## P1: Security Hardening (security → reassigned)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 920 | TLS/mTLS for connector connections | security | unresponsive | rustls, per-connector cert config — TlsConfig exists in tls.rs |
| 921 | RBAC fine-grained permissions | pm | done | DatasourceRbac with Read/Write/Admin, 8 tests. Commit: 823dad6 |
| 922 | Secret management (connector credentials) | ai-lead | done | SecretResolver trait, validate_secret_refs(), 14 tests. Commit: bfff2e2 |
| 1100 | Connection pooling per connector | ai-lead | in-progress | Verify + add pooling where missing |

## P1: Continuous Monitoring (pm)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 910 | Anomaly detection alerts (continuous) | pm | done | AlertMonitor, webhook, background loop, 9 tests. Commit: 61bf654 |
| 911 | Alert rules management API | pm | done | CRUD handlers, 5 tests. Commit: 486819f |

## P1: Horizontal Scaling (sde)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 800 | Stateless server mode | sde | done | SharedSavedQueries/History/AuditLog, Redis+fallback, 16 tests. Commits: aa0b01a→d7c5259 |
| 802 | Shared tenant registry | sde | in-progress | Redis/config hot-reload |

## P1: Frontend — Query Visualization (fee)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 902 | Query plan visualization in playground | fee | done | Visual DAG of execution plan nodes |
| 901 | Flame graph visualization for EXPLAIN ANALYZE | fee | done | Interactive flame graph in playground |
| 912 | Alert history filters | fee | done | Status + search filters for alert timeline |
| 1150 | Dark mode | fee | done | Already implemented, verified across 12 pages |
| 1151 | Mobile responsive layout | fee | done | Responsive breakpoints for tablet/phone |
| 1010 | VS Code extension | fee | done | Verified + 2 bug fixes (port, PPL lookup). Commit: eb506b1 |

## P1: GA Readiness (pm)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1103 | Structured error codes (FUSE-XXXX) | pm | done | ErrorCode type, error_code() methods, 14 new tests. Commit: fc7e1bf |
| 1102 | Configuration validation on startup | pm | done | validate() on FuseConfig, fail-fast, 8 new tests. Commit: e68a5c0 |
| 841 | Kafka integration test compilation fix | pm | done | 6 tests compile+pass, dev-deps added. Commit: cb1085c |
| 1113 | Adaptive query timeout | pm | done | Module + wiring. Commits: 47ac395, d623d4d |
| 831 | Plugin manifest + discovery | pm | done | PluginManifest, subdirectory layout, 7 tests. Commit: 0c79214 |

## Already Done (verified per Rule 15)

| ID | Item | Status | Notes |
|----|------|--------|-------|
| 902 | Query plan visualization | done | Already implemented, verified by fee. Commit: 3051535 |
| 1101 | Graceful shutdown with drain | done | Already in main.rs — 10s grace, cancel_all |
| 931 | Fix MongoDB compile issues | done | Compiles clean, wired in main.rs (8f34729) |
| 932 | Fix InfluxDB compile issues | done | Compiles clean, wired in main.rs (8f34729) |
| 950 | Query optimization guide | done | docs/guides/query-optimization-guide.md exists |
| 951 | Security hardening guide | done | docs/guides/security-hardening-guide.md exists |
| 1110 | Parallel connector execution | done | tokio::spawn fan-out already in api.rs |
| 1140 | v1.0.0 announcement blog | done | docs-site/src/blog-v100-ga.md exists |
| 1141 | API stability guarantee doc | done | docs-site/src/api-stability.md exists |
| 1142 | Deployment patterns guide | done | docs-site/src/deployment-patterns.md exists |
| 1150 | Dark mode | done | Theme toggle + light/dark CSS in index.html |
| 1152 | Keyboard shortcuts | done | Ctrl+Enter, Ctrl+Shift+E, Ctrl+S, Ctrl+Shift+V |
