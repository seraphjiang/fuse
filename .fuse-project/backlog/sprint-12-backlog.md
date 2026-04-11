# Sprint 12 Backlog — Query Intelligence & Security Hardening

**Sprint:** 12
**Start:** 2026-04-11
**PM:** pm
**Focus:** Materialized view syntax, EXPLAIN ANALYZE, security hardening, query visualization

## P0: Materialized Views (sde)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 820 | CREATE MATERIALIZED VIEW syntax | sde | in-progress | Parse, store definition, execute on create |
| 821 | REFRESH MATERIALIZED VIEW | sde | todo | Re-execute and replace cached result |

## P0: Query Intelligence (ai-lead)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 900 | EXPLAIN ANALYZE with execution stats | ai-lead | in-progress | Per-node timing, rows scanned, bytes transferred |
| 903 | Prepared statements with parameter binding | ai-lead | todo | PREPARE/EXECUTE, prevent SQL injection |

## P1: Security Hardening (security)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 920 | TLS/mTLS for connector connections | security | in-progress | rustls, per-connector cert config |
| 922 | Secret management (connector credentials) | security | todo | AWS Secrets Manager, no plaintext |

## P1: Frontend — Query Visualization (fee)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 902 | Query plan visualization in playground | fee | done | Visual DAG of execution plan nodes |
| 901 | Flame graph visualization for EXPLAIN ANALYZE | fee | done | Interactive flame graph in playground |

## P1: GA Readiness (pm)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1103 | Structured error codes (FUSE-XXXX) | pm | done | ErrorCode type, error_code() methods, 14 new tests. Commit: fc7e1bf |
| 1102 | Configuration validation on startup | pm | done | validate() on FuseConfig, fail-fast, 8 new tests. Commit: e68a5c0 |
| 841 | Kafka integration test compilation fix | pm | done | 6 tests compile+pass, dev-deps added. Commit: cb1085c |
| 1113 | Adaptive query timeout | pm | done | Per-datasource p95*3x, 10 tests. Commit: 47ac395 |

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
