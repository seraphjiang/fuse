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
