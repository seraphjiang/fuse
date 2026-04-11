# Sprint 9 Backlog — Query Intelligence & Observability

**Sprint:** 9
**Start:** 2026-04-11
**Focus:** EXPLAIN ANALYZE, query plan visualization, anomaly alerts, prepared statements, TLS

## P0: Query Intelligence

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 900 | EXPLAIN ANALYZE with execution stats | planner | todo | Per-node timing, rows scanned, bytes transferred |
| 901 | Flame graph visualization for EXPLAIN ANALYZE | frontend | todo | Interactive flame graph in playground |
| 902 | Query plan visualization in playground | frontend | todo | Visual DAG of execution plan nodes |
| 903 | Prepared statements with parameter binding | planner | todo | PREPARE/EXECUTE, prevent SQL injection at protocol level |

## P1: Continuous Monitoring

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 910 | Anomaly detection alerts (continuous) | explorer | todo | Background monitor, configurable thresholds, webhook/email |
| 911 | Alert rules management API | explorer | todo | CRUD for alert rules, silence, acknowledge |
| 912 | Alert history + dashboard | frontend | todo | Show triggered alerts, timeline, acknowledge UI |

## P1: Security Hardening

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 920 | TLS/mTLS for connector connections | planner | todo | rustls, per-connector cert config |
| 921 | RBAC: fine-grained permissions | explorer | todo | Per-datasource read/write, per-tenant admin |
| 922 | Secret management (connector credentials) | infra | todo | AWS Secrets Manager integration, no plaintext |

## P1: Connectors

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 930 | Google BigQuery connector | general | todo | Service account auth, SQL pushdown |
| 931 | Fix MongoDB connector compile issues | general | todo | Unblock wiring in main.rs |
| 932 | Fix InfluxDB connector compile issues | general | todo | Unblock wiring in main.rs |

## P2: Testing & Docs

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 940 | EXPLAIN ANALYZE accuracy tests | tester | todo | Verify timing/row counts match actual execution |
| 941 | Prepared statement injection tests | tester | todo | Verify parameterized queries prevent injection |
| 942 | TLS connectivity tests | tester | todo | Verify mTLS handshake with test certs |
| 950 | Query optimization guide | docs | todo | How to read EXPLAIN output, optimization tips |
| 951 | Security hardening guide | docs | todo | TLS setup, RBAC config, secret management |
