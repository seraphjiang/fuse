# Sprint 14 Backlog — Polish, Performance & Ecosystem

**Sprint:** 14
**PM:** pm
**Status:** Draft — awaiting user approval

## P0: Production Hardening

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1400 | CORS configuration (engine.cors in fuse.toml) | — | todo | Security audit finding — needed for separate frontend deployments |
| 1401 | Graceful connector reconnection | — | todo | Auto-retry on transient connection failures |
| 1402 | Health check aggregation across federation | — | todo | /api/fuse/health includes remote instance health |
| 1403 | Query result size limits | — | todo | Configurable max_result_bytes to prevent OOM |

## P1: Write Path Connectors

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1410 | Postgres write_batches implementation | — | todo | INSERT via sqlx, batch mode |
| 1411 | DuckDB write_batches implementation | — | todo | INSERT via duckdb crate |
| 1412 | S3 write_batches (Parquet output) | — | todo | Write query results as Parquet to S3 |

## P1: Ecosystem

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1420 | Go SDK client | — | todo | Same API surface as Python/TypeScript |
| 1421 | Jupyter magic command (%fuse) | — | todo | Native notebook integration |
| 1422 | OpenSearch Dashboards plugin update | — | todo | Sync OSD plugin with v1.1 API |

## P1: Observability

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1430 | Distributed tracing (OpenTelemetry) | — | todo | Trace queries across federated instances |
| 1431 | Query audit log export (S3/CloudWatch) | — | todo | Ship audit logs to external storage |

## P2: Performance

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1440 | Predicate pushdown for Arrow Flight | — | todo | Push filters to Flight SQL endpoints |
| 1441 | Adaptive parallelism | — | todo | Auto-tune fan-out concurrency per datasource |
| 1442 | Query plan caching across sessions | — | todo | Reuse plans for identical query patterns |
