# Sprint 13 Backlog — Federation, Write Path & New Connectors

**Sprint:** 13
**Start:** 2026-04-11
**PM:** pm
**Focus:** Fuse-to-Fuse federation, write path (CTAS/INSERT/transactions), new connectors

## P0: Federation

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1000 | Fuse-to-Fuse connector | ai-lead | done | REST forwarding, schema discovery, 11 tests. Commit: 3abef2e |
| 1001 | Cross-cluster query routing | ai-lead | in-progress | Wire connector + registry into planner |
| 1002 | Federation topology page | fee | done | Canvas diagram, stats, fallback. Commit: 5fc82b2 |

## P0: Write Path

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1020 | CREATE TABLE AS SELECT (CTAS) | sde | done | write_batches() trait, parser, handler. Commit: 8bd3dfe |
| 1021 | INSERT INTO ... SELECT | sde | done | Commit: 8bd3dfe |
| 1022 | Transaction support (BEGIN/COMMIT/ROLLBACK) | sde | in-progress | Per-session state, buffered writes |

## P1: New Connectors

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 641 | Amazon Timestream connector | — | todo | Time-series, SQL pushdown |
| 642 | Snowflake connector | — | todo | OAuth/key-pair auth, SQL pushdown |
| 810 | Apache Cassandra connector | — | todo | CQL, partition-aware queries |
| 930 | Google BigQuery connector | — | todo | Service account auth, SQL pushdown |
| 1050 | Apache Spark connector | — | todo | Spark SQL, Thrift/Arrow Flight |
| 1051 | Amazon Athena connector | — | todo | SQL pushdown, S3 results |

## P1: Performance

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1111 | Arrow Flight for data transfer | — | todo | Zero-copy between connectors |

## P2: Testing

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1030 | Federation integration tests | — | todo | 2 Fuse instances, cross-cluster JOIN |
