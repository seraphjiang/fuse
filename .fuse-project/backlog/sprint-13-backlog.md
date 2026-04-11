# Sprint 13 Backlog — Federation, Write Path & New Connectors

**Sprint:** 13
**Start:** 2026-04-11
**PM:** pm
**Status:** ✅ COMPLETE

## P0: Federation (COMPLETE)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1000 | Fuse-to-Fuse connector | ai-lead | done | REST forwarding, 11 tests. Commit: 3abef2e |
| 1001 | Cross-cluster query routing | ai-lead | done | find_owner(), resolve_route(), 5 tests. Commit: 5eb9885 |
| 1002 | Federation topology API | pm | done | FederationRegistry, 7 tests. Commit: 02d4250 |
| 1002b | Federation topology page | fee | done | Canvas diagram, stats. Commit: 5fc82b2 |
| 1030 | Federation integration tests | pm | done | 10 test cases. Commit: f663eeb |

## P0: Write Path (COMPLETE)

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1020 | CREATE TABLE AS SELECT | sde | done | write_batches() trait. Commit: 8bd3dfe |
| 1021 | INSERT INTO ... SELECT | sde | done | Commit: 8bd3dfe |
| 1022 | Transaction support | sde | done | BEGIN/COMMIT/ROLLBACK, 15 tests. Commit: 0390f41 |
| — | Write path E2E tests | pm | done | 14 test cases. Commit: 093d1f4 |

## P1: New Connectors

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1051 | Amazon Athena | ai-lead | done | SQL pushdown, Glue catalog, 11 tests. Commit: dbeaf61 |
| 641 | Amazon Timestream | ai-lead | done | SQL pushdown, type-aware Arrow, 10 tests. Commit: cfa7545 |
| 642 | Snowflake | ai-lead | done | SQL API, async polling, 9 tests. Commit: 9c744c0 |
| 930 | Google BigQuery | ai-lead | done | Jobs API, backtick quoting, 8 tests. Commit: 971c3dc |
| 810 | Apache Cassandra | sde | in-progress | CQL, partition-aware |

## P1: Performance

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1111 | Arrow Flight | ai-lead | in-progress | Zero-copy RecordBatch streaming |

## Security

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| — | Security audit | security | done | Error leakage fix, full audit report. Commit: 6164f7c |
| — | Identifier quoting hardening | security | in-progress | SQL pushdown connectors |
| 920 | TLS/mTLS (Sprint 12 carryover) | security | done | CertificateValidation fix |
| 922 | Recursive secret resolution | security | done | Nested TOML tables. Commit: 4a3f61a |

## Docs

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| — | README update (20 connectors) | pm | done | Commit: d2ef6cc |

## Scoreboard

| Agent | Sprint 12 | Sprint 13 | Total |
|-------|-----------|-----------|-------|
| pm | 10 | 4 | 14 |
| fee | 12 | 1 | 13 |
| ai-lead | 4 | 10 | 14 |
| sde | 4 | 3+wip | 7+ |
| security | 1 | 3 | 4 |
| **Total** | **31** | **21+** | **52+** |
