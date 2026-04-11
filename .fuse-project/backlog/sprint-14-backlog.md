# Sprint 14 Backlog — Polish, Performance & Ecosystem

**Sprint:** 14
**PM:** pm
**Status:** ✅ COMPLETE

## P0: Production Hardening — ✅ COMPLETE

| ID | Item | Owner | Status | Commit |
|----|------|-------|--------|--------|
| 1400 | CORS configuration | pm | ✅ done | 06e412e |
| 1401 | Graceful connector retry | pm | ✅ done | 2fcac2f |
| 1402 | Federation health aggregation | pm | ✅ done | 02a417c |
| 1403 | Query result size limits | pm+sde | ✅ done | 5e70ea5, 2cb5ef1 |

## P1: Write Path Connectors — ✅ COMPLETE

| ID | Item | Owner | Status | Commit |
|----|------|-------|--------|--------|
| 1410 | Postgres write_batches | sde | ✅ done | 4312f7c |
| 1411 | DuckDB write_batches | sde | ✅ done | e22ea5f |
| 1412 | S3 write_batches (Parquet) | sde | ✅ done | 907b48f |

## P1: Observability — ✅ COMPLETE

| ID | Item | Owner | Status | Commit |
|----|------|-------|--------|--------|
| 1430 | Distributed tracing (W3C + OTLP) | pm+ai-lead | ✅ done | 0ec96b6, ffea27b |
| 1431 | Audit log export (NDJSON) | pm | ✅ done | 9c85729 |

## P1: Performance — ✅ COMPLETE

| ID | Item | Owner | Status | Commit |
|----|------|-------|--------|--------|
| 1440 | Arrow Flight predicate pushdown | ai-lead | ✅ done | be34c6a |
| 1441 | Adaptive parallelism | pm | ✅ done | fc70675 |
| 1442 | Plan cache normalization + metrics | ai-lead | ✅ done | ff4db7b |
| 1450 | Per-datasource rate limiting | ai-lead | ✅ done | 401fd99 |

## P1: Security Hardening — ✅ COMPLETE

| ID | Item | Owner | Status | Commit |
|----|------|-------|--------|--------|
| — | Identifier quoting (6 connectors) | security | ✅ done | 0591272 |
| — | Telemetry security review | security | ✅ done | (review only) |

## P1: Ecosystem — ✅ COMPLETE

| ID | Item | Owner | Status | Commit |
|----|------|-------|--------|--------|
| 1422 | OSD plugin update | ai-lead | ✅ done | bba41c8 |
| 1420 | Go SDK client | — | deferred to Sprint 15 | — |
| 1421 | Jupyter magic command | sde | ✅ done | — |

## Capstone Features — ✅ COMPLETE

| ID | Item | Owner | Status | Commit |
|----|------|-------|--------|--------|
| 1460 | Async query API (submit/poll) | ai-lead | ✅ done | a1827f8 |

## Docs & Frontend

| Item | Owner | Status | Commit |
|------|-------|--------|--------|
| Docs sweep (API ref, README) | fee | ✅ done | d6cc356 |
| What's New in v1.0 guide | fee | ✅ done | 7f7a651 |
| OpenAPI spec (24 endpoints) | fee | ✅ done | 3de9703 |
| CHANGELOG Sprint 14 | fee | ✅ done | 4918645 |
| Getting-started tutorial | fee | ✅ done | 4918645 |
| History search/filter | fee | ✅ done | 88bd834 |
| Connection test button | fee | ✅ done | c035407 |
| Query cost badge | fee | ✅ done | a361368 |
| Schema explorer sidebar | fee | 🔄 in-progress | — |

## Infrastructure

| Item | Owner | Status | Commit |
|------|-------|--------|--------|
| Bench script | pm | ✅ done | f0ba315 |
| Alert CRUD routes wired | pm | ✅ done | 94880fd |
| Federation API endpoint | pm | ✅ done | f7f40e9 |
| test_trace_ids_are_unique fix | pm+fee | ✅ done | ce09ddb |
| Postgres compile fixes | pm | ✅ done | ce09ddb |
