# Sprint 17 Backlog — Testing & Hardening

**Sprint:** 17
**PM:** pm
**Status:** Active
**Theme:** Test coverage, connector integration tests, performance validation

## P0: Test Coverage — ✅ PARTIAL

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1700 | Fix 3 ignored tenant auth integration tests | ai-lead | todo | Wire auth middleware for tenant identity in test helpers |
| 1701 | Connector unit tests: Spark SQL building | ai-lead | ✅ done | 6998c87 — 16 tests (8 new edge cases) |
| 1702 | Connector unit tests: Delta Lake log parsing | ai-lead | ✅ done | 6998c87 — 13 tests (5 new edge cases) |
| 1703 | Connector unit tests: Iceberg schema evolution | ai-lead | ✅ done | 6998c87 — 12 tests (5 new edge cases) |
| 1704 | NL-to-SQL prompt injection tests | security | todo | Verify LLM prompt can't be manipulated via user input |
| 1705 | Query advisor edge cases | ai-lead | ✅ done | f23e17b — 18 tests (6 new edge cases) |

## P1: Performance

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1710 | Run server benchmarks baseline | pm | todo | Record plan_cache, autocomplete, anomaly, advisor benchmarks |
| 1711 | Load test: 100 concurrent queries | pm | todo | Stress test with mixed query types |

## P1: Integration

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1720 | Wire Parquet reader into Delta Lake execute() | sde | todo | Complete the read path |
| 1721 | Wire Parquet reader into Iceberg execute() | sde | todo | Complete the read path |

## P2: Docs & Cleanup

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1730 | Update README with 25 connectors | fee | todo | Add Spark, Delta Lake, Iceberg to connector table |
| 1731 | Connector guide update | fee | todo | Add lakehouse connector examples |
