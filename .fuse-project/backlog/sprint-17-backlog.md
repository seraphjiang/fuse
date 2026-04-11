# Sprint 17 Backlog — Testing & Hardening

**Sprint:** 17
**PM:** pm
**Status:** Active
**Theme:** Test coverage, connector integration tests, performance validation

## P0: Test Coverage

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1700 | Fix 3 ignored tenant auth integration tests | ai-lead | todo | Wire auth middleware for tenant identity in test helpers |
| 1701 | Connector unit tests: Spark SQL building | ai-lead | todo | Edge cases: nested filters, HAVING, OFFSET, special chars |
| 1702 | Connector unit tests: Delta Lake log parsing | ai-lead | todo | Multi-version logs, checkpoint files, concurrent adds/removes |
| 1703 | Connector unit tests: Iceberg schema evolution | ai-lead | todo | Schema changes across snapshots, nested types, map/list |
| 1704 | NL-to-SQL prompt injection tests | security | todo | Verify LLM prompt can't be manipulated via user input |
| 1705 | Query advisor edge cases | ai-lead | todo | Subqueries, CTEs, window functions, nested JOINs |

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
