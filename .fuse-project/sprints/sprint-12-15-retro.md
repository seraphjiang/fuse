# Retrospective — Sprints 12-15

**Date:** 2026-04-11
**PM:** pm
**Agents:** pm, ai-lead, fee, sde, security

## Summary

4 sprints, 100+ items shipped, 22 connectors, 719+ core tests. Fuse went from a 14-connector query engine to a full production platform with federation, write path, AI-powered queries, distributed tracing, and ecosystem SDKs.

## What Went Well

1. **Velocity** — 100+ items in a single session. ai-lead shipped 21 items (most productive individual). fee delivered 23+ frontend/docs items with zero regressions.
2. **Test discipline** — Every feature shipped with tests. Core test count grew from ~980 to 719+ (restructured) with 257 integration tests restored.
3. **Security-first** — security agent found real vulnerabilities (tenant_id bypass, WASM DoS, identifier injection) and fixed them same-sprint. All 9 SQL/CQL connectors now have proper quoting.
4. **Federation story** — Complete end-to-end: connector + registry + routing + cost-based selection + UI page + health aggregation.
5. **Write path** — CTAS + INSERT + transactions + 3 write connectors (Postgres, DuckDB, S3 Parquet).

## What Didn't Go Well

1. **Duplicate work** — 5+ instances of agents working on already-completed items (#1051, #641, #1400, #1403, rate limiting). Root cause: message latency in hive protocol. Cost: ~30 min wasted effort.
2. **Cross-agent compile breaks** — Multiple agents modifying AppState/api_test.rs caused cascading compile errors. #1550 required dedicated fix effort.
3. **Stale broadcasts** — Assignments broadcast didn't always arrive before agents picked up work. Need immediate per-agent tells for assignments, not just broadcasts.

## Action Items

1. **Assignment protocol** — Always `tell` the specific agent AND broadcast. Don't rely on broadcast alone.
2. **Shared file lock** — Announce `[WORKING]` on shared files (api.rs, lib.rs, api_test.rs) per steering rules. Enforce more strictly.
3. **Pre-work check** — Agents must `git log --oneline -20` before starting any item to verify it's not already done.
4. **AppState changes** — Any agent adding fields to AppState must also update ALL test constructors in the same commit.

## Per-Agent Stats

| Agent | S12 | S13 | S14 | S15 | Total |
|-------|-----|-----|-----|-----|-------|
| pm | 10 | 4 | 11 | 4 | 29 |
| ai-lead | 4 | 6 | 6 | 4 | 20 |
| fee | 12 | 1 | 10+ | 3 | 26+ |
| sde | 4 | 4 | 6 | 3 | 17 |
| security | 3 | 2 | 3 | 2 | 10 |
| **Total** | **33** | **17** | **36+** | **16** | **102+** |
