# Sprint 1: Foundation

**Goal:** All 4 crates compile clean, basic REST API serves a hardcoded response.
**Duration:** 2026-04-08 → 2026-04-15
**Status:** In Progress

## Sprint Items

| Backlog ID | Item | Owner | Status |
|------------|------|-------|--------|
| 001 | fuse-core: traits, errors, config, registry | general | blocked (compile errors) |
| 002 | fuse-connector-opensearch | general | blocked (depends on 001) |
| 003 | fuse-engine: DataFusion integration | planner | in-progress |
| 004 | fuse-server: REST API | explorer | in-progress |
| 005 | Fix compile errors, full workspace build | sisyphus | todo |
| 007 | Sample fuse.toml | explorer | todo |
| 011 | README with build/run instructions | sisyphus | todo |

## Blockers

- fuse-core has import mismatches in registry.rs (ConnectorHealth, FederatedConnector, RegistryError not found)
- planner and explorer may have stale context after overnight — need to re-sync

## Definition of Done

- `cargo check` passes for entire workspace (zero errors)
- `fuse-server` starts and responds to `GET /api/fuse/health`
- Sample fuse.toml exists with example OpenSearch connector config
- README has build instructions
