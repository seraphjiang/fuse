# Backlog

Items are ordered by priority. Status: `todo` | `in-progress` | `blocked` | `done`

## Phase 1: Same-Type Federation

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 001 | fuse-core: connector trait, errors, config, registry | general | done | — |
| 002 | fuse-connector-opensearch: client, schema, pushdown | general | done | 001 |
| 003 | fuse-engine: DataFusion federation planner | planner | done | 001 |
| 004 | fuse-server: REST API (axum) | explorer | done | 001, 003 |
| 005 | Full workspace compiles clean | sisyphus | done | 001-004 |
| 006 | Integration test: 2-cluster federation | — | todo | 005 |
| 007 | Sample fuse.toml config | explorer | todo | 004 |
| 008 | CI workflow (.github/workflows/ci.yml) | — | todo | 005 |
| 009 | PPL parser extension (multi-source FROM) | — | todo | 003 |
| 010 | Result merger (union, global sort/limit) | — | todo | 003 |
| 011 | README update with build/run instructions | — | todo | 005 |
| 012 | docker-compose for local dev (OS cluster) | — | todo | — |

## Phase 2: Cross-Type Federation

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 020 | S3/Parquet connector | — | todo | 001 |
| 021 | Cross-type JOIN execution (hash-join, semi-join) | — | todo | 003, 020 |
| 022 | Query optimizer: cost-based planning | — | todo | 003 |
| 023 | Spark delegation for heavy JOINs | — | todo | 003 |
| 024 | Visual join builder (OSD plugin) | — | todo | 021 |

## Phase 3: Extensibility

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 030 | Prometheus connector | — | todo | 001 |
| 031 | Connector SDK + docs | — | todo | 001, 020, 030 |
| 032 | Connector protocol versioning | — | todo | 031 |

## Phase 4: Advanced

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 040 | Query result caching (TTL per connector) | — | todo | 003 |
| 041 | Materialized views | — | todo | 040 |
| 042 | Alerting integration | — | todo | 003 |
| 043 | RBAC / field-level security | — | todo | 004 |
