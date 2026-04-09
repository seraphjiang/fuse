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
| 006 | Integration test: 2-cluster federation | explorer | done | 005 |
| 007 | Sample fuse.toml config | explorer | done | 004 |
| 008 | CI workflow (.github/workflows/ci.yml) | explorer | done | 005 |
| 009 | PPL parser extension (multi-source FROM) | planner | done | 003 |
| 010 | Result merger (union, global sort/limit) | planner | done | 003 |
| 011 | README update with build/run instructions | planner | done | 005 |
| 012 | docker-compose for local dev (OS cluster) | explorer | done | — |
| 013 | Wire FuseExecutor::execute() to connectors | general | done | 001, 003 |

## P0: Playground & Live Test Site

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 050 | Dockerfile (multi-stage build) | general | done | 005 |
| 051 | docker-compose.yml (fuse-server + OpenSearch) | general | done | 050 |
| 052 | CodeCommit repo + dual remote setup | infra | done | — |
| 053 | ECR repository | infra | done | — |
| 054 | CodeBuild project (Rust toolchain) | infra | done | 052, 053 |
| 055 | ECS Fargate cluster + service + task def | infra | done | 053 |
| 056 | ALB + security group (Amazon VPN IPs only) | infra | done | 055 |
| 057 | CodePipeline: CodeCommit → Build → Deploy | infra | done | 054, 055, 056 |
| 058 | OpenSearch Serverless collections + data | infra | done | — |
| 059 | fuse.toml for playground | infra | in-progress | 058 |
| 060 | End-to-end: push → deploy → query works | tester | in-progress | 057, 059 |
| 061 | Custom domain: fuse.huanji.profile.aws.dev | infra | done | 056 |
| 062 | OpenAPI spec (docs/api/openapi.yaml) | general | done | 004 |
| 063 | Expanded test suite | tester | done | 005 |

## Phase 2: Cross-Type Federation

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 020 | S3/Parquet connector | general | done | 001 |
| 021 | Cross-type JOIN execution (hash-join, semi-join) | planner | done | 003, 020 |
| 022 | Query optimizer: cost-based planning | planner | done | 003 |
| 023 | Spark delegation interface | planner | done | 003 |
| 024 | Visual join builder (OSD plugin) | general | done | 021 |

## Phase 3: Extensibility

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 030 | Prometheus connector | general | done | 001 |
| 031 | Connector SDK + docs | general | done | 001, 020, 030 |
| 032 | Connector protocol versioning | general | done | 031 |

## Phase 4: Advanced

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 040 | Query result caching (TTL per connector) | explorer | done (needs tests) | 003 |
| 041 | Materialized views | general | done | 040 |
| 042 | Alerting integration | general | done | 003 |
| 043 | RBAC / field-level security | general | done | 004 |

## Community Adoption

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 070 | OSD plugin (query bar + results table) | general | done | 004 |
| 071 | Publish fuse-connector-sdk to crates.io | general | done (dry-run) | 031 |
| 072 | Blog post + demo video | explorer | done (draft) | 073 |
| 073 | Federated demo data (logs across 2 clusters) | infra | in-progress | 058 |
| 074 | CONTRIBUTING.md with DCO | explorer | done | — |
| 075 | GitHub Issues templates | explorer | done | — |
| 077 | Performance benchmarks (benches/) | planner | done | 003 |
| 080 | OpenSearch community forum post | explorer | done | 072 |
| 081 | RFC on opensearch-project/OpenSearch-Dashboards | planner | done (draft) | 070 |

## Test Coverage (P0 — Steering Rule #1)

| ID | Item | Owner | Status | Depends On |
|----|------|-------|--------|------------|
| 090 | Tests for cache.rs + cache_middleware.rs | explorer | done | 040 |
| 091 | Tests for S3 connector (select, reader) | planner | in-progress | 020 |
| 092 | Tests for Prometheus connector (promql, parsing) | planner | in-progress | 030 |
| 093 | Coverage audit across all modules | tester | in-progress | — |
