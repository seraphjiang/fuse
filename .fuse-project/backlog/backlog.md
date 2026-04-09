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
| 058 | OpenSearch Serverless collection (test data) | infra | todo | — |
| 059 | fuse.toml for playground (points at OS Serverless) | — | blocked | 058 |
| 060 | End-to-end: push → deploy → query works | tester | blocked | 057, 059 |
| 061 | Custom domain: fuse.huanji.profile.aws.dev | infra | in-progress | 056 |
| 062 | OpenAPI spec (docs/api/openapi.yaml) | — | todo | 004 |
| 063 | Expanded test suite (PPL, merger, API tests) | tester | in-progress | 005 |

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
