# Agent Skills Registry

Reusable skill definitions that any agent can be assigned.

## Rust Development

- **rust-core**: Traits, generics, lifetimes, error handling, async/await
- **rust-datafusion**: DataFusion SessionContext, LogicalPlan, ExecutionPlan, optimizer rules
- **rust-axum**: HTTP server, routing, extractors, middleware, state management
- **rust-arrow**: RecordBatch, Schema, DataType, array builders, IPC

## Domain Skills

- **opensearch-connector**: Query DSL, index mappings, scroll API, bulk API, auth (Basic/SigV4)
- **s3-connector**: S3 Select, Parquet reading, Glue catalog, IAM auth
- **prometheus-connector**: PromQL, range queries, instant queries, metric types
- **query-planning**: SQL parsing, logical plan optimization, predicate push-down, cost estimation
- **federation**: Multi-source query decomposition, result merging, semi-join optimization

## Infrastructure Skills

- **ci-cd**: GitHub Actions, cargo test/clippy/fmt, release workflows
- **docker**: Compose files, multi-stage builds, dev environments
- **api-design**: REST conventions, OpenAPI spec, error response standards

## Project Skills

- **coordination**: Task decomposition, delegation, status tracking, blocker resolution
- **research**: Codebase exploration, ecosystem analysis, integration pattern discovery
- **documentation**: README, design docs, API reference, contributor guides
