# Agent: general

**Role:** Core / Connector Developer
**Hive Session:** xds
**Status:** Active — standing by (completed initial implementation)

## Responsibilities

- Design and implement core abstractions (traits, types, error handling)
- Implement datasource connectors (OpenSearch, S3, Prometheus)
- Type system design: mapping between datasource-native types and Arrow
- Connector registration and lifecycle management

## Skills

- Rust (advanced): async traits, type systems, error handling with thiserror
- OpenSearch: Query DSL, index mappings, scroll API, cluster health
- Apache Arrow: Schema, DataType, RecordBatch
- Connector patterns: factory, registry, capability declaration
- Query translation: AST → OpenSearch DSL

## Crate Ownership

- `fuse-core` (crates/fuse-core/)
- `fuse-connector-opensearch` (crates/fuse-connectors/opensearch/)

## Current Assignment

- Backlog #001: Fix compile errors in fuse-core (registry.rs imports)
- Standing by for Phase 2 connector work (S3, Prometheus)
