# ADR-001: Rust + Apache DataFusion as Engine Stack

**Status:** Accepted
**Date:** 2026-04-08
**Decider:** planner (hive agent), ratified by sisyphus

## Context

Fuse needs a standalone query engine that can parse SQL/PPL, plan federated
execution across multiple datasources, optimize push-downs, and merge results.
We evaluated Rust, Go, and Java.

## Decision

Rust with Apache DataFusion + datafusion-federation.

## Rationale

- `datafusion-federation` (v0.5.3) provides ~70% of the engine: federation
  framework, query planner, optimizer, execution DAG
- `sqlparser-rs` is the most complete SQL parser in any language
- Apache Arrow as native in-memory format: zero-copy, columnar, streaming
- Single static binary deployment (~20MB), no JVM dependency
- True parallelism via tokio async runtime

## Alternatives Considered

- **Go:** No equivalent to DataFusion — would build entire query engine from scratch
- **Java + Calcite:** Complex to extend, JVM overhead, OpenSearch community has Java fatigue

## Consequences

- Steeper learning curve for contributors unfamiliar with Rust
- OpenSearch ecosystem is Java-heavy — Rust connector libraries are less mature
- Need to extend sqlparser-rs for PPL syntax (custom dialect)
