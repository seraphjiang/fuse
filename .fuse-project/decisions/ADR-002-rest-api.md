# ADR-002: REST API for OSD Integration

**Status:** Accepted
**Date:** 2026-04-08
**Decider:** explorer (hive agent), ratified by sisyphus

## Context

Fuse runs as a separate service. OSD needs to communicate with it.
Options: REST, gRPC, or both.

## Decision

REST API via axum, with SSE streaming for progressive results.

## Rationale

- OSD is Node.js — REST is natural, no protobuf compilation needed
- Consistent with OpenSearch ecosystem (all REST APIs)
- SSE provides streaming without gRPC complexity
- gRPC can be added later if needed for service-to-service communication

## Consequences

- No strongly-typed client generation from proto files (mitigated by OpenAPI spec)
- SSE streaming is simpler but less efficient than gRPC streaming for high-throughput
