# Phase 1 Acceptance Criteria

All items must pass for Phase 1 to be considered complete.

## Core Engine
- [ ] `fuse-core` compiles with connector trait, error types, config, registry
- [ ] `fuse-engine` integrates DataFusion with federation optimizer
- [ ] SQL queries with `datasource.table` syntax are parsed and planned
- [ ] Sub-queries are routed to correct connectors based on datasource qualifier

## OpenSearch Connector
- [ ] Connects to OpenSearch cluster with Basic/SigV4/NoAuth
- [ ] Discovers indices and their field mappings (→ Arrow Schema)
- [ ] Pushes down: filters, projections, aggregations, sort, limit
- [ ] Streaming via scroll API for large result sets
- [ ] Health check via cluster health API

## Federation
- [ ] Multi-source FROM: `SELECT * FROM cluster_a.logs, cluster_b.logs WHERE status=500`
- [ ] Parallel fan-out to multiple connectors
- [ ] Result merge: union, global sort, global limit
- [ ] Schema alignment when index mappings differ slightly

## API Server
- [ ] POST /api/fuse/query — execute SQL, return JSON results
- [ ] GET /api/fuse/datasources — list connectors
- [ ] GET /api/fuse/datasources/{id}/schemas — schema discovery
- [ ] GET /api/fuse/health — engine + connector health
- [ ] Configurable via TOML file

## Quality
- [ ] All crates compile with zero warnings (`cargo check`)
- [ ] `cargo clippy` passes
- [ ] `cargo fmt --check` passes
- [ ] Integration test: 2-cluster federation query end-to-end
