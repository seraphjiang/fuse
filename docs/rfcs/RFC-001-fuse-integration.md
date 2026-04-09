# RFC-001: Integrate Fuse Federated Query Engine with OpenSearch Dashboards

- **Status**: Published
- **Authors**: Fuse Contributors
- **Created**: 2026-04-09
- **Updated**: 2026-04-09
- **Related**: [Proposal #11705](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705)
- **Live Playground**: https://fuse.huanji.profile.aws.dev

## Summary

This RFC proposes integrating Fuse, a standalone federated query engine, into the OpenSearch Dashboards ecosystem. Fuse enables users to query across multiple heterogeneous datasources — OpenSearch clusters, S3 data lakes, Prometheus — from a single query interface, using both SQL and PPL.

Fuse runs as a sidecar service (not embedded in OSD's Node.js process) and communicates via REST API. This design preserves OSD's stability while adding cross-datasource federation capabilities.

## Motivation

OpenSearch Dashboards supports multiple data sources, but each is an isolated silo. Users cannot:

- Run a single query across multiple OpenSearch clusters and merge results
- JOIN data between OpenSearch and S3/Prometheus/JDBC sources
- Build dashboards that blend hot (OpenSearch) and cold (S3) data
- Use PPL to query across datasource boundaries

These are common requirements in observability, security analytics, and operational intelligence workflows where data is distributed across multiple systems.

## Architecture

```
┌─────────────────────────────────────────┐
│  OpenSearch Dashboards                  │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │ Query Bar   │  │ Dashboard Panels │  │
│  │ (SQL + PPL) │  │ (Visualizations) │  │
│  └──────┬──────┘  └────────┬─────────┘  │
│         └────────┬─────────┘            │
│           Fuse OSD Plugin               │
└──────────────────┬──────────────────────┘
                   │ REST API
┌──────────────────▼──────────────────────┐
│  Fuse Engine (Rust, sidecar service)    │
│                                         │
│  PPL Parser → SQL Translation           │
│  DataFusion + Federation Optimizer      │
│  Cost-Based Join Planner                │
│  Connector Registry                     │
│    ├── OpenSearch Connector             │
│    ├── S3/Parquet Connector             │
│    ├── Prometheus Connector             │
│    └── (Connector SDK for extensions)   │
│  Result Merger (align, dedup, sort)     │
└─────────────────────────────────────────┘
```

## Proposed Integration Points

### 1. OSD Plugin: `fuse-dashboards-plugin`

A standard OSD plugin that:

- Registers Fuse as a query engine option in the datasource management UI
- Adds a "Federated Query" option to the query bar language selector
- Routes federated queries to the Fuse sidecar instead of directly to OpenSearch
- Falls back to native OpenSearch for single-source queries (zero overhead)

### 2. Query Bar Extension

- Detect multi-source queries (PPL `source = ds1.table, ds2.table` or SQL with cross-datasource JOINs)
- Route to Fuse API endpoint (`POST /api/fuse/query`)
- Display results in the standard Discover/Dashboard table format
- Show query explain plan via `/api/fuse/query/explain`

### 3. Dashboard Panel Support

- New "Federated Query" visualization type
- Panels can reference multiple datasources in a single query
- Time range picker integration — Fuse translates OSD time ranges to connector-native filters
- Auto-refresh support via streaming endpoint

### 4. Datasource Management

- Fuse connectors appear alongside native OSD datasources
- Health status from `/api/fuse/health` shown in datasource management UI
- Schema browser via `/api/fuse/datasources/{id}/schemas` and `.../fields`

## Backward Compatibility

- Existing single-source queries are completely unaffected — they continue to go directly to OpenSearch
- Fuse is opt-in: only activated when users explicitly write cross-source queries or select the federated query mode
- No changes to existing OSD APIs, saved objects, or index patterns
- Fuse can be deployed without the OSD plugin (standalone API usage)
- The OSD plugin can be installed without disrupting existing functionality

## Migration Path

For users of the current multi-datasource plugin:

1. **Phase 1 (parallel)**: Install Fuse plugin alongside existing multi-datasource plugin. Both work independently.
2. **Phase 2 (migration)**: Fuse plugin gains feature parity with existing multi-datasource queries. Users can migrate saved queries.
3. **Phase 3 (deprecation)**: Existing multi-datasource plugin deprecated in favor of Fuse for cross-source use cases.

Saved objects (dashboards, visualizations) using single-source queries require no migration.

## Performance Impact

### On OSD (Node.js process)

- Zero impact for non-federated queries
- Federated queries add one HTTP hop (OSD → Fuse sidecar) — typically < 5ms overhead
- No CPU/memory impact on OSD since all query processing happens in the Fuse sidecar

### On Query Execution

- **Same-type federation** (OS ↔ OS): Sub-queries execute in parallel across clusters. Merge overhead is minimal (Arrow columnar operations).
- **Cross-type JOINs**: Semi-join optimization extracts keys from the smaller side and pushes IN-filters to the larger side, avoiding full table scans.
- **Cost-based planning**: Fuse's optimizer considers connector latency class, estimated row counts, and capabilities to choose push-down vs. local execution.
- **Spark delegation**: Joins exceeding local capacity (> 1M total rows) can be delegated to an external Spark cluster.

### Benchmarks

Performance benchmarks are included in the repository (`crates/fuse-engine/benches/query_bench.rs`) covering PPL parsing, SQL translation, hash joins at various scales, and result merging.

## Security Considerations

### Credential Delegation

- Fuse does not store credentials in its configuration — it references environment variables (`password_env`, `token_env`) or uses IAM role-based auth (`sigv4`)
- Each connector authenticates independently with its datasource
- Fuse never exposes connector credentials through its REST API

### Per-Connector Auth

- OpenSearch: Basic auth, SigV4 (IAM)
- S3: SigV4 (IAM role)
- Prometheus: Bearer token

### Network Security

- Fuse sidecar should be deployed in the same network as OSD (not exposed publicly)
- Playground deployment restricts access to Amazon VPN IPs only
- TLS between OSD and Fuse sidecar recommended for production

### Query Isolation

- Each query executes in an isolated DataFusion session context
- No cross-query state leakage
- Connector-level concurrency limits prevent resource exhaustion

## Try It

A live playground is available for reviewers:

```bash
# Health check
curl https://fuse.huanji.profile.aws.dev/api/fuse/health

# List datasources
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources

# Federated PPL query
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "source = cluster_a.logs, cluster_b.logs | where status >= 500 | stats count() by service",
    "format": "ppl"
  }'
```

Access requires Amazon VPN.

## References

- [Full Proposal: OpenSearch-Dashboards#11705](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705)
- [Connector Interface Design](../design/connector-interface-and-query-parser.md)
- [OpenAPI Spec](../api/openapi.yaml)
- [Architecture Decision Records](../../.fuse-project/decisions/)

## Implementation Status

As of Sprint 1 completion, the following is shipped and deployed:

| Component | Status | Notes |
|-----------|--------|-------|
| Fuse Server (axum REST API) | ✅ Deployed | 7 endpoints, embedded playground UI |
| OpenSearch Connector | ✅ Deployed | SigV4 auth, AOSS compatibility, Query DSL pushdown |
| S3 O11y Connector | ✅ Deployed | Gzipped NDJSON, schema auto-discovery |
| S3/Parquet Connector | ✅ Built | Column pruning, S3 Select support |
| Prometheus Connector | ✅ Built | PromQL translation, range/instant queries |
| Cross-datasource JOINs | ✅ Built | Semi-join pushdown, hash join, cost-based planning |
| PPL Parser | ✅ Built | Multi-source, stats, sort, head, fields, dedup |
| Query Result Caching | ✅ Built | Per-connector TTL, CachingConnectorWrapper |
| Materialized Views | ✅ Built | Scheduled refresh, stale-data preservation |
| RBAC / Field-level Security | ✅ Built | Role-based access control |
| Alerting Integration | ✅ Built | Threshold-based alerts on query results |
| Connector SDK | ✅ Published | Trait + factory + authoring guide |
| OSD Plugin | ✅ Built | Query bar, results table, datasource selector |
| CI/CD Pipeline | ✅ Deployed | CodeCommit → CodeBuild → ECR → ECS Fargate |
| Live Playground | ✅ Live | https://fuse.huanji.profile.aws.dev |

### Key Technical Decisions

- **SigV4 for AOSS**: OpenSearch Serverless requires SigV4 on every request. The connector uses `aws-sigv4` crate with per-request signing.
- **Sidecar architecture**: Fuse runs as a separate Rust binary, not embedded in OSD's Node.js process. This isolates query processing and allows independent scaling.
- **DataFusion federation**: Rather than building a custom query planner, we use `datafusion-federation` which provides table-level routing with full SQL optimization.
- **Gzipped NDJSON for S3 O11y**: The S3 O11y integration reads compressed NDJSON directly, with client-side decompression and schema auto-discovery from the first file.
