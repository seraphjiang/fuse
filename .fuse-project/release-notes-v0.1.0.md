# Fuse v0.1.0 — First Release

**Cross-Datasource Federated Query Engine for OpenSearch Dashboards**

Fuse federates queries across multiple OpenSearch clusters, S3 data lakes, and Prometheus from a single SQL or PPL query. Built on Apache DataFusion and datafusion-federation.

## Highlights

- **5 connectors**: OpenSearch (SigV4/AOSS), S3 (Parquet + partition pruning), Prometheus (PromQL), S3-O11y (NDJSON logs), plus the connector SDK for custom connectors
- **21 REST API endpoints**: query, stream (SSE), validate, explain, history, stats, saved queries, cancellation, alerts, materialized views, datasource discovery
- **SQL + PPL**: Full SQL pushdown (filters, projections, aggregations, sort, limit, HAVING, DISTINCT, BETWEEN, IN/NOT IN, ILIKE) plus PPL with multi-source fan-out, lookup, top, rare, eval, rename
- **Cross-datasource federation**: UNION ALL fan-out with type coercion, global ORDER BY, partial failure resilience, and cross-datasource hash JOIN
- **Cost-based optimizer**: Pushdown decisions based on connector capabilities, table stats, and latency class
- **Production-ready**: Structured JSON logging, Prometheus metrics, graceful shutdown, rate limiting, query timeouts, ECS auto-scaling

## Live Playground

https://fuse.huanji.profile.aws.dev (Amazon VPN required)

## Documentation

https://seraphjiang.github.io/fuse/

## Quick Start

```bash
tar xzf fuse-server-v0.1.0-linux-amd64.tar.gz
# Edit fuse.toml with your datasource endpoints
FUSE_CONFIG=fuse.toml ./fuse-server
# Open http://localhost:9400
```

## Test Coverage

582 tests, 0 failures — covering SQL/PPL parsing, pushdown pipeline, federation routing, REST API, connectors, alerting, caching, and E2E scenarios.

## What's Next

- OpenTelemetry distributed tracing
- crates.io SDK publish
- Connector versioning and hot-reload
- Materialized view scheduling
