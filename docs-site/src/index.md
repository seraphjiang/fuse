# Fuse — Federated Query Engine

Fuse is a cross-datasource federated query engine for [OpenSearch Dashboards](https://opensearch.org/docs/latest/dashboards/). Write a single SQL or PPL query that spans multiple OpenSearch clusters, S3 data lakes, and Prometheus — Fuse fans out, executes, and merges results automatically.

**[🎮 Live Playground](https://fuse.huanji.profile.aws.dev)** *(Amazon VPN required)*

## Architecture

```
                    ┌─────────────────────┐
                    │  OpenSearch          │
                    │  Dashboards / REST   │
                    └─────────┬───────────┘
                              │
                    ┌─────────▼───────────┐
                    │    Fuse Server       │
                    │  (Axum REST API)     │
                    └─────────┬───────────┘
                              │
                    ┌─────────▼───────────┐
                    │   Fuse Engine        │
                    │  (DataFusion-based   │
                    │   planner/optimizer) │
                    └──┬──────┬───────┬───┘
                       │      │       │
              ┌────────▼┐ ┌──▼─────┐ ┌▼────────┐
              │ OS (a)   │ │ OS (b) │ │ S3 O11y │
              │ SigV4    │ │ SigV4  │ │ NDJSON  │
              └──────────┘ └────────┘ └─────────┘
```

## Key Features

- **Same-type federation** — Query across multiple OpenSearch clusters with UNION ALL
- **Cross-type federation** — JOIN OpenSearch data with S3 or Prometheus
- **SQL + PPL** — Full support for both query languages
- **Query pushdown** — Filters, projections, and limits pushed to connectors
- **Data provenance** — `_datasource` column shows where each row came from
- **Execution profiling** — EXPLAIN and ANALYZE with visual plan tree
- **5 connectors** — OpenSearch, S3 Parquet, S3 O11y (NDJSON), Prometheus, custom via SDK
- **Caching** — TTL-based query result cache per connector type
- **Alerting** — Rule-based alerting on federated queries
- **Materialized views** — Pre-computed cross-datasource views

## Quick Start

```bash
# Clone and build
git clone https://github.com/seraphjiang/fuse.git
cd fuse
cargo build --release

# Run with sample config
./target/release/fuse-server --config fuse.toml

# Query
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs LIMIT 5"}'
```

## Connectors

| Connector | Type | Auth | Status |
|-----------|------|------|--------|
| OpenSearch | `opensearch` | SigV4, Basic, None | ✅ Production |
| S3 Parquet | `s3` | IAM | ✅ Production |
| S3 O11y | `s3-o11y` | IAM | ✅ Production |
| Prometheus | `prometheus` | Bearer, None | ✅ Production |
| Custom | SDK | Any | ✅ Via fuse-connector-sdk |
