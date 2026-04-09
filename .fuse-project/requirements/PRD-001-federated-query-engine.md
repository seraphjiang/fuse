# PRD: Cross-Datasource Federated Query Engine (Fuse)

**Source:** https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705
**Author:** Huan Jiang
**Status:** Approved
**Date:** 2026-04-07

## Problem

OpenSearch Dashboards supports multiple data sources since v2.4, but each is an
isolated silo. Users cannot run queries across multiple clusters, JOIN between
OpenSearch and S3/Prometheus, or build dashboards blending heterogeneous sources.

## Goals

1. Federate queries across multiple OpenSearch clusters from a single query
2. Support cross-type federation: OpenSearch ↔ S3, OpenSearch ↔ Prometheus
3. Push down operations to connectors where possible for performance
4. Expose a REST API consumable by OpenSearch Dashboards
5. Run as a standalone service (not embedded in OSD Node.js)

## Non-Goals (Phase 1)

- Full PPL parser (extend SQL first, PPL in Phase 2)
- Spark delegation for heavy JOINs (Phase 2+)
- Caching / materialized views (Phase 4)
- RBAC / field-level security (Phase 4)

## Success Criteria

- [ ] User can query across 2+ OpenSearch clusters with `FROM cluster_a.logs, cluster_b.logs`
- [ ] Results are merged, sorted, and limited correctly
- [ ] Filters and aggregations are pushed down to each cluster
- [ ] REST API returns JSON results consumable by OSD
- [ ] Latency < 2x single-cluster query for same-type federation
- [ ] Connector SDK allows adding new datasource types

## User Stories

### US-001: Multi-cluster search
As an operator, I want to search logs across 3 OpenSearch clusters in one query
so I don't have to manually merge results.

### US-002: Cross-cluster aggregation
As an analyst, I want to aggregate error counts by service across all clusters
so I get a unified view of production health.

### US-003: Schema discovery
As a dashboard builder, I want to browse available indices across all registered
datasources so I can build federated queries visually.

### US-004: Health monitoring
As an admin, I want to see the health status of all registered connectors
so I know which datasources are available.

## References

- [Full proposal](https://github.com/opensearch-project/OpenSearch-Dashboards/issues/11705)
- [OSD Multi-datasource plugin](https://opensearch.org/blog/develop-guideline-multiple-data-source-in-opensearch-and-plugins/)
- [Federated PPL engine (sql#561)](https://github.com/opensearch-project/sql/issues/561)
