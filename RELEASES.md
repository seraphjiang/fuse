# Releases

| Version | Date | Highlights |
|---------|------|------------|
| [v0.6.0](#v060) | 2026-04-10 | Enterprise stack (multi-tenancy, audit, query governor), AI (NL→SQL, advisor), Python + TypeScript SDKs, Grafana plugin, DuckDB connector (15 total), 1,072 tests |
| [v0.5.0](#v050) | 2026-04-10 | Production hardening (API key auth, rate limiting, timeout, cancellation), recursive CTEs, anomaly detection, RIGHT JOIN, saved views, 983 tests |
| [v0.4.0](#v040) | 2026-04-10 | Dashboard platform (12 chart types, templates, variables, drill-down), MongoDB + InfluxDB + ClickHouse connectors (14 total), architecture doc, 904 tests |
| [v0.3.0](#v030) | 2026-04-10 | 7 new connectors (11 total), cross-datasource JOINs, semi/anti-join, correlated subqueries, cursor pagination, window functions, cost estimator, 660 tests |
| [v0.2.0](#v020) | 2026-04-09 | PPL lookup/top/rare, HAVING/BETWEEN/IN pushdown, data provenance, EXPLAIN ANALYZE, saved queries, rate limiting, 586 tests |
| [v0.1.0](#v010) | 2026-04-09 | Foundation — FederatedConnector trait, 4 connectors (OpenSearch, S3, Prometheus, S3 O11y), SQL + PPL, hash JOIN, UNION ALL, REST API, playground, 256 tests |

See [CHANGELOG.md](CHANGELOG.md) for full details per release.

---

<a id="v060"></a>
## v0.6.0 — Enterprise, AI & SDKs

- Multi-tenancy with per-tenant datasource isolation and resource limits
- Query governor: max_rows, max_time_ms, max_result_bytes per tenant
- Audit logging with structured JSON (identity, action, status, timing)
- Natural language → SQL endpoint (`POST /api/fuse/nl`)
- Query advisor endpoint (`GET /api/fuse/advisor`)
- Python SDK (zero deps, pip install fuse-client)
- TypeScript SDK (zero deps, npm install fuse-client)
- Grafana datasource plugin
- DuckDB connector (15 total)
- All connectors pass security audit (InfluxDB + Prometheus injection fixes)
- 25-page docs site, admin guide, community guide
- 1,072 tests

<a id="v050"></a>
## v0.5.0 — Production Hardening

- API key authentication (x-api-key / Bearer, viewer/editor/admin roles)
- Rate limiting (global + per-IP, configurable)
- Query timeout with per-query override and automatic cancellation
- Recursive CTEs for dependency chain tracing
- Anomaly detection primitives (moving avg, stddev, z-score)
- RIGHT JOIN
- Top-N pushdown to connectors
- Saved queries / virtual views
- Parallel connector health checks
- UNION ALL cursor pagination (global cursor)
- OSD plugin packaging
- 983 tests

<a id="v040"></a>
## v0.4.0 — Visualization Platform

- Dashboard platform: 12 chart types (line, bar, stacked bar, pie, area, scatter, histogram, table, heatmap, timeline, flame chart, Sankey)
- Auto-visualization (detect column types → suggest chart)
- Dashboard templates (Error Analysis, Trace Correlation, Capacity)
- Variables, drill-down, auto-refresh, time range picker
- Save/load/share/export (PNG, CSV, PDF)
- MongoDB, InfluxDB, ClickHouse connectors (14 total)
- Architecture documentation, OpenAPI v0.4.0
- 904 tests

<a id="v030"></a>
## v0.3.0 — Cross-Datasource Analytics

- DynamoDB, PostgreSQL, MySQL, Elasticsearch, Redis, CSV/JSON, CloudWatch connectors (11 total)
- Cross-datasource JOINs with build-side selection
- Semi-join (EXISTS), anti-join (NOT EXISTS), correlated subqueries
- Cross-datasource GROUP BY with federated re-aggregation
- Server-side cursor pagination (keyset-based)
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- CASE WHEN, computed columns, date/time/string/math functions
- Cost estimator, trace reconstruction endpoint
- 660 tests

<a id="v020"></a>
## v0.2.0 — Query Engine Depth

- PPL lookup, top, rare, eval, rename commands
- HAVING, BETWEEN, IN/NOT IN, ILIKE pushdown
- Data provenance (_datasource column)
- EXPLAIN ANALYZE with execution profiling
- Query history, saved queries, query cancellation
- Rate limiting, CSV export, parameterized queries
- Partial failure resilience in UNION ALL
- 586 tests

<a id="v010"></a>
## v0.1.0 — Foundation

- FederatedConnector trait, ConnectorRegistry, SubQuery
- 4 connectors: OpenSearch, S3 Parquet, S3 O11y, Prometheus
- SQL + PPL parsing with DataFusion federation
- Cross-datasource hash JOIN and UNION ALL
- Cost-based pushdown optimizer, query cache
- REST API (8 endpoints), embedded playground UI
- Alerting, materialized views, RBAC
- 256 tests
