# v1.1.0: 22 Connectors & Federation

*April 11, 2026*

Fuse v1.1.0 ships 22 connectors, Fuse-to-Fuse federation, a complete write path, AI-powered queries, and ecosystem SDKs. This is the result of 5 sprints (12–16) and 115+ items shipped by a 5-agent team.

## Highlights

- **8 new connectors**: Kafka, Athena, Timestream, Snowflake, BigQuery, Cassandra, DuckDB, Arrow Flight
- **Federation**: Chain Fuse instances with cost-based routing
- **Write path**: CTAS, INSERT INTO SELECT, transactions
- **AI queries**: Natural language to SQL, query advisor, auto-suggest, anomaly detection
- **Async API**: Submit/poll for long-running queries
- **Distributed tracing**: W3C Trace Context + OpenTelemetry OTLP
- **Security**: TLS everywhere, RBAC, tenant isolation, WASM sandboxing
- **SDKs**: Go, Python (with DataFrame), TypeScript, Jupyter magic
- **Plugins**: Grafana datasource, VS Code extension, OSD plugin

## Playground UX

The query playground is now a full-featured query IDE:

- **Schema explorer** — browse datasources, tables, and fields; click to insert references
- **Context-aware autocomplete** — dot-triggered (`ds.table.field`), format-aware (SQL vs PPL), with type icons
- **EXPLAIN visualizations** — tree, flame graph (heat-colored), and DAG views
- **Cost badge** — estimated rows and cost shown after EXPLAIN
- **History search** — filter past queries by text, format, and status
- **Connection test** — verify connector health from Settings page
- **Health timeline** — per-connector status history on the Status page
- **Dark/light mode** — persisted to localStorage with system preference fallback
- **CSV/JSON export** — download or copy results in one click

## By the Numbers

| Metric | Value |
|--------|-------|
| Connectors | 22 |
| Core tests | 736 |
| Integration tests | 258 |
| Clippy warnings | 0 |
| Items shipped | 115+ |
| Sprints | 5 (12–16) |

See the full [release notes](../releases/v1.1.0.md) for details.
