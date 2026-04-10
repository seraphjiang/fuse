# Fuse Query — OpenSearch Dashboards Plugin

Cross-datasource federated query engine plugin for OpenSearch Dashboards. Write SQL or PPL queries that span multiple datasources — OpenSearch, S3, DynamoDB, PostgreSQL, Redis, CloudWatch, Prometheus, and more — from a single interface.

## Features

- **Query Editor** — SQL/PPL toggle with syntax highlighting, Ctrl+Enter to run
- **Results Table** — Column sorting, pagination, `_datasource` color-coding for provenance
- **Execution Plan** — Interactive tree with cost gradient (green→red), critical path highlighting
- **Trace Timeline** — Cross-source trace reconstruction with per-datasource color-coded spans
- **Dashboard Panel** — Auto-visualization: detects column types → suggests chart (line/bar/pie/scatter/area)
- **Datasource Picker** — Browse connected datasources, schemas, and fields
- **Health Indicator** — Real-time connector health status
- **Query History** — Recent queries with replay
- **Saved Dashboards** — Persist and share dashboard configurations

## Components

| Component | Description |
|-----------|-------------|
| `QueryEditor` | SQL/PPL input with syntax highlighting overlay |
| `ResultsTable` | Sortable, paginated results with datasource provenance |
| `ExecutionPlanTree` | Cost-gradient tree visualization for EXPLAIN/ANALYZE |
| `TraceTimeline` | Cross-source trace reconstruction timeline |
| `DashboardPanel` | Auto-viz charts (ECharts) from query results |
| `DatasourceSelector` | Datasource browser with schema discovery |
| `HealthIndicator` | Connector health status badges |
| `QueryHistory` | Recent query list with replay |
| `DatasourcePicker` | Compact datasource selector dropdown |
| `SavedDashboards` | Dashboard save/load management |

## Installation

```bash
# From OpenSearch Dashboards directory
cd plugins/
git clone <repo> fuse-query
# Or copy the osd-plugin/fuse-query directory

# Build
cd fuse-query
yarn osd bootstrap
yarn build
```

## Configuration

The plugin proxies requests to a running Fuse server. Configure the server URL in `opensearch_dashboards.yml`:

```yaml
fuse.server.url: "http://localhost:3000"
```

## Requirements

- OpenSearch Dashboards >= 2.14.0
- Node.js >= 18
- Running Fuse server instance

## API

The plugin exposes `FuseApiService` with methods:

- `query(request)` — Execute SQL/PPL query
- `explain(request)` — Get execution plan
- `validate(request)` — Validate query syntax
- `datasources()` — List connected datasources
- `getSchemas(id)` — Get datasource schemas
- `getFields(id, table)` — Get table fields
- `health()` — Connector health check
- `history()` — Query history
- `trace(traceId)` — Cross-source trace reconstruction

## License

Apache-2.0
