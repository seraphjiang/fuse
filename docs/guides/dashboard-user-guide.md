# Dashboard User Guide

Fuse includes a built-in dashboard platform at `/dashboard` for building visual analytics over federated queries. Dashboards are grid-based, support 12 chart types, auto-refresh, variables, drill-down, and can be saved/shared/exported.

**URL:** `http://localhost:9400/dashboard` (local) or `https://fuse.huanji.profile.aws.dev/dashboard` (playground)

## Quick Start

1. Open `/dashboard` in your browser
2. Click **Templates** and select "Error Analysis" to load a pre-built dashboard
3. Panels auto-execute their queries and render charts
4. Adjust the time range dropdown (top bar) to change the window
5. Click any panel's **✏️** button to edit its query or chart type

## Panels

Each panel is a self-contained query + visualization. Panels live on a 12-column grid.

### Adding a Panel

Click **+ Add Panel** in the header. Fill in:

- **Title** — display name
- **Query** — SQL or PPL query (uses `datasource.table` syntax)
- **Format** — `sql` or `ppl`
- **Chart Type** — auto-detect or pick manually
- **Width** — grid columns (3, 4, 6, 8, or 12)

The panel executes immediately and renders the chart.

### Editing a Panel

Click **✏️** on any panel header to open the edit modal. Change the query, chart type, or title and click Save. The panel re-executes.

### Removing a Panel

Click **🗑** on the panel header.

### Resizing

Panels snap to grid columns. Set width in the edit modal:
- `3` — quarter width (4 panels per row)
- `4` — third width (3 per row)
- `6` — half width (2 per row)
- `12` — full width

## Chart Types

Fuse auto-detects the best chart type based on column types. Override with the chart type dropdown in the panel editor.

### Auto-Detection Rules

| Data Pattern | Suggested Chart |
|-------------|-----------------|
| Timestamp + number | Line |
| Category + number (≤12 rows) | Pie |
| Category + number | Bar |
| 2+ numbers | Scatter |
| Category + number + second category | Stacked Bar |

### Standard Charts (8)

| Type | Best For | Example Query |
|------|----------|---------------|
| **Line** | Time series, trends | `SELECT DATE_TRUNC('hour', timestamp) as hour, count(*) as n FROM ... GROUP BY hour ORDER BY hour` |
| **Bar** | Category comparison | `SELECT service, count(*) as errors FROM ... GROUP BY service ORDER BY errors DESC` |
| **Stacked Bar** | Category breakdown by group | `SELECT service, status, count(*) as n FROM ... GROUP BY service, status` |
| **Pie** | Proportions (≤12 categories) | `SELECT method, count(*) as n FROM ... GROUP BY method` |
| **Area** | Cumulative trends | Same as line — renders with filled area |
| **Scatter** | Correlation between two numbers | `SELECT response_time_ms, status FROM ... LIMIT 200` |
| **Histogram** | Distribution of a numeric column | `SELECT response_time_ms FROM ... LIMIT 500` |
| **Table** | Raw data, detailed records | `SELECT * FROM ... LIMIT 50` |

### Observability Charts (4)

| Type | Best For | Example Query |
|------|----------|---------------|
| **Heatmap** | Density (category × category) | `SELECT service, status, count(*) as cnt FROM ... GROUP BY service, status` |
| **Timeline / Gantt** | Span durations, trace timelines | `SELECT service, response_time_ms FROM ... ORDER BY response_time_ms DESC LIMIT 30` |
| **Flame Chart** | Nested execution spans by depth | `SELECT service, response_time_ms FROM ... WHERE response_time_ms > 100 LIMIT 30` |
| **Sankey** | Request flow between services | `SELECT source_service, target_service, count(*) as n FROM ... GROUP BY source_service, target_service` |

## Time Range

The time range dropdown in the top bar sets a global time window. Options: 5m, 15m, 1h, 6h, 24h, 7d.

Changing the time range re-executes all panels.

## Auto-Refresh

Set the refresh interval in the top bar. Options: Off, 10s, 30s, 1m, 5m.

When active, a green pulse indicator appears. All panels re-execute on each interval.

## Variables

Variables are dropdown filters that update all panels. They appear in the variable bar below the time range.

### Adding a Variable

Variables are defined per dashboard. Each variable has:

- **Name** — used in queries as `$name`
- **Label** — display name in the dropdown
- **Type** — `query` (populated from a SQL query) or `custom` (static list)
- **Query** (for query type) — e.g., `SELECT DISTINCT service FROM cluster_a.application_logs`

### Using Variables in Queries

Reference variables with `$name` in panel queries:

```sql
SELECT status, count(*) as n
FROM cluster_a.application_logs
WHERE service = $service
GROUP BY status
```

When the user selects a value from the dropdown, all panels re-execute with the new value.

## Drill-Down

Click a chart element (bar, pie slice, data point) to filter all panels to that value. For example:

1. Click a bar for "api-gateway" in a service chart
2. All panels add `WHERE service = 'api-gateway'` and re-execute
3. Click again to clear the drill-down filter

## Templates

Click **Templates** in the header to load a pre-built dashboard. Three templates are included:

| Template | Panels | Focus |
|----------|--------|-------|
| 🔴 Error Analysis | 5 | Error rates by service, status distribution, cross-cluster error timeline, top error messages, error heatmap |
| 🔗 Trace Correlation | 5 | Cross-cluster trace matches, service flow (Sankey), latency by service, latency distribution, trace span timeline |
| 📈 Capacity & Performance | 6 | Request volume, cluster comparison, method distribution, top endpoints, P99 latency, cross-cluster stacked bar |

Templates include pre-configured variables and refresh intervals.

## Save / Load / Share

### Save

Click **💾 Save** to save the current dashboard to browser local storage. Dashboards persist across page reloads.

Name your dashboard in the dropdown — multiple dashboards can be saved.

### Load

Select a saved dashboard from the dropdown in the header. It loads all panels, variables, time range, and refresh settings.

### Share

Click **🔗 Share** to copy a shareable URL to the clipboard. The URL encodes the full dashboard state.

### Export

Click the export menu for:
- **📷 Export PNG** — screenshot of the full dashboard
- **📄 Export CSV** — download all panel data as CSV
- **🖨 Print / PDF** — browser print dialog for PDF export

## Cross-Datasource Dashboard Patterns

### Unified Error View (UNION ALL)

```sql
SELECT _datasource, service, count(*) as errors
FROM cluster_a.application_logs WHERE status >= 500
GROUP BY _datasource, service
UNION ALL
SELECT _datasource, service, count(*) as errors
FROM cluster_b.application_logs WHERE status >= 500
GROUP BY _datasource, service
ORDER BY errors DESC
```

Use a **Stacked Bar** chart with `_datasource` as the series to compare error rates across clusters.

### Service Dependency Flow (JOIN + Sankey)

```sql
SELECT a.service as source, b.service as target, count(*) as requests
FROM cluster_a.application_logs a
JOIN cluster_b.application_logs b ON a.trace_id = b.trace_id
GROUP BY a.service, b.service
ORDER BY requests DESC LIMIT 30
```

Use a **Sankey** chart to visualize request flow between services across clusters.

### Enriched Error Table (JOIN)

```sql
SELECT a.timestamp, a.service, a.status, a.message, d.name, d.team
FROM cluster_a.application_logs a
JOIN dynamodb.fuse_user_profiles d ON a.user_id = d.user_id
WHERE a.status >= 500
ORDER BY a.timestamp DESC LIMIT 50
```

Use a **Table** chart to show error logs enriched with user profile data from DynamoDB.
