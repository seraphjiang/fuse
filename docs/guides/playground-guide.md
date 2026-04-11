# Playground User Guide

The Fuse Playground is an interactive query editor at `http://localhost:9400/` with schema browsing, autocomplete, visualization, and export. This guide covers every feature.

## Query Editor

The main editor supports SQL and PPL with syntax highlighting. Select the format from the dropdown before the editor.

- `Ctrl+Enter` — Run query
- `Ctrl+Shift+E` — Explain query
- `Ctrl+Shift+V` — Validate query
- `✨ Format` — Auto-format/prettify the query

## Autocomplete

Start typing (2+ characters) to see suggestions. The autocomplete is format-aware:

- **SQL mode** — SQL keywords (`SELECT`, `JOIN`, `WHERE`, etc.) and functions (`COUNT()`, `COALESCE()`, `ROW_NUMBER()`, etc.)
- **PPL mode** — PPL keywords (`source`, `where`, `stats`, `lookup`, `dedup`, etc.)
- **Dot-triggered** — Type `datasource.` to see tables, `datasource.table.` to see fields
- **Schema items** — Datasources (📦), tables (📋), fields (🏷️) from your connected sources

Suggestions load automatically from `/api/fuse/datasources` on page load.

## Schema Explorer

Click **📋 Schema** in the toolbar to open the collapsible schema browser:

1. Click a datasource → shows its tables
2. Click a table → shows fields with types
3. Click a field → inserts `datasource.table.field` at the cursor

Data is lazy-loaded and cached after first expand.

## Query Cost Badge

After running **Explain**, a badge appears next to the ▶ Run button showing:

- Estimated rows (e.g., `~10k rows`)
- Estimated cost (e.g., `cost 245.0`)

Hover for full details. Helps gauge query impact before executing.

## Results

Query results display as a sortable table. The meta bar shows:

- Row count, column count, execution time
- **⬇ CSV** — Download results as CSV
- **⬇ JSON** — Download as JSON (column-keyed objects)
- **📋 Copy** — Copy results to clipboard
- **Table / Chart** toggle — Switch between table and chart view

## Charts

Click **Chart** in the view toggle to visualize results. Chart types:

- Auto (engine picks best fit), Line, Bar, Stacked Bar, Pie, Area, Scatter, Histogram
- Log/O11y: Heatmap, Timeline/Gantt, Flame Chart, Sankey

The engine auto-detects the best chart type based on column types and cardinality.

## EXPLAIN Visualizations

Three views for query plans (toggle with buttons above the plan):

- **🌲 Tree** — Indented tree with node details
- **🔥 Flame Graph** — Time-based heat coloring for ANALYZE, percentage of total, hover tooltips
- **🔀 DAG** — Directed acyclic graph with bezier edges, row count labels, cost display

Flame graph and DAG auto-show when EXPLAIN ANALYZE data is available.

## Query History

Click the **🕐 History** tab to see past queries. Filter with:

- **Text search** — Filter by query content
- **Format** — All / SQL / PPL
- **Status** — All / Success / Error

Click any history entry to reload it into the editor.

## Demos

Click **🎬 Demos ▾** for pre-built example queries:

- Cross-Cluster Trace Correlation (JOIN)
- Unified Log View (UNION ALL)
- Error Hotspots (PPL)
- Log Enrichment (OpenSearch + S3)
- Latency Analysis (PPL aggregation)
- Trace Cardinality (COUNT DISTINCT)

## 🎲 Feeling Lucky

Click for a random query from the demo pool — great for exploring your data.

## Query Advisor

Click **💡 Advisor** to get optimization recommendations for your query, such as:

- Missing filters that could reduce scan size
- Suggested indexes or pushdown opportunities
- Join order improvements

## Other Pages

| Page | URL | Purpose |
|------|-----|---------|
| Dashboard | `/dashboard` | Visual analytics builder with 12 chart types |
| Explore | `/explore` | Browse datasources and sample data |
| Settings | `/settings` | Manage datasources, API keys, preferences. **🔌 Test** button verifies connectivity |
| Status | `/status` | Health dashboard with connector status, query stats, health timeline |
| Admin | `/admin` | Server configuration and user management |
| Alerts | `/alerts` | Alert rules and history with status/search filters |
| Views | `/views` | Materialized view management |
| Terminal | `/terminal` | CLI-style query interface |
| Federation | `/federation` | Federation topology visualization |
| Help | `/help` | SQL/PPL reference and examples |

## Dark Mode

All pages support dark/light mode:

- Click **🌓 Theme** to toggle
- Preference saved to `localStorage`
- Falls back to system preference (`prefers-color-scheme`) if no saved choice

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+Enter` | Run query |
| `Ctrl+Shift+E` | Explain query |
| `Ctrl+Shift+V` | Validate query |
| `Tab` / `Enter` | Accept autocomplete suggestion |
| `↑` / `↓` | Navigate autocomplete |
| `Escape` | Close autocomplete |
