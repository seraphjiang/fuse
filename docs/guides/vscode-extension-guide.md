# VS Code Extension Guide

The Fuse VS Code extension provides SQL/PPL editing with schema-aware autocomplete, inline query execution, and result visualization.

## Install

1. Open VS Code → Extensions (`Ctrl+Shift+X`)
2. Search "Fuse Query Engine"
3. Click Install

Or from the command line:

```bash
code --install-extension fuse.fuse-vscode
```

## Configure

Open Settings (`Ctrl+,`) → search "Fuse":

| Setting | Default | Description |
|---------|---------|-------------|
| `fuse.serverUrl` | `http://localhost:9400` | Fuse server URL |
| `fuse.apiKey` | (empty) | API key for authentication |
| `fuse.defaultFormat` | `sql` | Default query format (sql or ppl) |
| `fuse.timeout` | `30000` | Query timeout in milliseconds |
| `fuse.maxRows` | `1000` | Default row limit for inline results |

Or in `settings.json`:

```json
{
  "fuse.serverUrl": "https://fuse.internal:9400",
  "fuse.apiKey": "your-api-key",
  "fuse.defaultFormat": "sql"
}
```

## Features

### Schema-Aware Autocomplete

The extension fetches datasource schemas from `/api/fuse/datasources` and provides:

- Datasource names after `FROM` or `JOIN`
- Table names after `datasource.`
- Column names after `table.` or in `SELECT`, `WHERE`, `GROUP BY`
- SQL keywords and functions

Trigger manually with `Ctrl+Space`.

### Inline Query Execution

1. Write a query in any `.sql`, `.ppl`, or `.fuse` file
2. Place cursor on the query
3. Run: `Ctrl+Shift+Enter` (or Command Palette → "Fuse: Execute Query")
4. Results appear in a panel below the editor

Multiple queries in one file are separated by `;` or blank lines. The extension executes the query under the cursor.

### Result Panel

- Table view with sortable columns
- Export to CSV, JSON, or clipboard
- Row count and execution time in the status bar
- Click a column header to sort
- Pagination for large results (uses cursor pagination)

### EXPLAIN View

Run `Ctrl+Shift+E` (or "Fuse: Explain Query") to see the query plan:

- Tree view of plan nodes
- Pushdown indicators per connector
- Estimated cost and row counts
- With ANALYZE: actual timing and data_bytes

### Datasource Explorer

The sidebar shows a tree of all connected datasources:

```
📁 Datasources
├── 📦 cluster_a (opensearch)
│   ├── 📄 application_logs
│   │   ├── timestamp (datetime)
│   │   ├── service (keyword)
│   │   └── status (integer)
│   └── 📄 access_logs
├── 📦 ddb (dynamodb)
│   └── 📄 users
└── 📦 metrics (prometheus)
    └── 📄 node_cpu
```

Click a table to insert `SELECT * FROM datasource.table LIMIT 10` into the editor.

### Snippets

| Prefix | Expands To |
|--------|-----------|
| `fsel` | `SELECT $1 FROM $2 WHERE $3 LIMIT 100` |
| `fjoin` | `SELECT $1 FROM $2 a JOIN $3 b ON a.$4 = b.$5` |
| `funion` | `SELECT $1 FROM $2 UNION ALL SELECT $1 FROM $3` |
| `fexplain` | `EXPLAIN ANALYZE SELECT $1 FROM $2` |
| `fview` | `CREATE MATERIALIZED VIEW $1 AS SELECT $2 FROM $3` |

### Query History

Command Palette → "Fuse: Query History" shows recent queries with timing. Click to re-run or edit.

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+Shift+Enter` | Execute query |
| `Ctrl+Shift+E` | Explain query |
| `Ctrl+Shift+D` | Show datasource explorer |
| `Ctrl+Shift+H` | Query history |
| `Ctrl+Space` | Trigger autocomplete |

## Multi-Server Profiles

For multiple Fuse environments, use workspace settings:

```json
// .vscode/settings.json (per project)
{
  "fuse.serverUrl": "https://prod-fuse.internal:9400",
  "fuse.apiKey": "prod-key"
}
```

Switch between profiles using VS Code's workspace feature — each workspace can point to a different Fuse server.

## Troubleshooting

| Issue | Fix |
|-------|-----|
| No autocomplete | Check `fuse.serverUrl` is reachable, verify API key |
| "Connection refused" | Start Fuse server, check URL and port |
| Slow autocomplete | Schema is cached on first load; restart extension to refresh |
| Results truncated | Increase `fuse.maxRows` in settings |
| TLS errors | Add server CA to VS Code's certificate trust store |
