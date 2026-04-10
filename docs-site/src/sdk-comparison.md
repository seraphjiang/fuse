# SDK Comparison: Python vs TypeScript

Both SDKs provide the same functionality with zero dependencies. Choose based on your environment.

## Quick Comparison

| | Python SDK | TypeScript SDK |
|---|-----------|---------------|
| Install | `pip install fuse-client` | `npm install fuse-client` |
| Dependencies | None (stdlib `urllib`) | None (native `fetch`) |
| Async | `async/await` optional | `async/await` required |
| DataFrame | `pandas` integration | — |
| Environments | Python 3.8+, Jupyter | Node.js, Deno, browsers |
| Typing | Dataclasses | Full TypeScript types |
| Streaming | Not yet | Not yet |

## When to Use Python

- **Jupyter notebooks** — query → DataFrame → matplotlib/plotly in one cell
- **Data science workflows** — pandas, numpy, scikit-learn pipelines
- **ETL scripts** — extract from Fuse, transform in Python, load elsewhere
- **Quick analysis** — REPL-friendly, `fuse_df()` one-liner

```python
import pandas as pd
from fuse_client import FuseClient

fuse = FuseClient("http://localhost:9400", api_key="key")

# One-liner: query → DataFrame
df = pd.DataFrame((r := fuse.query("SELECT service, count(*) as n FROM os.logs GROUP BY service")).rows, columns=r.columns)

# Visualization
df.plot.bar(x='service', y='n')
```

## When to Use TypeScript

- **Web applications** — browser-native, works with React/Vue/Angular
- **Node.js services** — backend integration, microservices
- **Grafana plugin development** — same language as the plugin
- **Type safety** — full TypeScript interfaces for all responses

```typescript
import { FuseClient } from 'fuse-client';

const fuse = new FuseClient({ baseUrl: 'http://localhost:9400', apiKey: 'key' });

// Full type safety
const result = await fuse.query('SELECT service, count(*) as n FROM os.logs GROUP BY service');
// result.columns: string[], result.rows: unknown[][], result.nextCursor?: string
```

## API Parity

Both SDKs expose identical methods:

| Method | Python | TypeScript |
|--------|--------|-----------|
| Query | `fuse.query(sql, format, params, page_size, cursor)` | `fuse.query(sql, { format, params, pageSize, cursor })` |
| Paginate all | `fuse.query_all(sql, page_size)` | `fuse.queryAll(sql, { pageSize })` |
| Explain | `fuse.explain(sql)` | `fuse.explain(sql)` |
| Validate | `fuse.validate(sql)` | `fuse.validate(sql)` |
| Health | `fuse.health()` | `fuse.health()` |
| Datasources | `fuse.datasources()` | `fuse.datasources()` |
| Trace | `fuse.trace(id)` | `fuse.trace(id)` |
| History | `fuse.history()` | `fuse.history()` |

### Return Types

| | Python | TypeScript |
|---|--------|-----------|
| Query result | `QueryResult` dataclass | `QueryResult` interface |
| Trace result | `TraceResult` dataclass | `TraceResult` interface |
| Errors | `FuseError(status_code, body)` | `FuseError` with `.statusCode`, `.body` |
| Rows | `list[list[Any]]` | `unknown[][]` |

## Side-by-Side Examples

### Cross-Datasource JOIN

**Python:**
```python
result = fuse.query("""
    SELECT l.service, u.team, count(*) as errors
    FROM os.logs l JOIN ddb.users u ON l.user_id = u.user_id
    WHERE l.status >= 500 GROUP BY l.service, u.team
""")
for row in result.rows:
    print(f"{row[0]} ({row[1]}): {row[2]} errors")
```

**TypeScript:**
```typescript
const result = await fuse.query(`
    SELECT l.service, u.team, count(*) as errors
    FROM os.logs l JOIN ddb.users u ON l.user_id = u.user_id
    WHERE l.status >= 500 GROUP BY l.service, u.team
`);
for (const row of result.rows) {
    console.log(`${row[0]} (${row[1]}): ${row[2]} errors`);
}
```

### Cursor Pagination

**Python:**
```python
all_data = fuse.query_all("SELECT * FROM os.logs ORDER BY timestamp DESC", page_size=500)
print(f"{all_data.total_rows} rows fetched")
```

**TypeScript:**
```typescript
const allData = await fuse.queryAll('SELECT * FROM os.logs ORDER BY timestamp DESC', { pageSize: 500 });
console.log(`${allData.totalRows} rows fetched`);
```

### Error Handling

**Python:**
```python
from fuse_client import FuseError
try:
    fuse.query("SELECT * FROM bad.table")
except FuseError as e:
    print(f"HTTP {e.status_code}: {e.body}")
```

**TypeScript:**
```typescript
import { FuseError } from 'fuse-client';
try {
    await fuse.query('SELECT * FROM bad.table');
} catch (e) {
    if (e instanceof FuseError) console.error(`HTTP ${e.statusCode}: ${e.body}`);
}
```

## Using Both Together

In a full-stack app, use TypeScript for the frontend/API layer and Python for data analysis:

```
Browser (React + TS SDK) → Fuse Server → Connectors
                                ↑
Jupyter (Python SDK) ───────────┘
```

Both SDKs hit the same REST API — results are identical regardless of which SDK you use.
