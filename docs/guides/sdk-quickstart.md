# Fuse SDK Quickstart

Fuse provides official SDKs in Python, TypeScript/JavaScript, and Go.

## Python

```bash
pip install fuse-client
```

```python
from fuse_client import FuseClient

client = FuseClient("http://localhost:9400")
result = client.query("SELECT * FROM cluster_a.logs LIMIT 10")
print(result.columns)
print(result.to_dicts())
```

### Jupyter Notebook

```python
%load_ext fuse_client.magic

# Inline SQL
%fuse SELECT service, count(*) FROM cluster_a.logs GROUP BY service

# Multi-line
%%fuse
SELECT l.service, u.name
FROM cluster_a.logs l
JOIN dynamodb.users u ON l.user_id = u.user_id
LIMIT 20

# PPL
%%fuse ppl
source = cluster_a.logs | stats count() by service
```

Environment variables: `FUSE_URL` (default `http://localhost:9400`), `FUSE_API_KEY`.

### Pagination

```python
result = client.query_all("SELECT * FROM big_table", page_size=1000)
```

### Saved Queries

```python
client.save_query("daily_errors", "SELECT * FROM logs WHERE status >= 500")
queries = client.saved_queries()
client.delete_saved_query("daily_errors")
```

## TypeScript / JavaScript

```typescript
import { FuseClient } from 'fuse-client';

const client = new FuseClient({ baseUrl: 'http://localhost:9400' });
const result = await client.query('SELECT * FROM cluster_a.logs LIMIT 10');
console.log(result.columns, result.rows);
```

### Saved Queries

```typescript
await client.saveQuery('my_query', 'SELECT 1', 'description');
const saved = await client.savedQueries();
await client.deleteSavedQuery('my_query');
```

## Go

```go
import fuse "github.com/seraphjiang/fuse/sdk/go"

client := fuse.NewClient("http://localhost:9400")
result, err := client.Query("SELECT * FROM cluster_a.logs LIMIT 10", "sql")
dicts := result.ToDicts()
```

### API Key Auth

```go
client := fuse.NewClient("http://localhost:9400")
client.APIKey = "your-api-key"
```

### Pagination

```go
result, _ := client.QueryWithCursor("SELECT * FROM big_table", "sql", 1000, "")
// Use result.NextCursor for next page
```

## Common API Surface

All SDKs provide these methods:

| Method | Description |
|--------|-------------|
| `query(sql, format)` | Execute SQL or PPL query |
| `explain(sql, format)` | Get query execution plan |
| `health()` | Check connector health |
| `datasources()` | List connected datasources |
| `history()` | Get query history |
| `savedQueries()` | List saved queries |
| `saveQuery(name, sql)` | Save a query |
| `deleteSavedQuery(name)` | Delete a saved query |
| `trace(traceId)` | Reconstruct distributed trace |
