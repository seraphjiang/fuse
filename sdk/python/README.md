# Fuse Python SDK

Python client for the [Fuse](https://github.com/seraphjiang/fuse) federated query engine.

## Install

```bash
pip install fuse-client
```

## Quick Start

```python
from fuse_client import FuseClient

client = FuseClient("http://localhost:3000", api_key="your-key")

# Query across datasources
result = client.query("SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM s3.logs LIMIT 10")
for row in result.to_dicts():
    print(row)

# Auto-paginate large results
all_rows = client.query_all("SELECT * FROM cluster_a.logs", page_size=500)

# Trace reconstruction
trace = client.trace("abc-123")
for span in trace.spans:
    print(f"{span['datasource']}: {span['timestamp']}")

# Health check
health = client.health()

# Explain plan
plan = client.explain("SELECT * FROM ds.table WHERE status >= 500")
```

## API

- `query(sql, format, params, page_size, cursor)` → `QueryResult`
- `query_all(sql, format, page_size)` → `QueryResult` (auto-paginate)
- `explain(sql, format)` → dict
- `validate(sql, format)` → dict
- `health()` → dict
- `datasources()` → list
- `trace(trace_id)` → `TraceResult`
- `history()` → list

## License

Apache-2.0
