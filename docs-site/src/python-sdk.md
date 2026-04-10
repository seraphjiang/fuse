# Python SDK Quick Start

Get querying in 5 minutes.

## Install

```bash
pip install -e sdk/python/
```

## Connect

```python
from fuse_client import FuseClient

# Local
fuse = FuseClient("http://localhost:9400")

# With API key
fuse = FuseClient("http://localhost:9400", api_key="your-key")
```

## Query

```python
result = fuse.query("SELECT service, count(*) as n FROM cluster_a.application_logs GROUP BY service ORDER BY n DESC LIMIT 5")

print(result.columns)  # ['service', 'n']
print(result.rows)     # [['api-gateway', 42], ['auth-service', 18], ...]
print(result.total_rows)
```

### As DataFrame

```python
import pandas as pd

df = pd.DataFrame(result.rows, columns=result.columns)
df
```

### PPL

```python
result = fuse.query(
    "source = cluster_a.application_logs | where status >= 500 | stats count() by service",
    format="ppl"
)
```

### Parameters

```python
result = fuse.query(
    "SELECT * FROM cluster_a.application_logs WHERE service = $svc AND status >= $code LIMIT $n",
    params={"svc": "api-gateway", "code": 500, "n": 10}
)
```

## Paginate

```python
# Page by page
page1 = fuse.query("SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", page_size=50)
print(f"{len(page1.rows)} rows, cursor: {page1.next_cursor}")

page2 = fuse.query(
    "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC",
    page_size=50, cursor=page1.next_cursor
)

# Or fetch everything
all_results = fuse.query_all("SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", page_size=500)
print(f"{all_results.total_rows} total rows")
```

## Trace

```python
trace = fuse.trace("trace-001")

print(f"{trace.total_spans} spans from {trace.datasources_matched} in {trace.search_ms}ms")
for span in trace.spans:
    print(f"  [{span['datasource']}] {span.get('timestamp', '?')} — {span['fields'].get('service', '?')}")
```

## Explore

```python
# Health
print(fuse.health()['status'])

# Datasources
for ds in fuse.datasources():
    print(f"  {ds['id']} ({ds['connector_type']})")

# Explain
plan = fuse.explain("SELECT * FROM cluster_a.application_logs LIMIT 10")
print(plan['plan'])

# Validate
print(fuse.validate("SELECT * FROM cluster_a.application_logs")['valid'])

# History
for entry in fuse.history()[:3]:
    print(f"  {entry['query'][:60]}... ({entry['latency_ms']}ms)")
```

## Error Handling

```python
from fuse_client import FuseError

try:
    fuse.query("SELECT * FROM nonexistent.table")
except FuseError as e:
    print(f"HTTP {e.status_code}: {e.body}")
```

## Next Steps

- [Jupyter Integration Guide](jupyter-integration-guide.md) — DataFrames, visualization, full notebook workflow
- [API Reference](api-reference-guide.md) — all endpoints
- [SQL Reference](https://seraphjiang.github.io/fuse/sql-reference.html) — JOINs, UNION, window functions, CTEs
