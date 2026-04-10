# Jupyter Notebook Integration Guide

Query Fuse from Python and Jupyter notebooks using the `fuse-client` SDK. Get results as DataFrames, visualize with matplotlib/plotly, and build interactive analysis notebooks.

## Setup

```bash
# Install the Fuse Python SDK
pip install -e sdk/python/

# For DataFrame and visualization support
pip install pandas matplotlib
```

## Connect

```python
from fuse_client import FuseClient

# Local
fuse = FuseClient("http://localhost:9400")

# Playground (with API key)
fuse = FuseClient("https://fuse.huanji.profile.aws.dev", api_key="your-key")
```

## Query → DataFrame

```python
import pandas as pd

result = fuse.query("""
    SELECT service, count(*) as errors, avg(response_time_ms) as avg_ms
    FROM cluster_a.application_logs
    WHERE status >= 500
    GROUP BY service
    ORDER BY errors DESC
""")

df = pd.DataFrame(result.rows, columns=result.columns)
df
```

Output:
```
       service  errors  avg_ms
0  api-gateway      42   234.5
1 auth-service      18   189.2
2 user-service       7   312.8
```

### Helper Function

```python
def fuse_df(sql, **kwargs):
    """Query Fuse and return a pandas DataFrame."""
    r = fuse.query(sql, **kwargs)
    return pd.DataFrame(r.rows, columns=r.columns)
```

Usage:
```python
df = fuse_df("SELECT * FROM cluster_a.application_logs LIMIT 100")
df.describe()
```

## Cross-Datasource JOIN

```python
df = fuse_df("""
    SELECT l.service, l.status, u.name, u.team
    FROM cluster_a.application_logs l
    JOIN dynamodb.fuse_user_profiles u ON l.user_id = u.user_id
    WHERE l.status >= 500
""")

# Errors by team
df.groupby('team')['status'].count().sort_values(ascending=False)
```

## Visualization

### matplotlib

```python
import matplotlib.pyplot as plt

df = fuse_df("""
    SELECT service, count(*) as errors
    FROM cluster_a.application_logs
    WHERE status >= 500
    GROUP BY service ORDER BY errors DESC
""")

df.plot.barh(x='service', y='errors', title='Errors by Service', color='#f85149')
plt.tight_layout()
plt.show()
```

### Time Series

```python
df = fuse_df("""
    SELECT DATE_TRUNC('hour', timestamp) as hour, count(*) as requests
    FROM cluster_a.application_logs
    GROUP BY hour ORDER BY hour
""")

df['hour'] = pd.to_datetime(df['hour'])
df.plot(x='hour', y='requests', title='Request Volume', figsize=(12, 4))
plt.show()
```

### Cross-Datasource Comparison

```python
df = fuse_df("""
    SELECT _datasource, service, count(*) as n
    FROM cluster_a.application_logs
    UNION ALL
    SELECT _datasource, service, count(*) as n
    FROM cluster_b.application_logs
    GROUP BY _datasource, service
""")

df.pivot_table(index='service', columns='_datasource', values='n', fill_value=0).plot.bar(
    title='Requests by Service × Cluster', stacked=True
)
plt.show()
```

## Cursor Pagination

For large result sets, page through results:

```python
# One page at a time
result = fuse.query("SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", page_size=100)
df_page1 = pd.DataFrame(result.rows, columns=result.columns)
print(f"Page 1: {len(df_page1)} rows, next_cursor: {result.next_cursor}")

# Next page
result2 = fuse.query(
    "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC",
    page_size=100, cursor=result.next_cursor
)

# Or fetch all pages automatically
result_all = fuse.query_all("SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", page_size=500)
df_all = pd.DataFrame(result_all.rows, columns=result_all.columns)
```

## Trace Reconstruction

```python
trace = fuse.trace("trace-001")

print(f"Trace {trace.trace_id}: {trace.total_spans} spans across {trace.datasources_matched}")
print(f"Search time: {trace.search_ms}ms")

# Convert to DataFrame for analysis
spans_df = pd.DataFrame(trace.spans)
spans_df
```

## EXPLAIN

```python
plan = fuse.explain("""
    SELECT l.service, count(*)
    FROM cluster_a.application_logs l
    JOIN dynamodb.fuse_user_profiles u ON l.user_id = u.user_id
    GROUP BY l.service
""")

print(plan['plan'])
```

## Health Monitoring

```python
health = fuse.health()
print(f"Status: {health['status']}")

for name, info in health['connectors'].items():
    print(f"  {name}: {info['status']} ({info.get('latency_ms', '?')}ms)")
```

## PPL Queries

```python
df = fuse_df(
    "source = cluster_a.application_logs | where status >= 500 | stats count() by service | sort - count()",
    format="ppl"
)
df
```

## Example Notebook Workflow

A typical analysis notebook:

```python
from fuse_client import FuseClient
import pandas as pd
import matplotlib.pyplot as plt

fuse = FuseClient("http://localhost:9400")
fuse_df = lambda sql, **kw: pd.DataFrame((r := fuse.query(sql, **kw)).rows, columns=r.columns)

# 1. Check what's connected
print(fuse.health()['status'])

# 2. Explore schema
for ds in fuse.datasources():
    print(f"{ds['id']} ({ds['connector_type']})")

# 3. Quick look at the data
df = fuse_df("SELECT * FROM cluster_a.application_logs LIMIT 5")
display(df)

# 4. Cross-datasource analysis
errors = fuse_df("""
    SELECT u.team, count(*) as errors, avg(l.response_time_ms) as avg_ms
    FROM cluster_a.application_logs l
    JOIN dynamodb.fuse_user_profiles u ON l.user_id = u.user_id
    WHERE l.status >= 500
    GROUP BY u.team ORDER BY errors DESC
""")

errors.plot.bar(x='team', y=['errors', 'avg_ms'], secondary_y='avg_ms',
                title='Errors & Latency by Team')
plt.show()

# 5. Trace a specific request
trace = fuse.trace("trace-001")
pd.DataFrame(trace.spans)[['datasource', 'timestamp']].sort_values('timestamp')
```

## Tips

- Use `fuse_df()` helper for one-liner queries
- Always add `LIMIT` for exploratory queries
- Use `query_all()` for full exports, `query()` with `page_size` for interactive paging
- PPL works too — pass `format="ppl"`
- For large notebooks, reuse a single `FuseClient` instance
