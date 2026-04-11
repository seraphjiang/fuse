# Query Optimization Guide

How to read EXPLAIN output, identify bottlenecks, and optimize federated queries.

## Reading EXPLAIN Output

### Basic EXPLAIN

```bash
curl -X POST http://localhost:9400/api/fuse/query/explain \
  -d '{"query": "SELECT service, count(*) FROM cluster_a.logs GROUP BY service"}'
```

Returns a text plan and a structured `plan_tree`:

```json
{
  "plan": "FederatedQuery → RemoteScan(cluster_a) [pushdown: filter, projection, aggregation]",
  "plan_tree": {
    "op": "FederatedQuery",
    "children": [
      {
        "op": "RemoteScan",
        "datasource": "cluster_a",
        "pushdown": ["filter", "projection", "aggregation"]
      }
    ]
  }
}
```

### EXPLAIN ANALYZE

Add `"analyze": true` to execute the query and get actual timing:

```bash
curl -X POST http://localhost:9400/api/fuse/query/explain \
  -d '{"query": "SELECT l.service, u.team FROM cluster_a.logs l JOIN ddb.users u ON l.user_id = u.user_id", "analyze": true}'
```

```json
{
  "plan_tree": {
    "op": "HashJoin",
    "actual_rows": 1250,
    "actual_ms": 87,
    "estimated_rows": 20000,
    "estimated_cost": 870.0,
    "estimate_accuracy": "16.0x (est 20000 vs actual 1250)",
    "children": [
      {
        "op": "RemoteScan",
        "datasource": "cluster_a",
        "actual_rows": 5000,
        "actual_ms": 45,
        "data_bytes": 512000,
        "pushdown": ["projection"],
        "detail": "Scan cluster_a"
      },
      {
        "op": "RemoteScan",
        "datasource": "ddb",
        "actual_rows": 200,
        "actual_ms": 32,
        "data_bytes": 24000,
        "pushdown": ["projection"],
        "detail": "Scan ddb"
      }
    ]
  }
}
```

### Key Fields

| Field | Meaning |
|-------|---------|
| `op` | Operation type: RemoteScan, HashJoin, Sort, Aggregate, Filter, Union, Limit |
| `actual_rows` | Rows produced (ANALYZE only) |
| `actual_ms` | Wall-clock time in milliseconds (ANALYZE only) |
| `data_bytes` | Bytes transferred from connector |
| `estimated_rows` | Planner's row estimate |
| `estimated_cost` | Relative cost (lower = cheaper) |
| `estimate_accuracy` | Ratio of estimated vs actual — high ratios indicate bad estimates |
| `pushdown` | Operations pushed to the connector: filter, projection, aggregation, sort, limit |
| `children` | Child operations (tree structure) |

## Identifying Bottlenecks

### 1. Check Pushdown

The most impactful optimization. Compare what's pushed down vs what Fuse does in-memory:

```
RemoteScan(cluster_a) pushdown: [projection]          ← only projection pushed
  → 5000 rows, 512KB transferred

RemoteScan(cluster_a) pushdown: [filter, projection]   ← filter also pushed
  → 50 rows, 5KB transferred                           ← 100x less data
```

If `filter` is missing from pushdown, the connector doesn't support it or the filter can't be translated. See [Pushdown by Connector](#pushdown-by-connector).

### 2. Check estimate_accuracy

A ratio far from 1.0x means the planner made a bad estimate:

- `16.0x` — planner overestimated by 16x → may have chosen wrong join order
- `0.1x` — planner underestimated by 10x → may have skipped useful optimizations

### 3. Check data_bytes

Large `data_bytes` on a RemoteScan means too much data is being pulled:

| data_bytes | Action |
|-----------|--------|
| < 100KB | Fine |
| 100KB–1MB | Check if filters can be pushed down |
| 1MB–10MB | Add WHERE clauses, reduce columns |
| > 10MB | Add LIMIT, use cursor pagination, or create a materialized view |

### 4. Check actual_ms Distribution

If one child takes 90% of the time, that's your bottleneck:

```
HashJoin: 87ms total
  ├── RemoteScan(cluster_a): 45ms  ← 52% — slow connector or large scan
  └── RemoteScan(ddb): 32ms        ← 37%
       join overhead: 10ms          ← 11% — fine
```

## Optimization Techniques

### Add WHERE Clauses for Pushdown

```sql
-- Bad: scans all logs, filters in Fuse
SELECT * FROM cluster_a.logs WHERE service = 'api'

-- Good: same query, but OpenSearch pushes the filter down
-- (this happens automatically — just make sure the filter is pushdown-compatible)
```

Pushdown-compatible filters: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`.

Not pushed down: `ILIKE` (some connectors), functions on columns (`WHERE UPPER(service) = 'API'`), correlated subqueries.

### Select Only Needed Columns

```sql
-- Bad: transfers all columns
SELECT * FROM cluster_a.logs LIMIT 100

-- Good: transfers only 2 columns
SELECT service, timestamp FROM cluster_a.logs LIMIT 100
```

### Use LIMIT

```sql
-- Bad: fetches all matching rows, then truncates
SELECT service FROM cluster_a.logs WHERE status >= 500

-- Good: connector stops after 100 rows
SELECT service FROM cluster_a.logs WHERE status >= 500 LIMIT 100
```

### Optimize JOINs

Put the smaller table on the right (build side of hash join):

```sql
-- Good: 200 users (small) builds the hash table, 5000 logs probe it
SELECT l.service, u.team
FROM cluster_a.logs l
JOIN ddb.users u ON l.user_id = u.user_id
```

Add filters before the JOIN to reduce both sides:

```sql
SELECT l.service, u.team
FROM cluster_a.logs l
JOIN ddb.users u ON l.user_id = u.user_id
WHERE l.status >= 500          -- reduces logs from 5000 → 50
  AND u.role = 'admin'         -- reduces users from 200 → 10
```

### Use Materialized Views for Repeated Queries

```toml
[[view]]
name = "error_summary"
query = "SELECT service, count(*) as errors FROM cluster_a.logs WHERE status >= 500 GROUP BY service"
refresh_secs = 300
```

```sql
-- Served from cache (sub-millisecond)
SELECT * FROM view.error_summary
```

### Use Cursor Pagination for Large Results

```sql
-- First page
SELECT * FROM cluster_a.logs ORDER BY timestamp DESC LIMIT 500

-- Subsequent pages use cursor (no re-scanning)
-- Pass cursor from previous response's next_cursor field
```

## Pushdown by Connector

| Connector | Filter | Projection | Limit | Sort | Aggregation |
|-----------|--------|-----------|-------|------|-------------|
| OpenSearch | ✅ | ✅ | ✅ | ✅ | ✅ |
| Elasticsearch | ✅ | ✅ | ✅ | ✅ | ✅ |
| PostgreSQL | ✅ | ✅ | ✅ | ✅ | ✅ |
| MySQL | ✅ | ✅ | ✅ | ✅ | ✅ |
| DynamoDB | ✅ filter/key | ✅ | ✅ | ❌ | ❌ |
| S3 Parquet | ✅ predicate | ✅ | ❌ | ❌ | ❌ |
| S3 O11y | ✅ time range | ✅ | ✅ | ❌ | ❌ |
| Prometheus | ✅ label/time | ✅ | ✅ | ❌ | ❌ |
| CloudWatch | ✅ time/group | ✅ | ✅ | ❌ | ❌ |
| Redis | ✅ key pattern | ❌ | ✅ | ❌ | ❌ |
| CSV/JSON | ❌ | ✅ | ❌ | ❌ | ❌ |
| MongoDB | ✅ | ✅ | ✅ | ✅ | ✅ |
| InfluxDB | ✅ tag/time | ✅ | ✅ | ✅ | ✅ |
| ClickHouse | ✅ | ✅ | ✅ | ✅ | ✅ |
| DuckDB | ✅ | ✅ | ✅ | ✅ | ✅ |

## Query Advisor

Get automated optimization suggestions:

```bash
curl "http://localhost:9400/api/fuse/advisor?query=SELECT+*+FROM+cluster_a.logs"
```

Common suggestions:
- "Add LIMIT to avoid full table scan"
- "Use explicit columns instead of SELECT *"
- "Add WHERE clause for filter pushdown"
- "Consider cursor pagination for results > 10,000 rows"
- "This query is a good candidate for a materialized view"
