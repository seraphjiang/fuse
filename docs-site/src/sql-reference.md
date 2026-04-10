# SQL Reference

Fuse supports standard SQL with `datasource.table` addressing for cross-datasource queries.

## SELECT

```sql
SELECT service, status, message
FROM cluster_a.application_logs
LIMIT 10
```

Table names use `datasource_id.table_name` format. The datasource ID comes from your `fuse.toml` config.

## WHERE

```sql
SELECT service, message
FROM cluster_a.application_logs
WHERE status >= 500 AND service = 'api-gateway'
LIMIT 20
```

Filters are pushed down to connectors when supported.

## GROUP BY + Aggregations

```sql
SELECT service, count(*) as errors, avg(response_time_ms) as avg_ms
FROM cluster_a.application_logs
WHERE status >= 500
GROUP BY service
ORDER BY errors DESC
```

Supported functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `PERCENTILE`, `PERCENTILE_APPROX`.

### Cross-Datasource GROUP BY

GROUP BY works across multiple datasources. Fuse executes partial aggregation at each source, then re-aggregates after merge:

```sql
SELECT service, count(*) as total
FROM cluster_a.application_logs
UNION ALL
SELECT service, count(*) as total
FROM cluster_b.application_logs
GROUP BY service
```

## ORDER BY + LIMIT

```sql
SELECT service, response_time_ms
FROM cluster_a.application_logs
ORDER BY response_time_ms DESC
LIMIT 10
```

Multi-column ORDER BY with mixed directions:

```sql
SELECT service, status, response_time_ms
FROM cluster_a.application_logs
ORDER BY service ASC, response_time_ms DESC
LIMIT 20
```

## UNION / UNION ALL

Combine results from multiple datasources:

```sql
-- UNION ALL (keep duplicates)
SELECT service, status, message FROM cluster_a.application_logs
UNION ALL
SELECT service, status, message FROM cluster_b.application_logs
LIMIT 20

-- UNION (deduplicate)
SELECT service FROM cluster_a.application_logs
UNION
SELECT service FROM cluster_b.application_logs
```

Cross-cluster queries add a `_datasource` column automatically. See [Data Provenance](./data-provenance.md).

Three or more sources:

```sql
SELECT source, service, message FROM cluster_a.application_logs
UNION ALL
SELECT source, service, message FROM cloudwatch.events
UNION ALL
SELECT source, service, message FROM s3_o11y.logs
LIMIT 50
```

## JOIN (Cross-Datasource)

Join data across different datasource types. Fuse fetches both sides in parallel, then performs a local hash join with the smaller table as the build side:

```sql
SELECT l.trace_id, l.service, u.name, u.role
FROM cluster_a.application_logs l
JOIN dynamodb.users u ON l.user_id = u.user_id
WHERE l.status >= 500
```

### Semi-Join (EXISTS)

Return rows from the left side that have a match in the right side:

```sql
SELECT * FROM cluster_a.application_logs
WHERE user_id IN (SELECT user_id FROM dynamodb.users WHERE role = 'admin')
```

### Anti-Join (NOT EXISTS)

Return rows from the left side that have no match in the right side:

```sql
SELECT * FROM cluster_a.application_logs
WHERE user_id NOT IN (SELECT user_id FROM dynamodb.users WHERE role = 'admin')
```

## Correlated Subqueries

`IN (SELECT ...)` subqueries across datasources. The inner query executes first, then results are inlined:

```sql
SELECT * FROM cluster_a.application_logs
WHERE trace_id IN (
  SELECT trace_id FROM s3_o11y.logs WHERE level = 'ERROR'
)
```

## Window Functions

Window functions operate over federated result sets:

```sql
SELECT service, status, response_time_ms,
  ROW_NUMBER() OVER (PARTITION BY service ORDER BY response_time_ms DESC) as rn,
  RANK() OVER (ORDER BY response_time_ms DESC) as rank
FROM cluster_a.application_logs
WHERE status >= 500
```

Supported: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`.

## CASE WHEN

```sql
SELECT service,
  CASE WHEN status >= 500 THEN 'error'
       WHEN status >= 400 THEN 'client_error'
       ELSE 'ok'
  END as category,
  count(*) as cnt
FROM cluster_a.application_logs
GROUP BY service, category
```

## Computed Columns

Expressions in SELECT:

```sql
SELECT service,
  response_time_ms / 1000.0 as response_time_sec,
  UPPER(service) as service_upper
FROM cluster_a.application_logs
LIMIT 10
```

## Date/Time Functions

```sql
SELECT DATE_TRUNC('hour', timestamp) as hour,
  count(*) as requests
FROM cluster_a.application_logs
GROUP BY hour
ORDER BY hour DESC
```

Supported: `DATE_TRUNC`, `DATE_DIFF`, `NOW()`.

## String Functions

`UPPER`, `LOWER`, `SUBSTRING`, `TRIM`, `REGEXP`.

## Math Functions

`ROUND`, `CEIL`, `FLOOR`, `ABS`, `MOD`.

## DISTINCT

```sql
SELECT DISTINCT service
FROM cluster_a.application_logs
```

## OFFSET (Pagination)

```sql
SELECT service, status
FROM cluster_a.application_logs
ORDER BY timestamp DESC
LIMIT 10 OFFSET 20
```

## Cursor Pagination

For efficient paging through large result sets, use `page_size` and `cursor` instead of OFFSET:

```bash
# First page
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "format": "sql", "page_size": 20}'

# Response includes next_cursor
# {"columns": [...], "rows": [...], "next_cursor": "eyJvZmZzZXQiOjIwfQ=="}

# Next page
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "format": "sql", "page_size": 20, "cursor": "eyJvZmZzZXQiOjIwfQ=="}'
```

## BETWEEN

```sql
SELECT service, status, response_time_ms
FROM cluster_a.application_logs
WHERE response_time_ms BETWEEN 100 AND 500
```

## HAVING

```sql
SELECT service, count(*) as errors
FROM cluster_a.application_logs
WHERE status >= 500
GROUP BY service
HAVING count(*) > 5
ORDER BY errors DESC
```

## Parameterized Queries

Use named parameters with `$name` syntax:

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT * FROM cluster_a.application_logs WHERE service = $svc AND status >= $code LIMIT $n",
    "format": "sql",
    "params": {"svc": "api-gateway", "code": 500, "n": 10}
  }'
```

## EXPLAIN

Inspect the query plan and cost estimates:

```sql
EXPLAIN SELECT l.service, count(*)
FROM cluster_a.application_logs l
JOIN dynamodb.users u ON l.user_id = u.user_id
GROUP BY l.service
```

See [EXPLAIN / ANALYZE](./explain-analyze.md) for details.

## Tips

- Always use `LIMIT` to avoid fetching entire indices
- Use `WHERE` clauses — they get pushed down to connectors for performance
- `SELECT *` works but explicit column lists enable projection pushdown
- For large result sets, use cursor pagination (`page_size` + `cursor`) instead of OFFSET
- Check [EXPLAIN / ANALYZE](./explain-analyze.md) to understand query plans
