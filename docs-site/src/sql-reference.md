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

Supported functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.

## ORDER BY + LIMIT

```sql
SELECT service, response_time_ms
FROM cluster_a.application_logs
ORDER BY response_time_ms DESC
LIMIT 10
```

## UNION ALL (Cross-Cluster)

Combine results from multiple datasources:

```sql
SELECT service, status, message
FROM cluster_a.application_logs
UNION ALL
SELECT service, status, message
FROM cluster_b.application_logs
LIMIT 20
```

Cross-cluster queries add a `_datasource` column automatically. See [Data Provenance](./data-provenance.md).

## JOIN (Cross-Datasource)

Join data across different datasource types:

```sql
SELECT l.trace_id, l.service, l.status, s.level, s.message
FROM cluster_a.application_logs l
JOIN s3_o11y.logs s ON l.trace_id = s.trace_id
WHERE l.status >= 500
```

Fuse uses hash-join for cross-datasource JOINs. Both sides are fetched in parallel, then joined locally.

## Subqueries

```sql
SELECT service, cnt
FROM (
  SELECT service, count(*) as cnt
  FROM cluster_a.application_logs
  GROUP BY service
) sub
WHERE cnt > 10
ORDER BY cnt DESC
```

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

## Tips

- Always use `LIMIT` to avoid fetching entire indices
- Use `WHERE` clauses — they get pushed down to connectors for performance
- `SELECT *` works but explicit column lists enable projection pushdown
- Check the [EXPLAIN / ANALYZE](./explain-analyze.md) page to understand query plans
