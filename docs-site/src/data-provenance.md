# Data Provenance

When a query touches multiple datasources, Fuse adds a `_datasource` column to every row indicating which datasource produced it.

## The `_datasource` Column

For cross-datasource queries (UNION ALL, multi-source PPL, JOINs), each row gets tagged:

```sql
SELECT _datasource, service, status
FROM cluster_a.application_logs
UNION ALL
SELECT _datasource, service, status
FROM cluster_b.application_logs
LIMIT 10
```

Result:

| _datasource | service | status |
|-------------|---------|--------|
| cluster_a | api-gateway | 500 |
| cluster_a | auth-service | 200 |
| cluster_b | order-service | 500 |
| cluster_b | payment-service | 200 |

## Datasource Stats

The response metadata includes per-datasource statistics:

```json
{
  "metadata": {
    "datasources_queried": ["cluster_a", "cluster_b"],
    "datasource_stats": {
      "cluster_a": { "rows": 12, "latency_ms": 45 },
      "cluster_b": { "rows": 8, "latency_ms": 67 }
    }
  }
}
```

In the playground UI, this appears as a color-coded provenance bar below the results table.

## Cross-Cluster Best Practices

1. **Use LIMIT** — Cross-cluster queries can return large result sets. Always limit.
2. **Filter early** — WHERE clauses push down to each connector, reducing data transfer.
3. **Check provenance** — Use `_datasource` to verify data is coming from expected sources.
4. **Monitor latency** — `datasource_stats` shows which connector is slow. A slow connector delays the entire query.
5. **Use EXPLAIN** — Check the execution plan to verify pushdown is working.
