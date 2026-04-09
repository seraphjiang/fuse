# EXPLAIN / ANALYZE

Fuse provides two ways to inspect query execution: EXPLAIN (plan only) and ANALYZE (plan + actual timing).

## EXPLAIN

Shows the execution plan without running the query:

```bash
curl -X POST http://localhost:9400/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs UNION ALL SELECT * FROM cluster_b.application_logs LIMIT 20"}'
```

Response includes a text plan and structured tree:

```json
{
  "plan": "Merge\n  RemoteScan [cluster_a] application_logs (est. 1000 rows, cost: 1.0)\n  RemoteScan [cluster_b] application_logs (est. 1000 rows, cost: 1.0)",
  "plan_tree": {
    "op": "Merge",
    "children": [
      { "op": "RemoteScan", "detail": "cluster_a.application_logs", "estimated_rows": 1000, "estimated_cost": 1.0 },
      { "op": "RemoteScan", "detail": "cluster_b.application_logs", "estimated_rows": 1000, "estimated_cost": 1.0 }
    ]
  }
}
```

## ANALYZE

Run the query and get actual execution metrics. In the playground, check the **Analyze** checkbox. Via API, set `analyze: true`:

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs LIMIT 10", "analyze": true}'
```

The response includes `execution_profile` in metadata:

```json
{
  "execution_profile": {
    "total_ms": 112,
    "nodes": [
      {
        "op": "Merge",
        "actual_rows": 20,
        "actual_ms": 112,
        "children": [
          {
            "op": "RemoteScan",
            "datasource": "cluster_a",
            "actual_rows": 12,
            "actual_ms": 45,
            "data_bytes": 4096,
            "pushdown": ["filter", "projection", "limit"]
          },
          {
            "op": "RemoteScan",
            "datasource": "cluster_b",
            "actual_rows": 8,
            "actual_ms": 67,
            "data_bytes": 3200,
            "pushdown": ["filter", "limit"]
          }
        ]
      }
    ]
  }
}
```

## Reading the Plan Tree

| Node | Meaning |
|------|---------|
| **Merge** | Combines results from multiple sources |
| **RemoteScan** | Fetches data from a connector |
| **HashJoin** | Local hash-join of two datasources |
| **Filter** | Local filter (not pushed down) |
| **Projection** | Local column selection |
| **Sort** | Local sorting |
| **Limit** | Local row limit |

## Pushdown Badges

RemoteScan nodes show which operations were pushed to the connector:

- **filter** — WHERE clause sent to connector
- **projection** — Only selected columns fetched
- **limit** — Row limit applied at source
- **aggregation** — GROUP BY executed at source

More pushdown = less data transferred = faster queries.

## Optimizing Slow Queries

1. **Check pushdown** — If RemoteScan has no pushdown badges, the connector doesn't support it. Add explicit WHERE/LIMIT.
2. **Compare latencies** — If one RemoteScan is much slower, that connector may be overloaded or distant.
3. **Reduce data** — Use projection (explicit column list) to avoid transferring unused fields.
4. **Add LIMIT** — Especially for exploratory queries.
5. **Check data size** — `data_bytes` shows how much data each connector returned. Large values suggest missing filters.

## Playground

In the playground UI, the plan tree renders as an indented text tree with:
- Color-coded timing: **green** (<50ms), **yellow** (<200ms), **red** (>200ms)
- Pushdown badges on RemoteScan nodes
- Data size per node
