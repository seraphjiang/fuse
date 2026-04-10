# Performance Tuning Guide

How to get the most out of Fuse — from query design to server configuration.

## 1. Query Design

### Use Pushdown

The single biggest performance lever. When Fuse pushes operations to connectors, less data crosses the network.

```sql
-- ❌ Slow: fetches all rows, filters locally
SELECT * FROM my_os.logs

-- ✅ Fast: filter + projection + limit pushed to OpenSearch
SELECT service, status FROM my_os.logs WHERE status >= 500 LIMIT 100
```

Check what got pushed down with `analyze: true`:

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT service FROM my_os.logs WHERE status >= 500 LIMIT 10", "analyze": true}'
```

Look at `pushdown` badges on RemoteScan nodes in the execution profile. More badges = less data transferred = faster.

### Pushdown by Connector

| Connector | Filter | Projection | Aggregation | Sort | Limit |
|-----------|--------|------------|-------------|------|-------|
| OpenSearch | ✅ | ✅ | ✅ | ✅ | ✅ |
| Elasticsearch | ✅ | ✅ | ✅ | ✅ | ✅ |
| PostgreSQL | ✅ Full SQL | ✅ | ✅ | ✅ | ✅ |
| MySQL | ✅ Full SQL | ✅ | ✅ | ✅ | ✅ |
| ClickHouse | ✅ Full SQL | ✅ | ✅ | ✅ | ✅ |
| DynamoDB | ✅ | ✅ | ❌ | ❌ | ✅ |
| MongoDB | ✅ | ✅ | ❌ | ❌ | ✅ |
| S3 Parquet | ❌ | ✅ Column pruning | ❌ | ❌ | ✅ |
| Prometheus | ✅ Time range | ❌ | ❌ | ❌ | ❌ |
| InfluxDB | ✅ WHERE | ❌ | ❌ | ❌ | ❌ |
| CloudWatch | ✅ Filter pattern | ❌ | ❌ | ❌ | ❌ |
| Redis | ✅ Key pattern | ❌ | ❌ | ❌ | ❌ |
| CSV/JSON | ❌ | ❌ | ❌ | ❌ | ❌ |

**Rule of thumb:** SQL-native connectors (PG, MySQL, ClickHouse) push down everything. Document stores (OS, ES, DDB, Mongo) push down filters and projections. File-based connectors (S3, CSV) push down very little — always use LIMIT.

### Always Use LIMIT

Queries without LIMIT fetch entire tables. For UNION ALL, each source gets the full limit — the global limit is applied after merge.

```sql
-- Each source fetches up to 10,000 rows (default fan-out limit)
SELECT * FROM os.logs UNION ALL SELECT * FROM cw.logs

-- Each source fetches up to 100 rows
SELECT * FROM os.logs UNION ALL SELECT * FROM cw.logs LIMIT 100
```

### Use Explicit Column Lists

`SELECT *` transfers all columns. Explicit projections enable column pruning, especially important for wide tables and Parquet files:

```sql
-- ❌ Transfers all 30 columns
SELECT * FROM s3.events LIMIT 100

-- ✅ Transfers only 3 columns (10x less data for Parquet)
SELECT timestamp, service, status FROM s3.events LIMIT 100
```

### Use Cursor Pagination for Large Results

OFFSET-based pagination re-scans from the start each time. Cursor pagination is stateless and efficient:

```bash
# First page
curl -X POST .../api/fuse/query \
  -d '{"query": "SELECT * FROM os.logs ORDER BY timestamp DESC", "page_size": 50}'

# Next page (no re-scan)
curl -X POST .../api/fuse/query \
  -d '{"query": "SELECT * FROM os.logs ORDER BY timestamp DESC", "page_size": 50, "cursor": "..."}'
```

For OpenSearch, deep pagination (>10k rows) automatically uses `search_after` instead of `from+size`.

## 2. JOIN Optimization

### Smaller Table as Build Side

Fuse automatically selects the smaller table as the hash join build side. If you know which side is smaller, put it on the right:

```sql
-- DynamoDB users (50 rows) is build side, OS logs (10k rows) is probe side
SELECT l.*, u.name
FROM os.logs l JOIN ddb.users u ON l.user_id = u.user_id
```

### Semi-Join for Filtered JOINs

When the build side is small enough, Fuse extracts join keys and pushes them as an `IN` filter to the probe side — reducing data transfer:

```sql
-- Small subquery → keys pushed as IN filter to OpenSearch
SELECT * FROM os.logs
WHERE user_id IN (SELECT user_id FROM ddb.users WHERE role = 'admin')
```

### Avoid JOINing Large Tables

Cross-datasource JOINs fetch both sides fully. If both sides are large, add WHERE clauses to reduce row counts before the join:

```sql
-- ❌ Fetches all logs + all events
SELECT * FROM os.logs JOIN cw.events ON trace_id = trace_id

-- ✅ Filter both sides first
SELECT * FROM os.logs l JOIN cw.events e ON l.trace_id = e.trace_id
WHERE l.status >= 500 AND e.level = 'ERROR'
```

## 3. Caching

### Query Result Cache

Fuse caches query results per connector with TTL-based expiry:

| Connector Type | Default TTL |
|---------------|-------------|
| OpenSearch | 30s |
| S3 | 5 min |
| Prometheus | 60s |
| All others | 30s |

Identical queries to the same connector hit the cache. Different WHERE values = cache miss.

### Plan Cache

Parsed query plans are cached by query string. Repeated queries skip SQL parsing and source extraction. The plan cache stores source references, ORDER BY, LIMIT, DISTINCT, and OFFSET — so re-execution only does the fan-out.

## 4. Server Configuration

### Engine Settings

```toml
[engine]
bind = "0.0.0.0:9400"
max_concurrent_queries = 64    # Global query concurrency limit
default_timeout = "30s"        # Default per-query timeout
rate_limit_global = 1000       # Requests per minute (global)
rate_limit_per_ip = 100        # Requests per minute (per IP)
```

**Tuning:**
- `max_concurrent_queries`: increase for high-throughput workloads, decrease if connectors are overloaded
- `default_timeout`: increase for complex cross-datasource queries, decrease to fail fast
- `rate_limit_per_ip`: increase for trusted internal clients, keep low for public-facing deployments

### Per-Connector Settings

```toml
[[connector]]
id = "my_os"
type = "opensearch"
url = "https://..."
max_concurrent_queries = 16    # Concurrent queries to this connector
scroll_size = 1000             # Rows per scroll page (OpenSearch)
request_timeout = "30s"        # Per-request timeout to this connector
```

**Tuning:**
- `max_concurrent_queries`: match your datasource's capacity. OpenSearch Serverless handles 16+ well; a small PostgreSQL instance may need 4-8
- `scroll_size`: larger = fewer round trips but more memory. 1000 is a good default for OpenSearch
- `request_timeout`: set lower than `default_timeout` so connector timeouts are reported as partial errors rather than full query failures

## 5. Cost Estimator

Fuse's cost estimator decides whether to push operations down or compute locally. It factors in:

- **Latency class**: Low (1x), Medium (3x), High (10x) multiplier on network cost
- **Filter selectivity**: estimated 10% of rows pass a filter
- **Aggregation reduction**: estimated 1% of rows after GROUP BY
- **Column ratio**: fewer projected columns = less data transferred

Use `EXPLAIN` to see estimated costs:

```bash
curl -X POST .../api/fuse/query/explain \
  -d '{"query": "SELECT service, count(*) FROM os.logs GROUP BY service"}'
```

If a connector has high latency (e.g., cross-region S3), the estimator favors pushdown more aggressively.

## 6. Monitoring

### Query History

```bash
curl http://localhost:9400/api/fuse/history
```

Shows recent queries with latency. Look for:
- Queries with high `latency_ms` — candidates for optimization
- Queries with `error` — failing connectors

### Query Stats

```bash
curl http://localhost:9400/api/fuse/stats
```

Aggregated counts: total queries, success rate, average latency.

### Prometheus Metrics

```bash
curl http://localhost:9400/metrics
```

Exposes query count, latency histogram, and connector health for Prometheus scraping.

### Health Check

```bash
curl http://localhost:9400/api/fuse/health
```

Per-connector health with latency. A `degraded` connector slows all queries that touch it.

## Checklist

- [ ] All queries have `LIMIT`
- [ ] Explicit column lists instead of `SELECT *`
- [ ] `WHERE` clauses on columns the connector can push down
- [ ] `analyze: true` confirms pushdown is happening
- [ ] JOINs have filters on both sides
- [ ] `max_concurrent_queries` matches connector capacity
- [ ] `request_timeout` < `default_timeout`
- [ ] Dashboard panels use auto-refresh intervals ≥ cache TTL
- [ ] Cursor pagination for large result sets
