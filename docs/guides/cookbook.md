# Fuse Cookbook

20 real-world recipes for federated query workloads.

---

## 1. Cross-Datasource Error Correlation

**Problem:** Correlate application errors in OpenSearch with user profiles in DynamoDB and service ownership in PostgreSQL.

```sql
SELECT l.service, u.name, u.team, p.oncall_email, count(*) as errors
FROM opensearch.application_logs l
JOIN dynamodb.users u ON l.user_id = u.user_id
JOIN postgres.service_registry p ON l.service = p.service_name
WHERE l.status >= 500
  AND l.timestamp > NOW() - INTERVAL '1 hour'
GROUP BY l.service, u.name, u.team, p.oncall_email
ORDER BY errors DESC
LIMIT 20
```

**How it works:** Fuse pushes the timestamp and status filters to OpenSearch, fetches matching user IDs from DynamoDB, joins with PostgreSQL service registry, then aggregates locally.

**Tip:** Add the time filter first — it's the most selective and pushes down to OpenSearch's time-based indices.

---

## 2. Time-Series Anomaly Detection

**Problem:** Detect services with error rates more than 3 standard deviations above their 7-day average.

```sql
WITH hourly AS (
    SELECT service,
           DATE_TRUNC('hour', timestamp) as hour,
           count(*) as errors
    FROM opensearch.application_logs
    WHERE status >= 500
      AND timestamp > NOW() - INTERVAL '7 days'
    GROUP BY service, DATE_TRUNC('hour', timestamp)
),
stats AS (
    SELECT service,
           AVG(errors) as avg_errors,
           STDDEV(errors) as stddev_errors
    FROM hourly
    GROUP BY service
),
latest AS (
    SELECT service, count(*) as current_errors
    FROM opensearch.application_logs
    WHERE status >= 500
      AND timestamp > NOW() - INTERVAL '1 hour'
    GROUP BY service
)
SELECT l.service, l.current_errors, s.avg_errors, s.stddev_errors,
       (l.current_errors - s.avg_errors) / NULLIF(s.stddev_errors, 0) as z_score
FROM latest l
JOIN stats s ON l.service = s.service
WHERE (l.current_errors - s.avg_errors) / NULLIF(s.stddev_errors, 0) > 3
ORDER BY z_score DESC
```

**How it works:** Three CTEs compute hourly error counts, per-service statistics, and current-hour counts. The final query identifies anomalies via z-score.

**Tip:** Create a materialized view for the `stats` CTE to avoid recomputing the 7-day window on every query.

---

## 3. Multi-Tenant SaaS Query Layer

**Problem:** Serve analytics to SaaS customers where each tenant sees only their data.

```toml
# fuse.toml
[[api_key]]
key = "ak-tenant-acme"
identity = "acme"
role = "viewer"

[[tenant]]
id = "acme"
datasources = ["acme_cluster"]
max_rows = 10000
max_time_ms = 15000

[[api_key]]
key = "ak-tenant-globex"
identity = "globex"
role = "viewer"

[[tenant]]
id = "globex"
datasources = ["globex_cluster"]
max_rows = 10000
max_time_ms = 15000
```

```sql
-- Acme's API key can only run this against acme_cluster
SELECT service, count(*) as requests, avg(latency_ms) as avg_latency
FROM acme_cluster.access_logs
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY service
ORDER BY requests DESC
```

**How it works:** Tenant isolation ensures `acme` can never query `globex_cluster`. The query governor caps results at 10K rows and 15s execution.

**Tip:** Use Redis-backed tenant registry in stateless mode for zero-downtime tenant onboarding.

---

## 4. ETL with Materialized Views

**Problem:** Pre-compute a daily summary joining logs, metrics, and business data for dashboard consumption.

```toml
[[view]]
name = "daily_service_health"
query = """
    SELECT DATE_TRUNC('day', l.timestamp) as day,
           l.service,
           count(*) as total_requests,
           count(CASE WHEN l.status >= 500 THEN 1 END) as errors,
           avg(p.cpu_usage) as avg_cpu,
           max(p.memory_usage) as peak_memory
    FROM opensearch.access_logs l
    LEFT JOIN prometheus.node_metrics p
        ON l.service = p.service
        AND DATE_TRUNC('hour', l.timestamp) = DATE_TRUNC('hour', p.timestamp)
    WHERE l.timestamp > NOW() - INTERVAL '30 days'
    GROUP BY DATE_TRUNC('day', l.timestamp), l.service
"""
refresh_secs = 3600
max_age_secs = 7200
```

```sql
-- Dashboard query: sub-millisecond from cache
SELECT * FROM view.daily_service_health
WHERE day > NOW() - INTERVAL '7 days'
ORDER BY errors DESC
```

**How it works:** The view refreshes hourly, joining OpenSearch logs with Prometheus metrics. Dashboard queries hit the cache instead of re-executing the expensive federated query.

**Tip:** Set `max_age_secs` to 2x `refresh_secs` so stale data is served during refresh failures rather than returning errors.

---

## 5. Real-Time Grafana Dashboard

**Problem:** Build a Grafana dashboard showing cross-datasource metrics with auto-refresh.

```sql
-- Panel 1: Error rate time series (Grafana $__timeFilter)
SELECT DATE_TRUNC('minute', timestamp) as time,
       service,
       count(*) as errors
FROM opensearch.application_logs
WHERE status >= 500
  AND $__timeFilter(timestamp)
GROUP BY DATE_TRUNC('minute', timestamp), service
ORDER BY time

-- Panel 2: Top error services with user context
SELECT l.service, u.team, count(*) as errors
FROM opensearch.application_logs l
JOIN dynamodb.users u ON l.user_id = u.user_id
WHERE l.status >= 500
  AND $__timeFilter(l.timestamp)
GROUP BY l.service, u.team
ORDER BY errors DESC
LIMIT 10

-- Panel 3: Infrastructure correlation
SELECT p.service, p.cpu_usage, p.memory_usage, l.error_count
FROM prometheus.node_metrics p
JOIN (
    SELECT service, count(*) as error_count
    FROM opensearch.application_logs
    WHERE status >= 500 AND $__timeFilter(timestamp)
    GROUP BY service
) l ON p.service = l.service
WHERE $__timeFilter(p.timestamp)
```

**How it works:** Grafana's Fuse datasource plugin sends queries with template variable substitution. Auto-refresh polls at the configured interval.

**Tip:** Use Grafana variables for `$service` and `$environment` to make dashboards reusable across teams.

---

## 6. Security Audit Trail Analysis

**Problem:** Investigate unauthorized access attempts across the audit log.

```sql
-- Failed access attempts by identity and datasource
SELECT identity, action, count(*) as denied_count,
       MIN(timestamp) as first_attempt,
       MAX(timestamp) as last_attempt
FROM opensearch.fuse_audit_logs
WHERE status = 'Denied'
  AND timestamp > NOW() - INTERVAL '24 hours'
GROUP BY identity, action
ORDER BY denied_count DESC

-- Cross-reference with user directory
SELECT a.identity, a.denied_count, a.action,
       u.name, u.team, u.role
FROM (
    SELECT identity, action, count(*) as denied_count
    FROM opensearch.fuse_audit_logs
    WHERE status = 'Denied'
      AND timestamp > NOW() - INTERVAL '24 hours'
    GROUP BY identity, action
) a
JOIN postgres.user_directory u ON a.identity = u.api_key_identity
ORDER BY a.denied_count DESC
```

**How it works:** Queries the structured audit log (shipped to OpenSearch) and joins with the user directory in PostgreSQL to identify who is behind the failed attempts.

**Tip:** Ship audit logs to a separate OpenSearch index with a retention policy. Use `grep '"status":"Denied"'` for real-time monitoring.

---

## 7. Cost Optimization via Pushdown Analysis

**Problem:** Identify queries that transfer too much data because filters aren't being pushed down.

```sql
-- Find expensive queries from history
SELECT query, duration_ms, row_count,
       CASE WHEN row_count > 10000 THEN 'add LIMIT'
            WHEN query LIKE 'SELECT *%' THEN 'use explicit columns'
            ELSE 'check pushdown'
       END as optimization
FROM opensearch.fuse_query_history
WHERE duration_ms > 1000
  AND timestamp > NOW() - INTERVAL '24 hours'
ORDER BY duration_ms DESC
LIMIT 20
```

Then use EXPLAIN ANALYZE on the slow queries:

```bash
fuse explain --analyze "SELECT * FROM cluster_a.logs WHERE status >= 500"
```

Look for:
- `data_bytes > 1MB` → add filters or reduce columns
- `pushdown: []` (empty) → filter can't be translated, rewrite the WHERE clause
- `estimate_accuracy: 50.0x` → planner overestimated, consider adding LIMIT

**Tip:** Run `GET /api/fuse/advisor?query=...` for automated optimization suggestions.

---

## 8. Migration from Trino/Presto

**Problem:** Replace Trino federated queries with Fuse for lower latency and simpler operations.

**Trino:**
```sql
-- Trino: requires catalog.schema.table naming
SELECT o.order_id, c.name, o.total
FROM opensearch.default.orders o
JOIN postgresql.public.customers c ON o.customer_id = c.id
WHERE o.status = 'failed'
```

**Fuse:**
```sql
-- Fuse: datasource.table naming (no schema level)
SELECT o.order_id, c.name, o.total
FROM opensearch.orders o
JOIN postgres.customers c ON o.customer_id = c.id
WHERE o.status = 'failed'
```

**Key differences:**

| | Trino | Fuse |
|---|-------|------|
| Naming | `catalog.schema.table` | `datasource.table` |
| Setup | JVM cluster, coordinator + workers | Single binary |
| Config | `catalog/*.properties` | `fuse.toml` |
| PPL | Not supported | Supported |
| Dashboards | External (Superset, Metabase) | Built-in |

**Tip:** Start by running both in parallel — point Fuse at the same datasources and compare results. Fuse's SQL is DataFusion-based, so most ANSI SQL works unchanged.

---

## 9. Jupyter Data Science Workflow

**Problem:** Build a data science pipeline that queries across datasources, analyzes in pandas, and visualizes.

```python
from fuse_client import FuseClient
import pandas as pd
import matplotlib.pyplot as plt

fuse = FuseClient("http://localhost:9400", api_key="your-key")

# 1. Federated query: logs + user profiles
result = fuse.query("""
    SELECT u.team, l.service,
           count(*) as errors,
           avg(l.response_time_ms) as avg_latency
    FROM opensearch.application_logs l
    JOIN dynamodb.users u ON l.user_id = u.user_id
    WHERE l.status >= 500
      AND l.timestamp > NOW() - INTERVAL '7 days'
    GROUP BY u.team, l.service
""")
df = pd.DataFrame(result.rows, columns=result.columns)

# 2. Analyze
pivot = df.pivot_table(values='errors', index='team', columns='service', fill_value=0)

# 3. Visualize
pivot.plot(kind='bar', stacked=True, figsize=(12, 6))
plt.title('Errors by Team and Service (Last 7 Days)')
plt.ylabel('Error Count')
plt.tight_layout()
plt.savefig('errors_by_team.png')

# 4. Drill down into anomalies
top_team = df.groupby('team')['errors'].sum().idxmax()
traces = fuse.query(f"""
    SELECT trace_id, service, status, response_time_ms
    FROM opensearch.application_logs
    WHERE user_id IN (SELECT user_id FROM dynamodb.users WHERE team = '{top_team}')
      AND status >= 500
    ORDER BY response_time_ms DESC
    LIMIT 10
""")
```

**Tip:** Use `fuse.query_all()` for large datasets that need cursor pagination. Results stream page-by-page into a single DataFrame.

---

## 10. CI/CD Query Validation

**Problem:** Validate all saved queries and dashboard queries before deployment.

```bash
#!/bin/bash
# validate-queries.sh — run in CI pipeline

FUSE_URL="${FUSE_URL:-http://localhost:9400}"
FUSE_KEY="${FUSE_KEY:-ci-api-key}"
ERRORS=0

# Validate all .sql files in queries/
for f in queries/*.sql; do
    QUERY=$(cat "$f")
    RESULT=$(curl -sf -X POST "$FUSE_URL/api/fuse/query/validate" \
        -H "x-api-key: $FUSE_KEY" \
        -H "Content-Type: application/json" \
        -d "{\"query\": $(echo "$QUERY" | jq -Rs .)}")

    VALID=$(echo "$RESULT" | jq -r '.valid')
    if [ "$VALID" != "true" ]; then
        echo "❌ $f: $(echo "$RESULT" | jq -r '.error')"
        ERRORS=$((ERRORS + 1))
    else
        echo "✅ $f"
    fi
done

# Validate materialized view definitions
for view in $(grep -A1 '^\[\[view\]\]' fuse.toml | grep 'query' | sed 's/.*= *"//;s/"$//'); do
    RESULT=$(curl -sf -X POST "$FUSE_URL/api/fuse/query/validate" \
        -H "x-api-key: $FUSE_KEY" \
        -d "{\"query\": $(echo "$view" | jq -Rs .)}")
    VALID=$(echo "$RESULT" | jq -r '.valid')
    if [ "$VALID" != "true" ]; then
        echo "❌ view: $(echo "$RESULT" | jq -r '.error')"
        ERRORS=$((ERRORS + 1))
    fi
done

exit $ERRORS
```

**How it works:** The validate endpoint parses and plans the query without executing it. Catches syntax errors, unknown datasources, and schema mismatches.

**Tip:** Run against a staging Fuse instance with the same datasource config as production.

---

## 11. Log Search Across Regions

**Problem:** Search for a trace ID across all regional log clusters.

```sql
SELECT _datasource as region, timestamp, service, message
FROM us_east.application_logs
WHERE trace_id = 'abc-123-def'
UNION ALL
SELECT _datasource, timestamp, service, message
FROM eu_west.application_logs
WHERE trace_id = 'abc-123-def'
UNION ALL
SELECT _datasource, timestamp, service, message
FROM ap_northeast.application_logs
WHERE trace_id = 'abc-123-def'
ORDER BY timestamp
```

**How it works:** Fuse fans out to all three regional clusters in parallel, pushes the `trace_id` filter to each, and merges results sorted by timestamp. `_datasource` shows which region each log came from.

**Tip:** In a hub-spoke topology, each region is a spoke. The hub handles the UNION ALL and sort.

---

## 12. Redis Cache Hit Rate Analysis

**Problem:** Correlate Redis cache performance with application error rates.

```sql
SELECT DATE_TRUNC('hour', r.timestamp) as hour,
       r.hit_rate,
       count(l.status) as total_requests,
       count(CASE WHEN l.status >= 500 THEN 1 END) as errors,
       count(CASE WHEN l.status >= 500 THEN 1 END)::float / count(l.status) as error_rate
FROM redis.cache_stats r
JOIN opensearch.access_logs l
    ON DATE_TRUNC('hour', r.timestamp) = DATE_TRUNC('hour', l.timestamp)
GROUP BY DATE_TRUNC('hour', r.timestamp), r.hit_rate
ORDER BY hour
```

**Tip:** Look for inverse correlation — when `hit_rate` drops, `error_rate` often spikes due to backend overload.

---

## 13. DynamoDB + S3 Data Lake Join

**Problem:** Enrich real-time DynamoDB records with historical S3 Parquet data.

```sql
SELECT d.order_id, d.status, d.updated_at,
       h.original_amount, h.discount_applied
FROM dynamodb.orders d
JOIN s3.order_history h ON d.order_id = h.order_id
WHERE d.status = 'disputed'
  AND h.order_date > '2026-01-01'
```

**How it works:** Fuse pushes the `status` filter to DynamoDB (FilterExpression) and the `order_date` predicate to S3 Parquet (predicate pushdown on row groups). The hash join runs locally.

**Tip:** Partition S3 Parquet files by date for efficient predicate pushdown.

---

## 14. Prometheus + CloudWatch Metric Correlation

**Problem:** Compare application metrics from Prometheus with AWS infrastructure metrics from CloudWatch.

```sql
SELECT p.service,
       avg(p.request_rate) as app_rps,
       avg(c.cpu_utilization) as aws_cpu,
       avg(c.network_in) as aws_net_in
FROM prometheus.http_requests p
JOIN cloudwatch.ec2_metrics c ON p.instance_id = c.instance_id
WHERE p.timestamp > NOW() - INTERVAL '6 hours'
  AND c.timestamp > NOW() - INTERVAL '6 hours'
GROUP BY p.service
ORDER BY aws_cpu DESC
```

**Tip:** CloudWatch pushes time range and dimension filters. Align time granularity between sources for accurate joins.

---

## 15. MongoDB Document Analytics

**Problem:** Run SQL analytics over MongoDB document collections joined with relational data.

```sql
SELECT m.category, m.author,
       count(*) as articles,
       avg(p.page_views) as avg_views
FROM mongodb.articles m
JOIN postgres.analytics p ON m._id = p.article_id
WHERE m.published = true
  AND p.date > '2026-03-01'
GROUP BY m.category, m.author
ORDER BY avg_views DESC
LIMIT 20
```

**How it works:** Fuse translates the `published = true` filter to a MongoDB BSON query, fetches matching documents, flattens nested fields, and joins with PostgreSQL analytics.

**Tip:** MongoDB pushdown supports `=`, `!=`, `>`, `<`, `IN`, and nested field access (`address.city`).

---

## 16. ClickHouse + OpenSearch Real-Time Analytics

**Problem:** Combine ClickHouse analytical aggregations with OpenSearch full-text search.

```sql
SELECT c.product_id, c.total_revenue, c.order_count,
       l.recent_errors
FROM clickhouse.product_sales c
JOIN (
    SELECT product_id, count(*) as recent_errors
    FROM opensearch.error_logs
    WHERE message LIKE '%payment%'
      AND timestamp > NOW() - INTERVAL '24 hours'
    GROUP BY product_id
) l ON c.product_id = l.product_id
WHERE c.order_date > '2026-04-01'
ORDER BY c.total_revenue DESC
LIMIT 50
```

**Tip:** ClickHouse handles the heavy aggregation (revenue, counts) while OpenSearch handles the full-text search. Both push down their respective filters.

---

## 17. InfluxDB Time-Series with Business Context

**Problem:** Enrich IoT sensor data from InfluxDB with device metadata from PostgreSQL.

```sql
SELECT i.device_id, p.location, p.owner,
       avg(i.temperature) as avg_temp,
       max(i.temperature) as max_temp
FROM influxdb.sensor_readings i
JOIN postgres.device_registry p ON i.device_id = p.device_id
WHERE i.time > NOW() - INTERVAL '1 hour'
  AND p.active = true
GROUP BY i.device_id, p.location, p.owner
HAVING max(i.temperature) > 80
ORDER BY max_temp DESC
```

**Tip:** InfluxDB pushes time range and tag filters. The `HAVING` clause runs locally after the join.

---

## 18. CSV Import + Live Data Comparison

**Problem:** Compare a CSV budget file with live spending data from PostgreSQL.

```sql
SELECT b.department, b.budget_amount,
       COALESCE(s.actual_spend, 0) as actual_spend,
       b.budget_amount - COALESCE(s.actual_spend, 0) as remaining
FROM csv.budget_2026 b
LEFT JOIN postgres.spending s ON b.department = s.department
WHERE b.quarter = 'Q2'
ORDER BY remaining ASC
```

**How it works:** Fuse reads the CSV file as a table, joins with live PostgreSQL data. LEFT JOIN ensures departments with no spending still appear.

**Tip:** Place CSV files in the configured data directory. Fuse auto-detects schema from headers.

---

## 19. DuckDB Local Analytics + Remote Data

**Problem:** Run complex analytics locally in DuckDB while enriching with remote datasource data.

```sql
SELECT d.category, d.subcategory,
       count(DISTINCT l.user_id) as unique_users,
       sum(l.purchase_amount) as revenue
FROM duckdb.product_catalog d
JOIN opensearch.purchase_logs l ON d.product_id = l.product_id
WHERE l.timestamp > NOW() - INTERVAL '30 days'
GROUP BY d.category, d.subcategory
ORDER BY revenue DESC
```

**How it works:** DuckDB runs in-process with Arrow-native data exchange (zero-copy). The product catalog lives locally for fast lookups while purchase logs come from OpenSearch.

**Tip:** Use DuckDB for reference data, lookup tables, and local analytics that don't need a remote database.

---

## 20. Automated Alerting Pipeline

**Problem:** Define alert conditions that span multiple datasources and trigger notifications.

```sql
-- Materialized view: refreshes every 5 minutes
-- Alert rule queries this view
SELECT service, error_rate, avg_latency, active_incidents
FROM (
    SELECT l.service,
           count(CASE WHEN l.status >= 500 THEN 1 END)::float / count(*) as error_rate,
           avg(l.response_time_ms) as avg_latency,
           COALESCE(i.active_count, 0) as active_incidents
    FROM opensearch.access_logs l
    LEFT JOIN postgres.incidents i ON l.service = i.service AND i.status = 'open'
    WHERE l.timestamp > NOW() - INTERVAL '10 minutes'
    GROUP BY l.service, i.active_count
)
WHERE error_rate > 0.05           -- >5% error rate
   OR avg_latency > 2000          -- >2s average latency
   OR (error_rate > 0.01 AND active_incidents = 0)  -- errors with no incident
ORDER BY error_rate DESC
```

Configure as a Fuse alert rule or query from Grafana with alerting enabled.

**Tip:** The third condition (`error_rate > 1% AND no open incident`) catches situations where errors are occurring but nobody has been paged yet.
