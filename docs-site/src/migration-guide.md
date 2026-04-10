# Migration Guide: OpenSearch to Fuse

How to migrate from direct OpenSearch queries to Fuse federated queries. Fuse is a superset — your existing queries work with minimal changes, and you gain cross-datasource capabilities.

## What Changes

| Aspect | Direct OpenSearch | Fuse |
|--------|------------------|------|
| Endpoint | `POST /_search` | `POST /api/fuse/query` |
| Query language | Query DSL (JSON) | SQL or PPL |
| Table reference | Index name | `datasource.index` |
| Auth | Direct to cluster | Fuse handles per-connector auth |
| Results | OpenSearch JSON | Columnar JSON (columns + rows) |

## What Doesn't Change

- Your OpenSearch clusters stay where they are
- Index mappings, data, and aliases are untouched
- Fuse reads from OpenSearch — it never writes
- Existing Query DSL dashboards continue to work alongside Fuse

## Step 1: Register Your Clusters

Add each OpenSearch cluster to `fuse.toml`:

```toml
# Existing cluster (direct access today)
[[connector]]
id = "prod"
type = "opensearch"
url = "https://your-cluster.us-west-2.aoss.amazonaws.com"
[connector.auth]
type = "sigv4"
region = "us-west-2"
service = "aoss"

# Second cluster you want to federate with
[[connector]]
id = "staging"
type = "opensearch"
url = "https://staging-cluster.us-west-2.es.amazonaws.com"
[connector.auth]
type = "basic"
username = "admin"
password = "admin"
```

Verify with:
```bash
curl http://localhost:9400/api/fuse/health
curl http://localhost:9400/api/fuse/datasources
```

## Step 2: Translate Queries

### Query DSL → SQL

| Query DSL | Fuse SQL |
|-----------|----------|
| `{"query": {"match_all": {}}}` | `SELECT * FROM prod.logs LIMIT 100` |
| `{"query": {"term": {"service": "api"}}}` | `SELECT * FROM prod.logs WHERE service = 'api'` |
| `{"query": {"range": {"status": {"gte": 500}}}}` | `SELECT * FROM prod.logs WHERE status >= 500` |
| `{"query": {"bool": {"must": [...]}}}` | `SELECT * FROM prod.logs WHERE cond1 AND cond2` |
| `{"_source": ["service", "status"]}` | `SELECT service, status FROM prod.logs` |
| `{"size": 10}` | `SELECT * FROM prod.logs LIMIT 10` |
| `{"sort": [{"timestamp": "desc"}]}` | `SELECT * FROM prod.logs ORDER BY timestamp DESC` |
| `{"aggs": {"by_svc": {"terms": {"field": "service"}}}}` | `SELECT service, count(*) FROM prod.logs GROUP BY service` |

### Query DSL → PPL

| Query DSL | Fuse PPL |
|-----------|----------|
| Match all, limit 10 | `source = prod.logs \| head 10` |
| Filter by field | `source = prod.logs \| where service = 'api'` |
| Aggregation | `source = prod.logs \| stats count() by service` |
| Sort descending | `source = prod.logs \| sort - timestamp \| head 10` |
| Select fields | `source = prod.logs \| fields service, status` |

### Common Patterns

**Error search:**
```
-- Before (Query DSL)
POST /application_logs/_search
{"query": {"range": {"status": {"gte": 500}}}, "size": 20, "sort": [{"timestamp": "desc"}]}

-- After (SQL)
SELECT * FROM prod.application_logs WHERE status >= 500 ORDER BY timestamp DESC LIMIT 20

-- After (PPL)
source = prod.application_logs | where status >= 500 | sort - timestamp | head 20
```

**Aggregation:**
```
-- Before (Query DSL)
POST /application_logs/_search
{"size": 0, "aggs": {"errors_by_service": {"terms": {"field": "service"}, "aggs": {"error_count": {"filter": {"range": {"status": {"gte": 500}}}}}}}}

-- After (SQL)
SELECT service, count(*) as errors FROM prod.application_logs WHERE status >= 500 GROUP BY service ORDER BY errors DESC
```

**Multi-index search:**
```
-- Before (Query DSL — comma-separated indices)
POST /index_a,index_b/_search
{"query": {"match_all": {}}}

-- After (SQL — UNION ALL)
SELECT * FROM prod.index_a UNION ALL SELECT * FROM prod.index_b LIMIT 100
```

## Step 3: Gain Cross-Datasource Power

Once your queries work through Fuse, you unlock capabilities that weren't possible with direct OpenSearch:

### Join with other datasources

```sql
-- Enrich OpenSearch logs with DynamoDB user profiles
SELECT l.service, l.status, u.name, u.team
FROM prod.application_logs l
JOIN dynamodb.users u ON l.user_id = u.user_id
WHERE l.status >= 500
```

### Unified view across clusters

```sql
-- Query production + staging in one statement
SELECT _datasource, service, count(*) as errors
FROM prod.application_logs WHERE status >= 500
UNION ALL
SELECT _datasource, service, count(*) as errors
FROM staging.application_logs WHERE status >= 500
GROUP BY _datasource, service
```

### Correlate with metrics

```sql
-- Join OpenSearch logs with Prometheus metrics
SELECT l.service, l.status, p.value as cpu_usage
FROM prod.application_logs l
JOIN prometheus.cpu_metrics p ON l.service = p.service
WHERE l.status >= 500
```

### Cross-source trace reconstruction

```bash
# Find a trace across all registered datasources
curl http://localhost:9400/api/fuse/trace/trace-abc123
```

## Step 4: Migrate Dashboards

### From OpenSearch Dashboards Visualizations

1. Identify the Query DSL behind each visualization
2. Translate to SQL using the table above
3. Create a Fuse dashboard panel at `/dashboard` with the SQL query
4. Select the matching chart type (Fuse auto-detects in most cases)

### From Saved Searches

Saved searches translate directly:

```
-- OpenSearch saved search: index=application_logs, query="status >= 500"
-- Fuse equivalent:
SELECT * FROM prod.application_logs WHERE status >= 500 LIMIT 100
```

Save as a Fuse saved query:
```bash
curl -X POST http://localhost:9400/api/fuse/saved \
  -H 'Content-Type: application/json' \
  -d '{"name": "error_search", "query": "SELECT * FROM prod.application_logs WHERE status >= 500 LIMIT 100"}'
```

## Pushdown Guarantee

Fuse pushes filters, projections, aggregations, sort, and limit to OpenSearch. A single-source Fuse query has the same performance as a direct Query DSL call — the SubQuery is translated back to Query DSL and sent to your cluster.

Verify with EXPLAIN:
```bash
curl -X POST http://localhost:9400/api/fuse/query/explain \
  -d '{"query": "SELECT service, count(*) FROM prod.logs WHERE status >= 500 GROUP BY service"}'
```

The plan shows `RemoteScan` with pushdown badges: `filter`, `projection`, `aggregation`, `limit`.

## Rollback

Fuse is read-only and additive. Your OpenSearch clusters are unchanged. To rollback:
1. Point clients back to OpenSearch directly
2. Stop the Fuse server
3. No data migration needed — Fuse stores nothing in your clusters
