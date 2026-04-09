# Getting Started with Fuse

Fuse is a federated query engine that lets you run a single SQL or PPL query across multiple OpenSearch clusters and datasources, merging results automatically. It runs as a standalone service and is accessible from OpenSearch Dashboards or directly via REST API.

**Playground:** https://fuse.huanji.profile.aws.dev *(Amazon VPN required)*

---

## Available Datasources

| ID | Type | Cluster | Index |
|----|------|---------|-------|
| `cluster_a` | OpenSearch Serverless | fuse-cluster-a (us-west-2) | `services`, `application_logs` |
| `cluster_b` | OpenSearch Serverless | fuse-cluster-b (us-west-2) | `services`, `application_logs` |

The `services` index contains simulated microservice request logs with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `trace_id` | keyword | Distributed trace identifier |
| `service` | keyword | Service name (e.g., `api-gateway`, `auth`, `payments`) |
| `status` | integer | HTTP status code |
| `latency_ms` | float | Request latency in milliseconds |
| `message` | text | Log message |
| `@timestamp` | date | Event timestamp |

---

## Example Queries

### Basic search (SQL)

```sql
SELECT trace_id, service, status, latency_ms
FROM cluster_a.services
LIMIT 10
```

### Basic search (PPL)

```ppl
source = cluster_a.services
| fields trace_id, service, status, latency_ms
| head 10
```

### Filter by status code

```sql
-- SQL: find all 5xx errors
SELECT trace_id, service, status, message
FROM cluster_a.services
WHERE status >= 500
LIMIT 50
```

```ppl
-- PPL equivalent
source = cluster_a.services
| where status >= 500
| fields trace_id, service, status, message
| head 50
```

### Aggregate by service

```sql
-- Count errors per service
SELECT service, COUNT(*) AS error_count
FROM cluster_a.services
WHERE status >= 500
GROUP BY service
```

```ppl
source = cluster_a.services
| where status >= 500
| stats count() as error_count by service
| sort - error_count
```

### Cross-cluster federated query

This is the core Fuse feature — one query, two clusters, merged results:

```sql
-- UNION ALL: fan out to both clusters, merge results
SELECT trace_id, service, status
FROM cluster_a.services
UNION ALL
SELECT trace_id, service, status
FROM cluster_b.services
WHERE status >= 500
LIMIT 100
```

```ppl
-- PPL multi-source: comma-separated sources fan out automatically
source = cluster_a.services, cluster_b.services
| where status >= 500
| head 100
```

### Cross-cluster trace correlation (JOIN)

Find a trace that spans both clusters using a hash join on `trace_id`:

```sql
SELECT a.trace_id, a.service AS service_a, b.service AS service_b,
       a.status AS status_a, b.status AS status_b
FROM cluster_a.services a
JOIN cluster_b.services b ON a.trace_id = b.trace_id
WHERE a.status >= 500
```

---

## API Usage

All endpoints are at `https://fuse.huanji.profile.aws.dev/api/fuse/`.

The playground UI at `/` includes a **Feeling Lucky** button that runs a random example query — useful for exploring the demo data without writing SQL.

### Health check

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/health
```

### List datasources

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources
```

### List schemas for a datasource

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources/cluster_a/schemas
```

### Get fields for a table

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources/cluster_a/schemas/services/fields
```

### Execute a query (SQL)

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT service, status FROM cluster_a.services WHERE status >= 500 LIMIT 10",
    "format": "sql"
  }'
```

### Execute a query (PPL)

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "source = cluster_a.services | where status >= 500 | stats count() by service",
    "format": "ppl"
  }'
```

### Stream query results (SSE)

```bash
curl -N -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.services LIMIT 100", "format": "sql"}'
```

Events arrive as `data: {...}` lines with `type`: `metadata`, `batch`, `progress`, `done`, or `error`.

### Validate a query

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.services", "format": "sql"}'
```

### Explain a query plan

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.services WHERE status = 500", "format": "sql"}'
```

### EXPLAIN ANALYZE (execution profiling)

Add `"analyze": true` to any query request to get per-node execution stats alongside results:

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT service, COUNT(*) FROM cluster_a.services GROUP BY service",
    "format": "sql",
    "analyze": true
  }'
```

The response includes an `execution_profile` field with timing and row counts per plan node. The playground UI shows this as a visual plan tree when the **Analyze** checkbox is checked.

### Query history

Returns the last 50 queries with latency and row count:

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/history
```

```json
[
  {
    "query": "SELECT * FROM cluster_a.services LIMIT 10",
    "format": "sql",
    "timestamp": 1712678400,
    "latency_ms": 45,
    "row_count": 10,
    "error": null
  }
]
```

### Alert rules

```bash
# List configured alert rules
curl https://fuse.huanji.profile.aws.dev/api/fuse/alerts

# Evaluate all rules now
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/alerts/evaluate
```

### Materialized views

```bash
# List views
curl https://fuse.huanji.profile.aws.dev/api/fuse/views

# Query a view (returns cached results)
curl https://fuse.huanji.profile.aws.dev/api/fuse/views/error_summary

# Refresh a view
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/views/error_summary/refresh
```

---

## Understanding Results

Query responses have this structure:

```json
{
  "columns": ["trace_id", "service", "status", "_datasource"],
  "rows": [
    ["abc-001", "api-gateway", "500", "cluster_a"],
    ["abc-002", "auth", "200", "cluster_b"]
  ],
  "metadata": {
    "total_rows": 2,
    "format": "sql",
    "datasources_queried": ["cluster_a", "cluster_b"]
  }
}
```

| Field | Description |
|-------|-------------|
| `columns` | Ordered list of column names |
| `rows` | Array of arrays — each inner array is one row, values in column order |
| `metadata.total_rows` | Number of rows returned |
| `metadata.format` | Query format used (`sql` or `ppl`) |
| `metadata.datasources_queried` | Which connectors were queried (multi-source only) |

**`_datasource` column:** For federated queries across multiple datasources, Fuse automatically adds a `_datasource` column showing which connector each row came from. This is useful for debugging and for understanding data provenance in UNION ALL results.

All values are returned as strings. Parse numeric fields client-side as needed.

---

## PPL vs SQL

Both languages are supported. Choose based on your use case:

| | SQL | PPL |
|--|-----|-----|
| Best for | Joins, aggregations, subqueries | Log analysis, pipe-style filtering |
| FROM syntax | `FROM datasource.table` | `source = datasource.table` |
| Filter | `WHERE status = 500` | `\| where status = 500` |
| Aggregate | `GROUP BY service` | `\| stats count() by service` |
| Sort | `ORDER BY latency_ms DESC` | `\| sort - latency_ms` |
| Limit | `LIMIT 100` | `\| head 100` |
| Select fields | `SELECT a, b` | `\| fields a, b` |

**Quick syntax comparison:**

```sql
-- SQL
SELECT service, COUNT(*) AS cnt
FROM cluster_a.services
WHERE status >= 500
GROUP BY service
ORDER BY cnt DESC
LIMIT 10
```

```ppl
-- PPL equivalent
source = cluster_a.services
| where status >= 500
| stats count() as cnt by service
| sort - cnt
| head 10
```

---

## Troubleshooting

### `"datasource 'X' not found"`

The datasource ID in your query doesn't match a registered connector.

```bash
# Check available datasource IDs
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources | python3 -m json.tool
```

Use the exact `id` value from the response (e.g., `cluster_a`, not `Cluster_A`).

### `"SQL query must contain FROM clause"`

Your SQL query is missing a `FROM datasource.table` clause. Fuse requires qualified table references:

```sql
-- ❌ Wrong
SELECT * FROM services

-- ✅ Correct
SELECT * FROM cluster_a.services
```

### `"PPL query must start with 'source = '"`

PPL queries must begin with `source = datasource.table`:

```ppl
-- ❌ Wrong
search source=services | where status=500

-- ✅ Correct
source = cluster_a.services | where status = 500
```

### `"expected 'datasource.table', got 'X'"`

The table reference must be qualified with a datasource prefix:

```sql
-- ❌ Wrong: unqualified
SELECT * FROM services

-- ✅ Correct: qualified
SELECT * FROM cluster_a.services
```

### Health shows `"degraded"` connectors

The engine is running but the OpenSearch Serverless clusters may be warming up or the AOSS compatibility fix hasn't deployed yet. Wait a few minutes and retry:

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/health
```

If status is `"healthy"`, queries will work. If `"degraded"`, basic queries may still succeed — schema discovery may be limited.

### Empty results

If a query returns `"rows": []`, the filter may be too restrictive or the index may not have matching data. Try removing the `WHERE` clause first:

```sql
SELECT * FROM cluster_a.services LIMIT 5
```

---

## Next Steps

- **Build a connector:** Copy `crates/fuse-connectors/example/` and follow [Writing a Connector](./writing-a-connector.md)
- **OSD Plugin:** Install the `fuseQuery` plugin to query from within OpenSearch Dashboards
- **API Reference:** Full reference at [docs/api/api-reference.md](../api/api-reference.md) and OpenAPI spec at [docs/api/openapi.yaml](../api/openapi.yaml)
- **Connector guide:** Auth config, S3 O11y, SigV4 — see [docs/connectors/connector-guide.md](../connectors/connector-guide.md)
- **GitHub:** https://github.com/seraphjiang/fuse
