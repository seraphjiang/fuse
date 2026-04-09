# API Reference

Base URL: `https://fuse.huanji.profile.aws.dev` (playground) or `http://localhost:9400` (local).

## Query Endpoints

### POST /api/fuse/query

Execute a SQL or PPL query.

**Request:**
```json
{
  "query": "SELECT * FROM cluster_a.application_logs LIMIT 5",
  "format": "sql",
  "analyze": false
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | string | required | SQL or PPL query |
| `format` | string | `"sql"` | `"sql"` or `"ppl"` |
| `analyze` | bool | `false` | Include execution profile with timing |

**Response:**
```json
{
  "columns": ["service", "status", "message"],
  "rows": [["api-gateway", 500, "timeout"], ...],
  "metadata": {
    "total_rows": 5,
    "format": "sql",
    "datasources_queried": ["cluster_a", "cluster_b"],
    "datasource_stats": {
      "cluster_a": { "rows": 3, "latency_ms": 45 },
      "cluster_b": { "rows": 2, "latency_ms": 67 }
    },
    "execution_profile": {
      "total_ms": 112,
      "nodes": [
        {
          "op": "Merge",
          "actual_rows": 5,
          "actual_ms": 112,
          "children": [
            {
              "op": "RemoteScan",
              "datasource": "cluster_a",
              "actual_rows": 3,
              "actual_ms": 45,
              "pushdown": ["filter", "limit"]
            }
          ]
        }
      ]
    }
  }
}
```

`datasource_stats` and `datasources_queried` appear for cross-datasource queries. `execution_profile` appears when `analyze: true`.

**curl:**
```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT service, count(*) as n FROM cluster_a.application_logs GROUP BY service"}'
```

### POST /api/fuse/query/explain

Show execution plan without running the query.

**Response:**
```json
{
  "plan": "Merge\n  RemoteScan [cluster_a] (est. 1000 rows)\n  RemoteScan [cluster_b] (est. 1000 rows)",
  "plan_tree": {
    "op": "Merge",
    "children": [
      { "op": "RemoteScan", "detail": "cluster_a.application_logs", "estimated_rows": 1000 }
    ]
  }
}
```

**curl:**
```bash
curl -X POST http://localhost:9400/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs UNION ALL SELECT * FROM cluster_b.application_logs"}'
```

### POST /api/fuse/query/validate

Validate query syntax without executing.

**Response:**
```json
{ "valid": true, "error": null }
```

**curl:**
```bash
curl -X POST http://localhost:9400/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs"}'
```

### POST /api/fuse/query/stream

Server-sent events (SSE) streaming query execution.

**Events:**
```
data: {"type":"batch","rows":[["v1","v2"]]}
data: {"type":"progress","batches_sent":5}
data: {"type":"done","total_rows":42}
data: {"type":"error","message":"..."}
```

## Discovery Endpoints

### GET /api/fuse/health

Engine and per-connector health status.

```bash
curl http://localhost:9400/api/fuse/health
```

**Response:**
```json
{
  "status": "ok",
  "connectors": {
    "cluster_a": { "status": "healthy", "latency_ms": 23 },
    "cluster_b": { "status": "healthy", "latency_ms": 31 }
  }
}
```

### GET /api/fuse/datasources

List all registered datasources.

```bash
curl http://localhost:9400/api/fuse/datasources
```

### GET /api/fuse/datasources/{id}/schemas

List tables/indices for a datasource.

```bash
curl http://localhost:9400/api/fuse/datasources/cluster_a/schemas
```

### GET /api/fuse/datasources/{id}/schemas/{table}/fields

Get field names and types for a table.

```bash
curl http://localhost:9400/api/fuse/datasources/cluster_a/schemas/application_logs/fields
```

## System Endpoints

### GET /api/fuse/history

Recent query executions with timing.

```bash
curl http://localhost:9400/api/fuse/history
```

**Response:**
```json
[
  {
    "query": "SELECT * FROM cluster_a.application_logs LIMIT 5",
    "format": "sql",
    "timestamp": 1712678400,
    "latency_ms": 45,
    "row_count": 5,
    "error": null
  }
]
```

## Alert Endpoints

### GET /api/fuse/alerts

List configured alert rules.

```bash
curl http://localhost:9400/api/fuse/alerts
```

### POST /api/fuse/alerts/evaluate

Evaluate all alert rules and return current state.

```bash
curl -X POST http://localhost:9400/api/fuse/alerts/evaluate
```

## View Endpoints

### GET /api/fuse/views

List materialized views.

```bash
curl http://localhost:9400/api/fuse/views
```

### GET /api/fuse/views/{name}

Get a specific materialized view (returns cached data).

```bash
curl http://localhost:9400/api/fuse/views/error_summary
```

### POST /api/fuse/views/{name}/refresh

Refresh a materialized view.

```bash
curl -X POST http://localhost:9400/api/fuse/views/error_summary/refresh
```

## Error Responses

All errors return:

```json
{ "error": "description of what went wrong" }
```

| Status | Meaning |
|--------|---------|
| 400 | Parse error, missing FROM, bad syntax |
| 404 | Datasource or view not found |
| 429 | Rate limited |
| 500 | Connector error, timeout, internal failure |
