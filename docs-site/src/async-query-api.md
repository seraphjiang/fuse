# Async Query API

Submit long-running queries and poll for results. Useful for large cross-source JOINs or batch analytics.

## Submit a Query

```bash
curl -X POST http://localhost:9400/api/fuse/query/async \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.logs JOIN dynamodb.users u ON ...", "format": "sql"}'
```

Response:
```json
{"job_id": "job-abc123", "status": "pending"}
```

## Poll for Results

```bash
curl http://localhost:9400/api/fuse/query/async/job-abc123
```

Response (in progress):
```json
{"job_id": "job-abc123", "status": "running"}
```

Response (complete):
```json
{"job_id": "job-abc123", "status": "completed", "columns": [...], "rows": [...], "metadata": {...}}
```

## Cancel a Job

```bash
curl -X DELETE http://localhost:9400/api/fuse/query/async/job-abc123
```

## Job Lifecycle

`pending` → `running` → `completed` | `failed` | `cancelled`

Jobs are stored in-memory with configurable TTL. Completed results are evicted after the TTL expires.
