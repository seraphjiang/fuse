# Troubleshooting

## Common Errors

### "datasource not found"

The datasource ID in your query doesn't match any registered connector.

**Fix:** Check your `fuse.toml` for the correct `id` values. List available datasources:

```bash
curl http://localhost:9400/api/fuse/datasources
```

### "empty results"

The query executed successfully but returned no rows.

**Fix:**
1. Verify the index/table has data: `SELECT * FROM datasource.table LIMIT 1`
2. Check your WHERE clause isn't too restrictive
3. Verify the table name is correct via schema discovery:
   ```bash
   curl http://localhost:9400/api/fuse/datasources/{id}/schemas
   ```

### "timeout" or slow queries

The connector took too long to respond.

**Fix:**
1. Add or reduce `LIMIT`
2. Add `WHERE` clauses to filter at the source
3. Check connector health: `curl http://localhost:9400/api/fuse/health`
4. Use `EXPLAIN` to verify pushdown is working
5. Check network connectivity to the datasource

### "degraded health"

One or more connectors are reporting unhealthy status.

**Fix:**
1. Check health endpoint for details: `curl http://localhost:9400/api/fuse/health`
2. Verify connector config in `fuse.toml` (endpoint, auth, region)
3. Check that the target service (OpenSearch, S3, Prometheus) is accessible
4. For SigV4 auth: verify IAM credentials are valid

### "parse error"

SQL or PPL syntax is invalid.

**Fix:**
1. Validate first: `POST /api/fuse/query/validate`
2. Check table format: `datasource_id.table_name`
3. For PPL: ensure pipe syntax `source = ds.table | command`
4. For SQL: check standard SQL syntax

### Rate limited (429)

Too many requests in a short period.

**Fix:** Reduce request frequency. The rate limiter resets automatically.

## Diagnostic Commands

```bash
# Full health check
curl -s http://localhost:9400/api/fuse/health | python3 -m json.tool

# List all datasources and their types
curl -s http://localhost:9400/api/fuse/datasources | python3 -m json.tool

# Check recent query history for errors
curl -s http://localhost:9400/api/fuse/history | python3 -m json.tool

# Validate a query without executing
curl -s -X POST http://localhost:9400/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "YOUR QUERY HERE"}' | python3 -m json.tool

# Explain a query plan
curl -s -X POST http://localhost:9400/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "YOUR QUERY HERE"}' | python3 -m json.tool
```
