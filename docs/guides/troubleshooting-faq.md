# Troubleshooting FAQ

Common issues and how to fix them.

---

### "no datasource.table references found"

Your query doesn't use `datasource.table` format.

```sql
-- ❌ Wrong
SELECT * FROM application_logs

-- ✅ Correct
SELECT * FROM cluster_a.application_logs
```

The datasource ID comes from `fuse.toml`. List available IDs:
```bash
curl http://localhost:9400/api/fuse/datasources
```

---

### "datasource 'X' not found"

The datasource ID in your query doesn't match any registered connector.

**Fix:**
1. Check spelling — IDs are case-sensitive
2. Verify `fuse.toml` has a `[[connector]]` block with `id = "X"`
3. Restart the server after editing `fuse.toml`

---

### Empty results (0 rows)

The query ran but returned nothing.

**Fix:**
1. Verify data exists: `SELECT * FROM datasource.table LIMIT 1`
2. Check table name: `curl http://localhost:9400/api/fuse/datasources/{id}/schemas`
3. Relax your WHERE clause — start broad, then narrow
4. For OpenSearch: verify the index has documents (`GET /_cat/indices`)

---

### Query timeout

The query exceeded `timeout_ms` (default 30s).

**Fix:**
1. Add `LIMIT` to reduce result size
2. Add `WHERE` clauses to push filters to connectors
3. Use explicit column lists instead of `SELECT *`
4. Increase timeout: `{"query": "...", "timeout_ms": 60000}`
5. Check connector health — a slow connector blocks the whole query:
   ```bash
   curl http://localhost:9400/api/fuse/health
   ```

---

### Slow queries / pushdown not working

Fuse is fetching all data and filtering locally instead of pushing down.

**Diagnose:**
```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -d '{"query": "YOUR QUERY", "analyze": true}'
```

Check `pushdown` badges on RemoteScan nodes. If empty, the connector doesn't support that operation.

**Fix:**
- Use columns the connector can filter on (see [pushdown table](performance-tuning-guide.md))
- Add `LIMIT` — almost all connectors support limit pushdown
- Use explicit column lists for projection pushdown
- For S3/CSV connectors: pushdown is minimal — always use LIMIT

---

### "query cancelled"

The query was cancelled via the cancel endpoint or timed out.

**Fix:** Increase `timeout_ms` or optimize the query (add LIMIT, WHERE, projections).

---

### HTTP 401 Unauthorized

API key authentication is enabled but no key was provided.

**Fix:**
```bash
# Add x-api-key header
curl -H "x-api-key: your-key" http://localhost:9400/api/fuse/datasources

# Or use Bearer token
curl -H "Authorization: Bearer your-key" http://localhost:9400/api/fuse/datasources
```

Health (`/api/fuse/health`) and metrics (`/metrics`) are always public.

---

### HTTP 429 Too Many Requests

Rate limit exceeded.

**Fix:** Reduce request frequency. Limits reset automatically (per-minute window). To increase limits, edit `fuse.toml`:
```toml
[engine]
rate_limit_global = 2000
rate_limit_per_ip = 200
```

---

### JOIN returns no results

No common column found, or no matching rows.

**Fix:**
1. Verify both sides have data: query each side separately with `LIMIT 5`
2. Check the join key exists in both tables:
   ```bash
   curl http://localhost:9400/api/fuse/datasources/{id}/schemas/{table}/fields
   ```
3. Verify join key values overlap — query distinct values from each side
4. Fuse auto-detects the join key from common column names. If column names differ, use explicit `ON a.col = b.col`

---

### UNION ALL schema mismatch

Columns don't align across sources.

**Fix:** Use explicit column lists with matching types:
```sql
-- ❌ SELECT * may have different columns per source
SELECT * FROM os.logs UNION ALL SELECT * FROM cw.logs

-- ✅ Explicit columns ensure alignment
SELECT timestamp, service, message FROM os.logs
UNION ALL
SELECT timestamp, service, message FROM cw.logs
```

Fuse widens types automatically (Int32 + Int64 → Int64), but column count must match.

---

### Connector shows "degraded" or "unhealthy"

The connector's health check failed.

**Diagnose:**
```bash
curl http://localhost:9400/api/fuse/health
```

**Common causes:**
- Network: connector endpoint unreachable (check URL in `fuse.toml`)
- Auth: expired credentials (SigV4 token, API key, password)
- Service: the backend datasource is down or overloaded
- Config: wrong region, bucket, log group, or database name

---

### "ChannelClosed" error in streaming

The SSE streaming connection was dropped.

**Fix:** This usually means the client disconnected. For `curl`, use `-N` to disable buffering:
```bash
curl -N -X POST http://localhost:9400/api/fuse/query/stream \
  -d '{"query": "SELECT * FROM cluster_a.application_logs LIMIT 100"}'
```

---

### Dashboard panels show "Not enough data"

The query returned too few rows or wrong column types for the selected chart.

**Fix:**
1. Switch chart type to "Table" to see raw data
2. Ensure the query returns at least one numeric column for charts
3. For pie charts: need a category column + a numeric column, ≤12 rows
4. For line charts: need a timestamp or ordered column + a numeric column
5. Try "Auto" chart type — Fuse picks the best match

---

### Build fails after adding a connector

**Fix:**
1. Verify crate is in root `Cargo.toml` members list
2. Verify dependency in `crates/fuse-server/Cargo.toml`
3. Run `cargo check` from repo root (not from the crate directory)
4. Check for missing `#[derive(Debug)]` on your connector struct — the trait requires it

---

## Diagnostic Commands

```bash
# Health + connector status
curl -s http://localhost:9400/api/fuse/health | python3 -m json.tool

# List datasources
curl -s http://localhost:9400/api/fuse/datasources | python3 -m json.tool

# Schema discovery
curl -s http://localhost:9400/api/fuse/datasources/{id}/schemas | python3 -m json.tool

# Validate query syntax
curl -s -X POST http://localhost:9400/api/fuse/query/validate \
  -d '{"query": "YOUR QUERY"}' | python3 -m json.tool

# Explain query plan
curl -s -X POST http://localhost:9400/api/fuse/query/explain \
  -d '{"query": "YOUR QUERY"}' | python3 -m json.tool

# Recent query history (check for errors)
curl -s http://localhost:9400/api/fuse/history | python3 -m json.tool
```
