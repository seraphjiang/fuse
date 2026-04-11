# Fuse Error Code Reference

All Fuse errors include a structured code in the format `FUSE-XXXX` for machine-readable identification.

## Error Code Ranges

| Range | Category | Description |
|-------|----------|-------------|
| FUSE-1xxx | Configuration | Config file parsing, missing fields, secret resolution |
| FUSE-2xxx | Connector / Registry | Connector creation, registration, query execution |
| FUSE-3xxx | Query Parsing | SQL/PPL syntax errors |
| FUSE-4xxx | Query Planning | Plan generation, optimization failures |
| FUSE-5xxx | Execution | Runtime query execution errors |
| FUSE-6xxx | Authentication | Auth failures, connection refused |
| FUSE-7xxx | I/O / Transport | File I/O, Arrow serialization, network |

## Error Codes

### Configuration (FUSE-1xxx)

| Code | Name | Description |
|------|------|-------------|
| FUSE-1000 | CONFIG_INVALID | General configuration error (invalid TOML, bad values) |
| FUSE-1001 | CONFIG_MISSING_FIELD | Required configuration field is missing |
| FUSE-1002 | CONFIG_SECRET_RESOLVE | Failed to resolve a `secret://` reference via Secrets Manager |

### Connector / Registry (FUSE-2xxx)

| Code | Name | Description |
|------|------|-------------|
| FUSE-2000 | CONNECTOR_ERROR | General connector error |
| FUSE-2001 | REGISTRY_DUPLICATE | Connector with the same ID already registered |
| FUSE-2002 | REGISTRY_NOT_FOUND | Referenced connector/datasource not found |
| FUSE-2010 | CONNECTOR_QUERY_FAILED | Query execution failed on a connector |
| FUSE-2011 | CONNECTOR_SCHEMA_FAILED | Schema discovery failed on a connector |
| FUSE-2012 | CONNECTOR_UNSUPPORTED | Connector does not support the requested operation |
| FUSE-2013 | CONNECTOR_CHANNEL_CLOSED | Streaming channel closed unexpectedly |

### Query Parsing (FUSE-3xxx)

| Code | Name | Description |
|------|------|-------------|
| FUSE-3000 | PARSE_ERROR | SQL or PPL query syntax error |

### Query Planning (FUSE-4xxx)

| Code | Name | Description |
|------|------|-------------|
| FUSE-4000 | PLAN_ERROR | Query plan generation or optimization failed |

### Execution (FUSE-5xxx)

| Code | Name | Description |
|------|------|-------------|
| FUSE-5000 | EXECUTION_ERROR | Runtime query execution error |

### Authentication (FUSE-6xxx)

| Code | Name | Description |
|------|------|-------------|
| FUSE-6000 | AUTH_FAILED | Authentication failed (bad credentials, expired token) |
| FUSE-6001 | AUTH_CONNECTION_FAILED | Connection to datasource refused or timed out |

### I/O / Transport (FUSE-7xxx)

| Code | Name | Description |
|------|------|-------------|
| FUSE-7000 | IO_ERROR | File or network I/O error |
| FUSE-7001 | ARROW_ERROR | Apache Arrow serialization/deserialization error |

## Error Response Format

All API error responses include the error code:

```json
{
  "error": "[FUSE-2002] connector 'missing_ds' not found",
  "code": "FUSE-2002"
}
```

## Troubleshooting by Code

- **FUSE-1xxx**: Check `fuse.toml` syntax and required fields. Run with `RUST_LOG=debug` for config details.
- **FUSE-2001**: You have duplicate connector IDs in `fuse.toml`. Each `[[connector]]` must have a unique `id`.
- **FUSE-2002**: The datasource name in your query doesn't match any configured connector. Check `GET /api/fuse/datasources`.
- **FUSE-2010**: The downstream datasource returned an error. Check connector health via `GET /api/fuse/health`.
- **FUSE-3000**: SQL/PPL syntax error. Use `EXPLAIN` to validate your query structure.
- **FUSE-6000/6001**: Check connector auth config — credentials, IAM roles, API keys, or network connectivity.

## Runtime Errors (non-coded)

These errors are returned as plain messages without a `FUSE-XXXX` code:

| Error | Cause | Fix |
|-------|-------|-----|
| `rate limit exceeded` | Per-datasource or global rate limit hit | Reduce query frequency or increase `rate_limit` in config |
| `rate limit exceeded for API key` | Per-tenant API key rate limit | Contact admin to increase tenant quota |
| `tenant exceeded rate limit: N queries/min` | Multi-tenancy query governor | Reduce query volume or request higher quota |
| `query governor: max rows exceeded` | Result set exceeds `max_rows` limit | Add `LIMIT` clause or increase governor limit |
| `query governor: execution time exceeded` | Query exceeded `max_execution_time_ms` | Optimize query or increase timeout |
| `query governor: result size exceeded` | Response exceeds `max_result_bytes` | Reduce columns/rows or increase limit |
| `Failed to deserialize the JSON body` | Malformed request JSON | Check request body format against API docs |
