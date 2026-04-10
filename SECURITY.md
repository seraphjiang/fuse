# Security Policy

## Reporting Vulnerabilities

**Do not open a public GitHub issue for security vulnerabilities.**

To report a vulnerability, email the maintainers at the address listed in the repository's `CODEOWNERS` file with:

- Description of the vulnerability
- Steps to reproduce
- Impact assessment
- Suggested fix (if any)

We will acknowledge receipt within 48 hours and provide an initial assessment within 7 days. We follow coordinated disclosure — please allow us reasonable time to release a fix before public disclosure.

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.5.x | ✅ Current |
| 0.4.x | ✅ Security fixes |
| 0.3.x | ❌ End of life |
| < 0.3 | ❌ End of life |

## Security Features

### API Key Authentication

Fuse supports API key authentication via `x-api-key` header or `Authorization: Bearer <key>`. When enabled, unauthenticated requests receive 401.

```toml
[[api_key]]
key = "your-secret-key"
identity = "my-service"
role = "viewer"   # viewer, editor, admin
```

Health (`/api/fuse/health`) and metrics (`/metrics`) endpoints are always public.

### Role-Based Access Control (RBAC)

Three roles control access:
- **Viewer** — read-only queries and schema discovery
- **Editor** — queries + saved queries + dashboard management
- **Admin** — full access including configuration

Field-level security via `PolicyEngine` can restrict which columns are visible per role.

### Rate Limiting

Configurable per-IP and global request limits prevent abuse:

```toml
[engine]
rate_limit_global = 1000    # requests per minute (all clients)
rate_limit_per_ip = 100     # requests per minute (per client IP)
```

Exceeding limits returns HTTP 429.

### Query Timeout

Every query has a configurable timeout to prevent resource exhaustion:

```toml
[engine]
default_timeout = "30s"
```

Per-query override via `timeout_ms` in the request body. Timed-out queries are cancelled automatically.

### SQL Injection Protection

Fuse does not execute raw SQL against backend datasources. Queries are parsed into a structured `SubQuery` representation, then translated to each connector's native format:

- **Parameterized queries** — `$name` parameters are bound safely with quote escaping (`'` → `''`)
- **String literal stripping** — SQL parsing operates on stripped queries to prevent injection via string contents
- **Connector-specific translation** — OpenSearch gets Query DSL JSON, DynamoDB gets expression attributes, MongoDB gets BSON documents. No raw SQL concatenation.

### Connector Authentication

Each connector manages its own authentication independently:

| Connector | Auth Methods |
|-----------|-------------|
| OpenSearch | SigV4 (IAM), Basic auth |
| Elasticsearch | API key, Basic auth |
| PostgreSQL / MySQL | Connection string credentials |
| DynamoDB / S3 / CloudWatch | SigV4 (IAM default credential chain) |
| Prometheus | Bearer token |
| MongoDB | Connection string |
| InfluxDB | Token (v2), Basic (v1) |
| ClickHouse | Basic (HTTP) |
| Redis | Password (in URL) |

Credentials are stored in `fuse.toml`. Use environment variables or secrets management for production deployments — do not commit credentials to version control.

### Read-Only by Design

Fuse is a read-only query engine. It never writes to, modifies, or deletes data in any connected datasource. There are no `INSERT`, `UPDATE`, `DELETE`, or `DROP` operations.

### Network Security

- Fuse binds to a configurable address (`0.0.0.0:9400` by default). Restrict to `127.0.0.1:9400` for local-only access.
- All connector connections support TLS. Use `https://` endpoints for OpenSearch, Elasticsearch, and other connectors.
- No data is stored persistently by Fuse — query results exist only in memory during execution.

## Security Checklist for Deployment

- [ ] Enable API key authentication in production
- [ ] Set `rate_limit_per_ip` to prevent abuse
- [ ] Set `default_timeout` to prevent resource exhaustion
- [ ] Bind to `127.0.0.1` or use a reverse proxy with TLS
- [ ] Use IAM/SigV4 for AWS connectors instead of static credentials
- [ ] Store `fuse.toml` credentials via environment variables or secrets manager
- [ ] Review connector `max_concurrent_queries` to prevent overloading backends
