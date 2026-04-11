# Security Hardening Guide

Production security configuration for Fuse: TLS, RBAC, secret management, network isolation, and audit.

## TLS

### Server TLS

Terminate TLS at the Fuse server or at a reverse proxy.

**Option 1: Reverse proxy (recommended)**

Use nginx or ALB for TLS termination:

```nginx
server {
    listen 443 ssl;
    ssl_certificate     /etc/ssl/fuse/cert.pem;
    ssl_certificate_key /etc/ssl/fuse/key.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    ssl_ciphers         HIGH:!aNULL:!MD5;

    location / {
        proxy_pass http://127.0.0.1:9400;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
    }
}
```

**Option 2: Fuse native TLS**

```toml
[server]
bind = "0.0.0.0:9400"
tls_cert = "/etc/ssl/fuse/cert.pem"
tls_key = "/etc/ssl/fuse/key.pem"
```

### Connector TLS

Each connector inherits TLS from its URL scheme. Use `https://` for encrypted connections:

```toml
[[datasource]]
id = "cluster_a"
type = "opensearch"
url = "https://opensearch.internal:9200"
# For self-signed certs:
# tls_ca = "/etc/ssl/ca.pem"
# tls_skip_verify = false  # never set true in production

[[datasource]]
id = "pg"
type = "postgres"
url = "postgresql://user:pass@pg.internal:5432/db?sslmode=require"
```

## RBAC Configuration

### API Key Roles

```toml
# Read-only analyst
[[api_key]]
key = "ak-analyst-001"
identity = "analyst-team"
role = "viewer"

# Dashboard editor
[[api_key]]
key = "ak-editor-001"
identity = "dashboard-team"
role = "editor"

# Full admin
[[api_key]]
key = "ak-admin-001"
identity = "ops-team"
role = "admin"
```

### Role Permissions

| Endpoint | viewer | editor | admin |
|----------|--------|--------|-------|
| `POST /api/fuse/query` | ✅ | ✅ | ✅ |
| `POST /api/fuse/query/explain` | ✅ | ✅ | ✅ |
| `GET /api/fuse/datasources` | ✅ | ✅ | ✅ |
| `GET /api/fuse/health` | ✅ (public) | ✅ | ✅ |
| `POST /api/fuse/saved-queries` | ❌ | ✅ | ✅ |
| `POST /api/fuse/dashboards` | ❌ | ✅ | ✅ |
| `GET /api/fuse/history` | ❌ | ❌ | ✅ |
| `GET /api/fuse/plugins` | ❌ | ❌ | ✅ |
| `GET /api/fuse/views` | ✅ | ✅ | ✅ |

### Tenant Isolation

Combine RBAC with multi-tenancy for defense in depth:

```toml
# Analyst can only query cluster_a
[[tenant]]
id = "analyst-team"
datasources = ["cluster_a"]
max_rows = 5000
max_time_ms = 15000

# Ops team has full access
[[tenant]]
id = "ops-team"
datasources = []  # empty = all
```

A viewer with tenant isolation can only read from their allowed datasources — they can't escalate to other datasources even if they know the IDs.

## Secret Management

### Environment Variables

Never put credentials in `fuse.toml`. Use environment variables:

```toml
[[datasource]]
id = "pg"
type = "postgres"
url = "${PG_URL}"           # resolved from env at startup

[[datasource]]
id = "cluster_a"
type = "opensearch"
url = "${OS_URL}"
```

```bash
export PG_URL="postgresql://user:secret@pg.internal:5432/db?sslmode=require"
export OS_URL="https://admin:secret@opensearch.internal:9200"
export REDIS_URL="redis://:secret@redis.internal:6379"
```

### API Key Rotation

1. Add the new key to `fuse.toml` with the same identity and role
2. Restart Fuse (both old and new keys work)
3. Migrate clients to the new key
4. Remove the old key from `fuse.toml`
5. Restart Fuse (old key is now invalid)

In stateless mode, update keys in Redis for zero-downtime rotation:

```bash
redis-cli SET "fuse:apikey:ak-new-001" '{"identity":"ops-team","role":"admin"}'
redis-cli DEL "fuse:apikey:ak-old-001"
```

### File Permissions

```bash
chmod 600 /etc/fuse/fuse.toml       # only owner can read config with keys
chmod 600 /etc/ssl/fuse/key.pem     # TLS private key
chmod 644 /etc/ssl/fuse/cert.pem    # TLS cert (public)
```

## Network Isolation

### Bind to Localhost

If using a reverse proxy on the same host:

```toml
[server]
bind = "127.0.0.1:9400"   # only accessible via proxy
```

### Firewall Rules

```bash
# Only allow HTTPS from load balancer
iptables -A INPUT -p tcp --dport 443 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 443 -j DROP

# Block direct access to Fuse port
iptables -A INPUT -p tcp --dport 9400 -s 127.0.0.1 -j ACCEPT
iptables -A INPUT -p tcp --dport 9400 -j DROP
```

### Docker Network Isolation

```yaml
services:
  fuse:
    networks:
      - frontend    # nginx can reach fuse
      - backend     # fuse can reach datasources
  nginx:
    networks:
      - frontend    # exposed to clients
  opensearch:
    networks:
      - backend     # only fuse can reach opensearch

networks:
  frontend:
  backend:
    internal: true  # no external access
```

## Rate Limiting

```toml
[engine]
rate_limit_global = 1000    # total requests/minute
rate_limit_per_ip = 50      # per client IP
```

Exceeding limits returns HTTP 429 with `Retry-After` header. Combine with per-tenant query governor limits for layered protection.

## Audit Logging

Enable structured audit logs for compliance:

```bash
# All access attempts (successful and denied)
grep '"audit":true' /var/log/fuse/fuse.log

# Denied access (unauthorized datasource, invalid key, rate limited)
grep '"status":"Denied"' /var/log/fuse/fuse.log

# Admin actions
grep '"identity":"ops-team"' /var/log/fuse/fuse.log
```

Ship audit logs to a separate, append-only store for tamper resistance.

## Security Checklist

| Item | Status |
|------|--------|
| TLS enabled (server or proxy) | ☐ |
| Connector TLS configured (custom CA or mTLS) | ☐ |
| All connector URLs use `https://` or `sslmode=require` | ☐ |
| API keys configured (no anonymous access) | ☐ |
| Tenant isolation enabled | ☐ |
| Query governor limits set per tenant | ☐ |
| Rate limiting enabled | ☐ |
| Secrets use `secret://` references (not plaintext) | ☐ |
| Config file permissions 600 | ☐ |
| Fuse bound to localhost (behind proxy) | ☐ |
| Audit logging enabled and shipped to secure store | ☐ |
| API keys rotated on schedule | ☐ |
| Docker networks isolated (frontend/backend) | ☐ |
| WASM plugins loaded from trusted directory only | ☐ |

## Connector TLS/mTLS

All HTTP-based connectors support custom CA certificates and mutual TLS (mTLS)
via the `[connector.tls]` config block:

```toml
[[connector]]
id = "my_service"
type = "opensearch"
url = "https://my-cluster:9200"

[connector.tls]
ca_cert = "/etc/ssl/custom-ca.pem"        # Custom CA certificate
client_cert = "/etc/ssl/client-cert.pem"   # mTLS client certificate
client_key = "/etc/ssl/client-key.pem"     # mTLS client private key
```

Supported connectors: OpenSearch, Elasticsearch, ClickHouse, Prometheus,
InfluxDB, Kafka, Snowflake, BigQuery, Fuse-to-Fuse. AWS SDK connectors
(DynamoDB, S3, CloudWatch, Athena, Timestream) handle TLS internally.

Kafka uses rustls via rskafka's `transport-tls` feature. Arrow Flight uses
tonic gRPC with TLS via `grpcs://` URL scheme.

**Warning**: Elasticsearch supports `tls_insecure = true` to skip certificate
validation. This logs a startup warning and should only be used for development.

## Secret Management

Use `secret://` references to avoid plaintext credentials in config files:

```toml
[[connector]]
id = "my_pg"
type = "postgres"
[connector.auth]
username = "app_user"
password = "secret://fuse/pg-password"   # Resolved from AWS Secrets Manager
```

Secrets are resolved recursively into nested TOML tables (e.g., `[connector.auth]`
blocks). Only secret *names* are logged — values are never written to logs or
trace spans.

## WASM Plugin Sandboxing

WASM connector plugins run with resource limits to prevent denial of service:

- **Fuel metering**: Each call limited to 10M fuel units (prevents infinite loops)
- **Input size**: Capped at 1 MiB per function call
- **No WASI**: Plugins cannot access filesystem, network, or environment variables
- **Fresh isolation**: Each invocation gets a new WASM Store (no state leakage)

Load plugins only from a trusted directory. No signature verification is
performed on `.wasm` files — ensure the plugin directory has restricted
write permissions.

## SQL Injection Prevention

All SQL/CQL pushdown connectors quote identifiers to prevent injection:

- PostgreSQL, DuckDB, Athena, Timestream, Snowflake, Arrow Flight, Cassandra:
  ANSI double-quote (`"identifier"`)
- ClickHouse, BigQuery: backtick (`` `identifier` ``)
- InfluxDB: sanitize_identifier (alphanumeric filter)

String values are escaped via `'` → `''` doubling in all connectors.
OpenSearch and Elasticsearch use JSON serialization (serde_json), which
prevents NoSQL injection by construction.

## Reporting Vulnerabilities

See [SECURITY.md](https://github.com/seraphjiang/fuse/blob/main/SECURITY.md) for responsible disclosure.
