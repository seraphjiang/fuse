# What's New in Fuse v1.0

This guide covers the major features added in Sprints 10–13.

---

## Arrow Flight Connector (Zero-Copy Streaming)

Connect to any Arrow Flight or Flight SQL server for zero-copy data transfer — no JSON serialization overhead.

### Configuration

```toml
[[connector]]
id = "my_flight"
type = "arrow-flight"
url = "grpc://flight-server:50051"
# mode = "flight-sql"   # or "flight" for ticket-based (default: flight-sql)
# token = "secret://fuse/flight-token"  # optional bearer token
```

### Modes

| Mode | Use case |
|------|----------|
| `flight-sql` (default) | SQL-capable servers (DataFusion, DuckDB, Spark) |
| `flight` | Custom ticket-based data retrieval |

### Query example

```sql
SELECT * FROM my_flight.measurements WHERE sensor_id = 'temp-01' LIMIT 100
```

Arrow Flight streams RecordBatches natively over gRPC, making it ideal for high-throughput cross-cluster queries and connecting to another Fuse instance.

---

## Write Path: CTAS and INSERT INTO SELECT

Fuse now supports writing query results back to connectors that implement `write_batches`.

### CREATE TABLE AS SELECT (CTAS)

```sql
CREATE TABLE my_pg.summary AS
SELECT service, count(*) as cnt
FROM cluster_a.application_logs
GROUP BY service
```

Creates the table in the target datasource and populates it with query results.

### INSERT INTO SELECT

```sql
INSERT INTO my_pg.error_archive
SELECT * FROM cluster_a.application_logs WHERE status >= 500
```

Appends query results to an existing table.

### Supported write targets

Any connector implementing `write_batches`: PostgreSQL, MySQL, SQLite, DuckDB. Other connectors are read-only.

### Transactions

```sql
BEGIN
INSERT INTO my_pg.table1 SELECT ...
INSERT INTO my_pg.table2 SELECT ...
COMMIT
```

`BEGIN`/`COMMIT`/`ROLLBACK` provide transaction boundaries for write operations.

---

## Datasource-Level RBAC

Role-based access control restricts which datasources a user can query.

### Permissions

| Permission | Allows |
|------------|--------|
| `Read` | SELECT queries |
| `Write` | INSERT, CTAS |
| `Admin` | Schema changes, DROP |

### Configuration

```toml
[[security.access_rules]]
datasource = "production_db"
roles = ["analyst", "admin"]
permission = "Read"

[[security.access_rules]]
datasource = "production_db"
roles = ["admin"]
permission = "Write"
```

### Behavior

- Queries referencing a datasource the user lacks `Read` permission for are rejected with `403 Forbidden`
- Cross-datasource JOINs require `Read` on all referenced datasources
- The `filter_readable` method filters the datasource list to only those the user can access

See the [Security Hardening Guide](guides/security-hardening-guide.md) for TLS, secret management, and error sanitization.

---

## Adaptive Query Timeout

Fuse automatically adjusts query timeouts based on observed datasource latency.

### How it works

1. Each query execution records the datasource latency
2. The system maintains a rolling p95 latency per datasource
3. Timeout = `p95 × 3`, clamped to `[5s, 120s]`

### Priority order

| Source | Example |
|--------|---------|
| Explicit `timeout_ms` in request | `{"query": "...", "timeout_ms": 10000}` |
| Adaptive (p95 × 3) | Auto-calculated per datasource |
| Default | 30 seconds |

### Configuration

The adaptive timeout is enabled by default. The `[5s, 120s]` clamp range prevents both premature timeouts on cold starts and runaway queries.

---

## Continuous Alert Monitor

Background monitoring that evaluates alert rules against live data and dispatches notifications.

### Alert rules

Define rules in `fuse.toml`:

```toml
[[alert]]
name = "high_error_rate"
query = "SELECT count(*) as cnt FROM cluster_a.logs WHERE status >= 500 AND timestamp > now() - interval '5 minutes'"
format = "sql"
interval_secs = 60

[alert.condition]
type = "threshold"
field = "cnt"
op = "gt"
value = 100

[[alert.notify]]
type = "webhook"
url = "https://hooks.example.com/fuse-alerts"
```

### API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/fuse/alerts` | GET | List configured alert rules |
| `/api/fuse/alerts/evaluate` | POST | Trigger immediate evaluation |

### Features

- **Background loop**: evaluates rules at configured intervals via `spawn_alert_loop`
- **State tracking**: fires on threshold breach, resolves when condition clears
- **Acknowledge**: suppress notifications for a known alert
- **History**: capped at 1000 entries, viewable in the playground Alerts page
- **Webhook notifications**: POST JSON payload to configured URLs on fire/resolve

### Playground

The Alerts page (`/alerts`) shows:
- Active alerts with acknowledge buttons
- Alert history timeline with status/search filters
- Stats cards (rules, firing, acknowledged)
