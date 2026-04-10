# Fuse v0.6.0: Enterprise, AI, and SDKs

*April 2026 · Fuse Team*

Fuse v0.6.0 completes the journey from query engine to enterprise analytics platform. This release adds multi-tenancy, AI-powered query generation, Python and TypeScript SDKs, and a Grafana datasource plugin.

## Enterprise Stack

### Multi-Tenancy

Isolate datasource access per team. Each API key maps to a tenant with a datasource allowlist — team-alpha sees `cluster_a` and `s3_o11y`, team-beta sees `cluster_b` and `dynamodb`. Unknown tenants get zero access.

```toml
[[tenant]]
id = "team-alpha"
datasources = ["cluster_a", "s3_o11y"]
max_rows = 10000
max_time_ms = 30000
```

*[Screenshot: Admin page showing tenant configuration with datasource checkboxes and resource limits]*

### Query Governor

Per-tenant resource limits prevent runaway queries:
- **max_rows** — truncates results beyond the limit
- **max_time_ms** — cancels queries that exceed the time budget
- **max_result_bytes** — rejects oversized results

Limits are enforced transparently — tenants see clean error messages, not crashes.

### Audit Logging

Every API action is recorded: who queried what, when, from where, and what happened. Structured JSON logs integrate with your existing log pipeline.

```json
{"audit":true, "identity":"team-alpha", "action":"Query", "status":"Success", "duration_ms":45, "row_count":20}
```

Filter by tenant, action, or status with standard tools. The admin page shows a live audit feed.

### API Key Authentication

Production-ready auth with three roles:
- **Viewer** — read-only queries and schema discovery
- **Editor** — queries + saved queries + dashboard management
- **Admin** — full access including tenant and key management

```bash
curl -H "x-api-key: your-key" http://localhost:9400/api/fuse/datasources
```

## AI-Powered Queries

### Natural Language → SQL

Describe what you want in plain English. Fuse generates the SQL using your actual schema:

```bash
curl -X POST http://localhost:9400/api/fuse/nl \
  -d '{"prompt": "show me the top 5 services with the most errors in the last hour"}'
```

```json
{
  "sql": "SELECT service, count(*) as errors FROM cluster_a.application_logs WHERE status >= 500 GROUP BY service ORDER BY errors DESC LIMIT 5",
  "explanation": "Counts rows with status >= 500, grouped by service, sorted by error count descending"
}
```

*[Screenshot: Playground with NL input bar showing "services with most errors" and generated SQL below]*

### Query Advisor

Get optimization recommendations for any query:

```bash
curl http://localhost:9400/api/fuse/advisor?query=SELECT+*+FROM+cluster_a.application_logs
```

Suggestions include: add LIMIT, use explicit columns, add WHERE for pushdown, use cursor pagination for large results.

### Auto-Suggest

The playground suggests interesting queries when you select a datasource — based on schema analysis and common patterns.

## SDKs

### Python

```bash
pip install fuse-client
```

```python
from fuse_client import FuseClient
import pandas as pd

fuse = FuseClient("http://localhost:9400", api_key="your-key")
result = fuse.query("SELECT service, count(*) as n FROM cluster_a.application_logs GROUP BY service")
df = pd.DataFrame(result.rows, columns=result.columns)
```

Zero dependencies (stdlib only). Supports query, pagination, trace, explain, health. See the [Python SDK Guide](./python-sdk.md).

### TypeScript

```bash
npm install fuse-client
```

```typescript
import { FuseClient } from 'fuse-client';

const fuse = new FuseClient({ baseUrl: 'http://localhost:9400', apiKey: 'your-key' });
const result = await fuse.query('SELECT service, count(*) as n FROM cluster_a.application_logs GROUP BY service');
console.log(result.rows);
```

Zero dependencies (uses native `fetch`). Works in Node.js, Deno, and browsers. See the [TypeScript SDK Guide](./typescript-sdk.md).

## Grafana Plugin

Use Fuse as a Grafana datasource. Cross-datasource JOINs, UNION ALL, and federated aggregations — all from Grafana panels.

```sql
-- Grafana panel query: time series from federated sources
SELECT DATE_TRUNC('hour', timestamp) as time, _datasource, count(*) as n
FROM cluster_a.application_logs
UNION ALL
SELECT DATE_TRUNC('hour', timestamp) as time, _datasource, count(*) as n
FROM cluster_b.application_logs
GROUP BY time, _datasource
ORDER BY time
```

Supports Grafana template variables, alerting, and all visualization types. See the [Grafana Plugin Guide](./grafana-plugin.md).

*[Screenshot: Grafana dashboard with Fuse datasource showing time series panel with cross-cluster data]*

## New Connectors

**DuckDB** joins the lineup — in-process analytics with Arrow-native data exchange. Perfect for local analysis alongside remote datasources:

```sql
-- Join local DuckDB analytics with remote OpenSearch logs
SELECT d.category, l.service, count(*) as n
FROM duckdb.product_catalog d
JOIN cluster_a.application_logs l ON d.product_id = l.product_id
GROUP BY d.category, l.service
```

Total: 15 connector types (OpenSearch, Elasticsearch, PostgreSQL, MySQL, DynamoDB, S3 Parquet, S3 O11y, Prometheus, CloudWatch, Redis, CSV/JSON, MongoDB, InfluxDB, ClickHouse, DuckDB).

## Security

All 14+ connectors passed security audit. InfluxDB and Prometheus injection vulnerabilities (flagged in Sprint 5) are fixed — quote escaping in InfluxQL, Flux, and PromQL filter paths.

## By the Numbers

| Metric | v0.5.0 | v0.6.0 |
|--------|--------|--------|
| Connectors | 14 | 15 |
| Tests | 983 | 1,072 |
| Docs pages | 12 | 25 |
| SDKs | 0 | 2 (Python + TypeScript) |
| API endpoints | 19 | 21 (+NL, +advisor) |
| Chart types | 12 | 12 |
| Web app pages | 7 | 8 (+Admin) |

## Documentation

The docs site grew to 25 pages across 6 sections:
- [Admin Guide](./admin-guide.md) — multi-tenancy, auth, rate limiting, audit, monitoring
- [Python SDK](./python-sdk.md) / [TypeScript SDK](./typescript-sdk.md) — quickstart guides
- [Jupyter Integration](./jupyter-integration.md) — DataFrames, visualization, notebooks
- [Grafana Plugin](./grafana-plugin.md) — setup, dashboards, variables, alerting
- [Troubleshooting FAQ](./troubleshooting-faq.md) — 14 common issues with fixes
- [Community Guide](./community-guide.md) — how to get involved

## Try It

**Playground:** [https://fuse.huanji.profile.aws.dev](https://fuse.huanji.profile.aws.dev)

**From source:**
```bash
git clone https://github.com/seraphjiang/fuse && cd fuse
cargo build --release
./target/release/fuse-server --config fuse.toml
```

**Python:** `pip install fuse-client`

**TypeScript:** `npm install fuse-client`

## What's Next

- Remaining connectors: Redshift, SQLite, Kafka, Timestream, Snowflake
- PyPI and npm publishing
- Grafana plugin verification
- Enterprise stack end-to-end integration testing

See the full [Roadmap](./roadmap.md). Contributions welcome — [CONTRIBUTING.md](https://github.com/seraphjiang/fuse/blob/main/CONTRIBUTING.md).
