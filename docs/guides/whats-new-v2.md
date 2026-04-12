# What's New in Fuse v2.0

Fuse v2.0 is a major feature release delivering 16 new capabilities across
scheduling, data quality, developer experience, performance, governance, and
observability. Built in Sprint 18 with 2600+ tests passing.

## Highlights

### Scheduled Queries (Cron)
Run queries on a schedule with cron expressions. Store results, track execution
history, and alert on changes. Lightweight ETL without leaving Fuse.

```bash
POST /api/fuse/schedules
{ "name": "hourly_errors", "cron": "0 * * * *",
  "query": "SELECT count(*) FROM cluster_a.logs WHERE status >= 500" }
```

### Data Quality Rules Engine
Define expectations per datasource — null rate, cardinality, freshness, row
count. Evaluate on schedule or per-query. Alert when data drifts.

```bash
POST /api/fuse/quality/rules
{ "datasource": "cluster_a", "table": "logs",
  "checks": [{"type": "null_rate", "column": "user_id", "max": 0.05}] }
```

### GraphQL API
Full alternative to REST at `/api/fuse/graphql`. Schema introspection maps to
datasource schemas. Execute queries, manage saved queries and views, browse
history — all via GraphQL. Includes GraphiQL playground on GET.

```graphql
mutation {
  executeQuery(input: { query: "SELECT * FROM cluster_a.logs LIMIT 5" }) {
    columns rows rowCount latencyMs
  }
}
```

### Arrow IPC Result Format
Return query results as Arrow IPC bytes for zero-copy consumption by Python
(pyarrow), Rust, and other Arrow-native clients. Supports both stream and file
formats.

```bash
curl -H "Accept: application/vnd.apache.arrow.stream" \
  -X POST http://localhost:9400/api/fuse/query \
  -d '{"query": "SELECT * FROM cluster_a.logs LIMIT 100"}' -o results.arrow
```

### Query Cost Estimation ($)
Real dollar estimates before execution: Athena $/TB scanned, DynamoDB $/RCU,
S3 $/request. Per-connector cost models in EXPLAIN output.

### Webhook Subscriptions
Register a callback URL with a query and condition. When evaluated, the query
runs and the webhook fires if the condition is met (rows returned or threshold
exceeded).

```bash
POST /api/fuse/webhooks
{ "name": "error_spike", "query": "SELECT count(*) as cnt FROM cluster_a.logs WHERE status >= 500",
  "condition": {"type": "threshold", "column": "cnt", "operator": "gt", "value": 100},
  "callback_url": "https://hooks.example.com/fuse" }
```

### Schema Relationship Discovery
Auto-detect foreign key relationships across datasources by column name
patterns and type compatibility. Suggests JOIN keys for cross-source queries.

```bash
GET /api/fuse/relationships
# Returns: [{"left_datasource":"cluster_a","left_column":"user_id",
#            "right_datasource":"dynamodb","right_column":"user_id",
#            "method":"name_match","confidence":0.8}]
```

### Federated Materialized Views with CDC
Change Data Capture tracker auto-refreshes materialized views when source data
changes. Register views with datasource dependencies, ingest change events, and
views are marked for refresh automatically.

### Query Replay & Regression Testing
Record production queries, replay against staging, diff results. Catch breaking
changes before deploy.

### Adaptive Query Caching
Learns which queries repeat and auto-caches results with per-datasource TTL
based on data freshness patterns.

### Parallel Fan-out with Backpressure
Adaptive concurrency per datasource — fast connectors stream results
immediately while slow connectors don't block others.

### Query Compilation
Skip re-parsing and planning for hot query patterns. Compiled queries go
straight to execution.

### Query Lineage & Data Catalog
Track data flow across connectors. Per-query lineage graph showing
source → transform → sink relationships.

### Multi-tenant SaaS Mode
Tenant isolation with usage metering (queries, rows, bytes per tenant),
rate limiting, and billing integration.

### OpenTelemetry Collector Mode
Fuse as an OTel backend — ingest OTLP traces, metrics, and logs via
`/v1/traces`, `/v1/metrics`, `/v1/logs`, then query with SQL.

### Query Explanation in Plain English
"This query joins error logs with user profiles to find premium users
hitting 500 errors." Reverse NL generation from query plans.

### Predictive Query Performance
Estimate query latency before execution using historical data. Matches
query shape and datasource patterns.

```bash
GET /api/fuse/predict?query=SELECT * FROM cluster_a.logs JOIN dynamodb.users ON ...
# Returns: {"estimated_ms": 450, "confidence": "high", "sample_count": 15}
```

## SDK Updates

All three SDKs (Python, TypeScript, Go) updated with Sprint 18 endpoints:
webhooks, relationships, CDC, and predictive performance.

## Playground

Seven new playground pages: Schedules, Quality, Lineage, Replay, OTel Status,
Adaptive Cache Stats, and Demo Tour.

## Numbers

- 16 new features shipped
- 2600+ tests passing
- 30+ commits
- 18 playground pages
- 25 connectors
- OpenAPI spec v2.0.0
