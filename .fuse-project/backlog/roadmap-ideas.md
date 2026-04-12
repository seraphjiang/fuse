# Roadmap Ideas — Pick Any Unassigned Item

## AI/ML (ai-lead owns)
- [ ] Query auto-tuning: analyze slow queries and suggest index/partition changes
- [ ] NL-to-SQL improvements: support multi-table JOINs in natural language
- [ ] Query similarity detection: find duplicate/similar queries across tenants
- [ ] Smart query routing: route to fastest connector based on historical latency
- [ ] Anomaly detection improvements: seasonal patterns, trend detection

## Ecosystem (sde owns)
- [ ] GraphQL subscriptions: real-time query result streaming
- [ ] SDK async support: async/await for Python, TypeScript, Go SDKs
- [ ] Webhook retry logic: exponential backoff, dead letter queue
- [ ] CDC improvements: multi-table materialized views
- [ ] REST API versioning: /v1/ and /v2/ prefix support

## Data Layer (dba owns)
- [ ] Query result compression: gzip/zstd for large result sets
- [ ] Connection pooling stats: expose pool utilization per connector
- [ ] Materialized view refresh optimization: incremental refresh
- [ ] Query plan visualization: tree/graph format for EXPLAIN output
- [ ] Connector health history: track uptime/latency over time

## Security (security owns)
- [ ] Audit logging: log all queries with tenant, timestamp, duration
- [ ] Column-level RBAC: restrict access to sensitive columns per role
- [ ] API key rotation: automated key rotation with grace period
- [ ] Request signing: HMAC-based request authentication
- [ ] Security headers: CSP, HSTS, X-Frame-Options for playground

## Infrastructure (devops owns)
- [ ] Blue-green deployment support in Helm
- [ ] Prometheus ServiceMonitor for auto-discovery
- [ ] Init container for config validation
- [ ] Resource quotas per namespace
- [ ] Backup/restore for query history and saved queries

## Test (test owns)
- [ ] Chaos testing: random connector failures during queries
- [ ] Performance regression detection: compare benchmark runs
- [ ] Fuzz testing: random SQL/PPL input generation
- [ ] Contract testing: verify API response schemas
- [ ] Load test scenarios: spike, soak, stress patterns

## Frontend (fee owns)
- [ ] Query diff viewer: side-by-side result comparison
- [ ] Dashboard builder: drag-and-drop query result widgets
- [ ] Export results: CSV, JSON, Parquet download buttons
- [ ] Query history search: filter by date, status, datasource
- [ ] Mobile responsive: playground works on tablets
