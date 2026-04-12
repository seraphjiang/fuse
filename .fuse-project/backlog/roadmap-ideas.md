# Roadmap Ideas — ALL COMPLETE (Overnight Run 2026-04-12)

## AI/ML (ai-lead owns) — COMPLETE
- [x] Query auto-tuning: analyze slow queries and suggest index/partition changes — sde (67263ce)
- [x] NL-to-SQL improvements: support multi-table JOINs in natural language — sde (98ee7c3)
- [x] Query similarity detection: find duplicate/similar queries across tenants — sde (1cb5cb4)
- [x] Smart query routing: route to fastest connector based on historical latency — pm (fe65f0d) + ai-lead (73302a6)
- [x] Anomaly detection improvements: seasonal patterns, trend detection — sde (ace3506) + ai-lead (449ee84)

## Ecosystem (sde owns) — COMPLETE
- [x] GraphQL subscriptions: real-time query result streaming — sde (e934726)
- [x] SDK async support: async/await for Python, TypeScript, Go SDKs — sde (cff1cba, cea4e99)
- [x] Webhook retry logic: exponential backoff, dead letter queue — sde (d352a33)
- [x] CDC improvements: multi-table materialized views — sde (b216e31)
- [x] REST API versioning: /v1/ and /v2/ prefix support — sde (0255e95)

## Data Layer (dba owns) — COMPLETE
- [x] Query result compression: gzip/zstd for large result sets — pm (77ac512)
- [x] Connection pooling stats: expose pool utilization per connector — pm (8c816b3) + sde (54fe925)
- [x] Materialized view refresh optimization: incremental refresh — already existed + sde (76b6653)
- [x] Query plan visualization: tree/graph format for EXPLAIN output — pm (b4bf4af)
- [x] Connector health history: track uptime/latency over time — pm (7879a0b) + sde (54fe925)

## Security (security owns) — COMPLETE
- [x] Audit logging: log all queries with tenant, timestamp, duration — security (9c1efe3)
- [x] Column-level RBAC: restrict access to sensitive columns per role — security (da4d9e6)
- [x] API key rotation: automated key rotation with grace period — security (8851de5)
- [x] Request signing: HMAC-based request authentication — security (942c93a)
- [x] Security headers: CSP, HSTS, X-Frame-Options for playground — already done (275ba63)

## Infrastructure (devops owns) — COMPLETE
- [x] Blue-green deployment support in Helm — devops (d360eb5)
- [x] Prometheus ServiceMonitor for auto-discovery — devops (97fc134)
- [x] Init container for config validation — devops (0fd73dd)
- [x] Resource quotas per namespace — devops (df3a01b)
- [x] Backup/restore for query history and saved queries — devops (eba5264)

## Test (test owns) — COMPLETE
- [x] Chaos testing: random connector failures during queries — pm (e7e4ff9) + test (d2da15d)
- [x] Performance regression detection: compare benchmark runs — test (5ca7a6e)
- [x] Fuzz testing: random SQL/PPL input generation — test (b3b1e40)
- [x] Contract testing: verify API response schemas — test (ff3d099)
- [x] Load test scenarios: spike, soak, stress patterns — test (3d6cbb5)

## Frontend (fee owns) — COMPLETE
- [x] Query diff viewer: side-by-side result comparison — fee (d5bb3f6)
- [x] Dashboard builder: drag-and-drop query result widgets — already existed
- [x] Export results: CSV, JSON, Parquet download buttons — existed + fee (6c2b511)
- [x] Query history search: filter by date, status, datasource — already existed
- [x] Mobile responsive: playground works on tablets — fee (b61f196)

## Bonus Work (not in original roadmap)
- [x] 5 security vulnerabilities found and fixed (SSRF, webhook auth, GraphQL DoS, RBAC bypass, schema caching)
- [x] CSV formula injection fix — security (1da32dd)
- [x] CDC auth bypass fix — security (f30e647)
- [x] 50+ integration/E2E tests added
- [x] Endpoint coverage test (36 endpoints) — test (1e07ebf)
- [x] Query history search backend — sde
- [x] Query diff viewer backend — sde
- [x] Build conflict resolution — pm (6e82f36, 5ca0117)
