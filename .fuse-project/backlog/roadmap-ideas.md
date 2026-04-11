# Roadmap — Post v1.1.0

All items from the original Sprint 6+ roadmap are **COMPLETE** as of v1.1.0.

## ✅ Completed (Sprints 12-16)

### Production Readiness — ALL DONE
- ✅ Multi-tenancy with per-tenant resource limits
- ✅ Query governor (max rows, time, memory, rate limit)
- ✅ Audit logging with NDJSON export
- ✅ TLS/mTLS for all 22 connectors
- ✅ Health dashboard with timeline
- ✅ Horizontal scaling (stateless server, Redis-backed)
- ✅ CORS, graceful shutdown, config validation

### Advanced Query — ALL DONE
- ✅ Materialized views with refresh
- ✅ Fuse-to-Fuse federation with cost-based routing
- ✅ Prepared statements with parameter binding
- ✅ Query plan visualization (flame graph + DAG)
- ✅ EXPLAIN ANALYZE
- ✅ Async query API (submit/poll)

### Connectors — ALL DONE (22 total)
- ✅ Kafka, Timestream, BigQuery, Snowflake, Cassandra, DuckDB, Arrow Flight, Athena

### AI/ML — ALL DONE
- ✅ NL-to-SQL (LLM-powered)
- ✅ Auto-suggest queries
- ✅ Query optimization advisor (7 rules)
- ✅ Anomaly detection

### Ecosystem — ALL DONE
- ✅ WASM plugin system with sandboxing
- ✅ Python SDK (DataFrame, streaming, saved queries)
- ✅ Go SDK (stdlib-only)
- ✅ TypeScript SDK (saved queries CRUD)
- ✅ Jupyter magic (%fuse / %%fuse)
- ✅ Grafana datasource plugin
- ✅ VS Code extension (inline results)
- ✅ OSD plugin (synced with v1.1 API)

## Future Ideas (v1.2.0+)

### ✅ Completed Post-GA
- Query scheduling registry (cron-based recurring queries)
- Data lineage tracking across federated queries
- WebSocket streaming protocol types
- Result cache invalidation by datasource
- Background connector health monitor
- Request ID middleware (X-Request-Id)
- API versioning header (X-Fuse-Version)
- Rate limit response headers (X-RateLimit-Remaining)
- Slow query detection and logging
- Query complexity scorer
- ConnectorUrl parser (shared URL handling)
- Server info endpoint (GET /api/fuse/info)
- OpenAPI client generation script
- Production docker-compose with Redis
- Dockerfile HEALTHCHECK
- Helm chart updated to v1.1.0
- CI pipeline: Docker build + test summary
- Startup banner with config summary

### Remaining v1.2.0 Ideas
- Apache Spark connector
- Delta Lake / Iceberg table format support
- Multi-region federation with geo-routing
- Role-based access control UI in playground
- WebSocket streaming handler (wire ws_streaming types into router)
- Data lineage visualization in playground
