# v2.0: Scheduled Queries, Data Quality & 25 Connectors

*April 12, 2026*

Fuse v2.0 ships 16 major features across scheduling, data quality, governance, AI, and ecosystem — built by an 8-agent team in Sprint 18.

## Highlights

- **Scheduled queries** — cron-based execution with alert-on-change
- **Data quality rules** — null rate, freshness, row count, cardinality checks
- **Arrow IPC format** — zero-copy binary results for Python/Rust consumers
- **Query cost estimation ($)** — real dollar estimates per connector (Athena $/TB, DDB $/RCU, S3 $/request)
- **GraphQL API** — schema introspection, query execution, saved queries
- **Webhook subscriptions** — event-driven notifications on result conditions
- **Query replay** — record, replay, diff for regression testing
- **Adaptive caching** — frequency tracking, per-datasource TTL, auto-promotion
- **Query lineage** — data flow graph across connectors
- **Multi-tenant SaaS mode** — usage metering, rate limiting, tenant isolation
- **OpenTelemetry collector** — ingest OTLP traces/metrics/logs, query with SQL
- **Query compilation** — skip re-parsing for hot patterns
- **25 connectors** — added Spark, Delta Lake, Iceberg

## Playground

The playground is now a full query IDE with 18 pages:

- **4 new pages**: Schedules, Quality, Lineage, Replay
- **Demo tour** — 6-step guided walkthrough with cross-source JOINs
- **Query sharing** — shareable URLs via hash encoding
- **Saved queries** — save/load/delete with the existing API
- **Snippets** — 12 templates including 3-way JOINs, anti-joins, correlated subqueries
- **Line numbers** — gutter with scroll sync
- **Column sorting** — click headers to sort results
- **Run selection** — select text and Ctrl+Enter to run just that portion
- **Keyboard shortcuts** — modal with all shortcuts (press ?)
- **OTel widget** — ingest counts on Status page

## By the Numbers

| Metric | Value |
|--------|-------|
| Connectors | 25 |
| Tests | 2637+ |
| Playground pages | 18 |
| UI regression checks | 137 |
| Sprint 18 items | 16/16 |
| Agents | 8 |

## What's Next

- RBAC role hierarchy (in progress)
- Grafana cost estimation display
- SDK async support
- Kubernetes HPA autoscaling
