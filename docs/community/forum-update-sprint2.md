# OpenSearch Community Forum Update — Fuse Sprint 2

**Target:** [OpenSearch Forum — Feature Proposals](https://forum.opensearch.org/c/feature-proposals/)
**In reply to:** Original Fuse RFC thread

---

**Title:** [Update] Fuse Federated Query Engine — Sprint 2 Shipped

**Body:**

Hi everyone — quick update on Fuse, the federated query engine for OpenSearch Dashboards we proposed a few weeks ago.

## What's New

We shipped Sprint 2 with significant additions:

**6 Connectors** — OpenSearch (SigV4/Basic), S3 Parquet, S3 O11y (gzipped NDJSON), Prometheus, CloudWatch Logs, plus a custom connector SDK.

**Data Provenance** — Cross-datasource queries now add a `_datasource` column showing which source produced each row, with per-datasource latency stats in the response metadata.

**Execution Profiling** — `EXPLAIN` shows the query plan without executing. `ANALYZE` runs the query and returns actual timing per node, with pushdown badges showing what was pushed to each connector.

**SQL Enhancements** — DISTINCT, OFFSET, BETWEEN, HAVING, COUNT(DISTINCT), parameterized queries with `$name` binding.

**PPL Enhancements** — `top`, `rare`, `eval`, `rename` commands. Multi-source queries across clusters.

**Production Features** — Query cancellation, per-query timeout, rate limiting, saved query templates, CSV export, graceful shutdown with in-flight query draining.

**OSD Plugin** — Native OpenSearch Dashboards plugin with SQL/PPL toggle, syntax highlighting, sortable/paginated results table with provenance colors.

**Docs Site** — Full documentation at https://seraphjiang.github.io/fuse/ covering SQL/PPL reference, API (21 endpoints), connector guide, troubleshooting.

## Live Playground

Try it: https://fuse.huanji.profile.aws.dev (requires Amazon VPN)

The playground includes embedded docs, execution plan visualization, query history, and a "Feeling Lucky" button with curated cross-datasource examples.

## By the Numbers

- 500+ tests
- 100+ commits
- 21 API endpoints
- 6 connectors
- 11-page docs site

## What's Next

- Visual execution plan component in OSD plugin (pgAdmin-style tree)
- More connector types (DynamoDB, CloudWatch Metrics)
- Query result caching improvements
- Community connector contributions via fuse-connector-sdk

## Get Involved

- **Source:** https://github.com/seraphjiang/fuse
- **Docs:** https://seraphjiang.github.io/fuse/
- **Connector SDK:** Build your own connector with `fuse-connector-sdk`
- **Issues:** Bug reports, feature requests, and connector requests welcome

We'd love feedback on the connector SDK API and the cross-datasource query experience. What datasources would you want to federate with OpenSearch?
