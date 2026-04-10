# Fuse Demo Video Script v2 — Cross-Datasource Analytics

**Duration:** 3–5 minutes
**Format:** Screen recording of playground + voiceover
**URL:** https://fuse.huanji.profile.aws.dev

---

## Scene 1: Intro + 14 Connectors (0:00–0:30)

**[Screen: Playground landing page]**

> Fuse is a federated query engine. One SQL or PPL query, any combination of datasources, merged results. Today I'll show you cross-datasource JOINs, UNION ALL across three sources, CTEs, trace reconstruction, dashboards, and cursor pagination.

**[Open terminal or playground API panel, run:]**

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources
```

> Fuse supports 14 connector types — OpenSearch, Elasticsearch, PostgreSQL, MySQL, DynamoDB, S3 Parquet, Prometheus, CloudWatch, Redis, CSV/JSON, MongoDB, InfluxDB, and ClickHouse. Here you can see the live datasources registered on this playground, each with their capabilities and latency class.

**[Highlight the JSON response showing connector IDs and types]**

---

## Scene 2: Cross-Datasource JOIN — OpenSearch + DynamoDB (0:30–1:15)

**[Screen: Playground query editor. Type:]**

```sql
SELECT a.timestamp, a.service, a.status, d.name, d.team
FROM cluster_a.application_logs a
JOIN dynamodb.fuse_user_profiles d ON a.user_id = d.user_id
WHERE a.status >= 500
ORDER BY a.timestamp DESC
LIMIT 20
```

**[Click ▶ Run]**

> This query joins OpenSearch application logs with DynamoDB user profiles in a single statement. Fuse fetches both sides in parallel, picks the smaller table as the hash join build side, and merges locally.

**[Point to results: log fields + user name/team columns side by side]**

> Every error log is now enriched with the user's name and team — no ETL pipeline, no data duplication.

**[Check "Analyze" checkbox, re-run]**

> With Analyze enabled, you can see the execution profile. The DynamoDB scan returned 50 rows in 41ms as the build side, OpenSearch returned 200 rows in 89ms as the probe side. Total join time: 12ms.

---

## Scene 3: UNION ALL — 3 Sources with Provenance (1:15–2:00)

**[Type new query:]**

```sql
SELECT timestamp, _datasource, user_id, trace_id, message
FROM cluster_a.application_logs
UNION ALL
SELECT timestamp, _datasource, user_id, trace_id, message
FROM s3_o11y.logs
UNION ALL
SELECT timestamp, _datasource, user_id, trace_id, message
FROM cloudwatch.lambda_logs
ORDER BY timestamp DESC
LIMIT 50
```

**[Click ▶ Run]**

> Three different datasource types — OpenSearch, S3, CloudWatch — unified in one result set. The `_datasource` column is added automatically so you always know where each row came from.

**[Point to provenance bar showing per-source row counts and latency]**

> The provenance bar shows cluster_a returned 20 rows in 45ms, s3_o11y returned 18 rows in 120ms, cloudwatch returned 12 rows in 95ms. All three ran in parallel — total wall time is the slowest source, not the sum.

> If one source fails, the others still return. Fuse reports partial errors without killing the whole query.

---

## Scene 4: CTE — Multi-Step Analysis (2:00–2:40)

**[Type new query:]**

```sql
WITH error_users AS (
    SELECT user_id, COUNT(*) AS error_count
    FROM cluster_a.application_logs
    WHERE status >= 500
    GROUP BY user_id
    HAVING COUNT(*) > 3
)
SELECT d.name, d.team, e.error_count
FROM error_users e
JOIN dynamodb.fuse_user_profiles d ON e.user_id = d.user_id
ORDER BY e.error_count DESC
```

**[Click ▶ Run]**

> CTEs let you build multi-step analysis pipelines. First, we find users with more than 3 errors from OpenSearch. Then we enrich those users with names and teams from DynamoDB. Two datasources, one query, zero intermediate tables.

**[Point to results showing user names with high error counts]**

> This is the kind of analysis that used to require a data warehouse. With Fuse, it's a single query against live data.

---

## Scene 5: Trace Reconstruction (2:40–3:15)

**[Switch to terminal or API panel:]**

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/trace/trace-001
```

**[Show JSON response]**

> The trace reconstruction endpoint takes a trace ID and fans out to every registered datasource. It searches for matching rows, collects them as spans, and returns a timeline sorted by timestamp.

**[Point to response fields]**

> Here we see spans from cluster_a, s3_o11y, and cloudwatch — the full journey of this request across three systems. `datasources_searched` shows all 6 sources were queried, `datasources_matched` shows which 3 had data. Total search time: 187ms.

> This is cross-source distributed tracing without a centralized trace store.

---

## Scene 6: Dashboard with Auto-Visualization (3:15–3:50)

**[Switch to playground dashboard tab or chart view]**

```sql
SELECT service, COUNT(*) AS errors
FROM cluster_a.application_logs
WHERE status >= 500
GROUP BY service
ORDER BY errors DESC
```

**[Click ▶ Run, then select "Bar" chart type]**

> Fuse auto-detects column types and suggests chart types. Categories like service names get bar charts, timestamps get line charts, numbers get gauges.

**[Switch chart type to "Pie", then back to "Bar"]**

> Eight chart types are built in — line, bar, stacked bar, pie, area, scatter, table, and histogram. All rendered client-side with Apache ECharts.

**[Run a time-series query:]**

```sql
SELECT DATE_TRUNC('hour', timestamp) AS hour, COUNT(*) AS requests
FROM cluster_a.application_logs
GROUP BY hour
ORDER BY hour
```

**[Select "Line" chart]**

> Time-series data automatically renders as a line chart. This is live data from OpenSearch — no pre-aggregation needed.

---

## Scene 7: Cursor Pagination (3:50–4:20)

**[Switch to terminal:]**

```bash
curl -s -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "format": "sql", "page_size": 5}' | python3 -m json.tool | head -20
```

**[Show response with next_cursor field]**

> Cursor pagination lets you page through large result sets efficiently. Set `page_size` and you get a `next_cursor` token in the response.

```bash
curl -s -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "format": "sql", "page_size": 5, "cursor": "<next_cursor>"}' | python3 -m json.tool | head -20
```

> Pass the cursor back to get the next page. No offset counting, no skipped rows, works across UNION ALL and JOINs.

---

## Scene 8: Wrap-Up (4:20–4:40)

**[Screen: Playground with results visible]**

> That's Fuse — 14 connectors, cross-datasource JOINs, federated UNION ALL, CTEs, trace reconstruction, auto-visualization, and cursor pagination. All from a single SQL or PPL query.
>
> Try the live playground, read the docs at seraphjiang.github.io/fuse, or check out the source on GitHub.

---

## Production Notes

- Record at 1920x1080, dark theme
- Browser zoom 110% for readability
- Paste queries (don't type live) — pause 1s before clicking Run
- Pause 2s after each Run to let results + charts render
- For terminal scenes, use a large font (16pt+) and pipe through `python3 -m json.tool` for pretty JSON
- Keep cursor movements smooth and intentional
- Total target: 4 minutes (tight editing, no dead air)
