# Fuse Demo Video Script v3 — v0.5.0 Release

**Duration:** 4–5 minutes
**Format:** Screen recording of playground + voiceover
**URL:** https://fuse.huanji.profile.aws.dev

---

## Scene 1: Intro (0:00–0:20)

**[Screen: Playground landing page showing nav tabs: Playground, Dashboard, Explore]**

> Fuse v0.5.0 — a federated query engine with 14 connectors, a full dashboard platform, and production-grade security. One SQL query across OpenSearch, DynamoDB, PostgreSQL, CloudWatch, and more. Let me show you what's new.

---

## Scene 2: 14 Connectors + Explore (0:20–0:55)

**[Click "Explore" tab]**

> The new Explore page lets you browse all connected datasources, their tables, and schemas — no queries needed.

**[Click through: datasource list → select cluster_a → application_logs → show fields]**

> 14 connector types are supported. Here's our OpenSearch cluster with its indices and field types. Let's also check DynamoDB.

**[Select dynamodb → fuse_user_profiles → show fields]**

> Every connector exposes the same schema discovery interface. Now let's query across them.

---

## Scene 3: Cross-Datasource JOIN + Saved View (0:55–1:40)

**[Switch to Playground tab, type:]**

```sql
SELECT l.service, l.status, u.name, u.team
FROM cluster_a.application_logs l
JOIN dynamodb.fuse_user_profiles u ON l.user_id = u.user_id
WHERE l.status >= 500
ORDER BY l.timestamp DESC LIMIT 20
```

**[Click ▶ Run]**

> Cross-datasource JOIN — OpenSearch logs enriched with DynamoDB user profiles. Fuse fetches both in parallel, picks the smaller table as the hash join build side.

**[Click "Save Query" button, name it "error_users"]**

> Save any query as a reusable view. Saved queries are accessible via the API and can be used as building blocks.

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/saved/error_users
```

---

## Scene 4: Recursive CTE — Dependency Chain (1:40–2:15)

**[Type new query:]**

```sql
WITH RECURSIVE call_chain AS (
    SELECT service, trace_id, 1 as depth
    FROM cluster_a.application_logs
    WHERE service = 'api-gateway' AND status >= 500

    UNION ALL

    SELECT l.service, l.trace_id, c.depth + 1
    FROM cluster_a.application_logs l
    JOIN call_chain c ON l.trace_id = c.trace_id
    WHERE l.service != c.service AND c.depth < 5
)
SELECT service, count(*) as error_count, max(depth) as max_depth
FROM call_chain
GROUP BY service
ORDER BY error_count DESC
```

**[Click ▶ Run]**

> Recursive CTEs trace dependency chains. Starting from api-gateway errors, we follow the trace_id to find every downstream service affected — up to 5 hops deep. This is distributed trace analysis in pure SQL.

---

## Scene 5: Anomaly Detection (2:15–2:50)

**[Type new query:]**

```sql
SELECT service,
    count(*) as requests,
    avg(response_time_ms) as avg_ms,
    stddev(response_time_ms) as stddev_ms,
    max(response_time_ms) as max_ms,
    CASE WHEN max(response_time_ms) > avg(response_time_ms) + 3 * stddev(response_time_ms)
         THEN 'ANOMALY' ELSE 'normal' END as status
FROM cluster_a.application_logs
GROUP BY service
ORDER BY avg_ms DESC
```

**[Click ▶ Run, switch to Chart view → Bar chart]**

> Anomaly detection primitives — moving averages, standard deviation, z-scores. Here we're flagging services where the max latency exceeds 3 standard deviations from the mean. The bar chart makes outliers immediately visible.

---

## Scene 6: Dashboard Platform (2:50–3:40)

**[Click "Dashboard" tab]**

**[Click "Templates" → select "Error Analysis"]**

> The dashboard platform has pre-built templates. Error Analysis gives you five panels out of the box — errors by service, status distribution, cross-cluster timeline, top error messages, and a service-by-status heatmap.

**[Point to variable dropdown at top, select a service]**

> Variables filter all panels at once. Select "api-gateway" and every panel updates to show only that service's data.

**[Click a bar in the "Errors by Service" chart]**

> Drill-down — click any chart element to filter the entire dashboard. Click again to clear.

**[Change time range to "6h", enable auto-refresh "30s"]**

> Global time range and auto-refresh keep your dashboard live. The green pulse shows it's actively updating.

**[Click "💾 Save", then "🔗 Share"]**

> Save dashboards locally, share via URL, or export as PNG, CSV, or PDF.

---

## Scene 7: Production Security (3:40–4:10)

**[Switch to terminal:]**

```bash
# Unauthenticated request → 401
curl -s -o /dev/null -w "%{http_code}" https://fuse.huanji.profile.aws.dev/api/fuse/query

# Authenticated request → 200
curl -s -o /dev/null -w "%{http_code}" -H "x-api-key: demo-key" \
  https://fuse.huanji.profile.aws.dev/api/fuse/datasources
```

> v0.5.0 adds API key authentication. Every request needs an `x-api-key` header or Bearer token. Keys have roles — viewer, editor, admin.

```bash
# Rate limiting → 429 after burst
for i in $(seq 1 5); do
  curl -s -o /dev/null -w "%{http_code} " -H "x-api-key: demo-key" \
    https://fuse.huanji.profile.aws.dev/api/fuse/health
done
```

> Rate limiting protects your backend — configurable per-IP and globally. Query timeouts cancel long-running queries automatically.

---

## Scene 8: Trace Reconstruction (4:10–4:30)

**[In terminal:]**

```bash
curl -s -H "x-api-key: demo-key" \
  https://fuse.huanji.profile.aws.dev/api/fuse/trace/trace-001 | python3 -m json.tool
```

> Trace reconstruction fans out to all 14 datasources, finds every span matching a trace ID, and returns a sorted timeline. Three datasources matched in 187ms — full distributed trace without a centralized trace store.

---

## Scene 9: Wrap-Up (4:30–4:50)

**[Screen: Playground with dashboard visible in background]**

> Fuse v0.5.0 — 14 connectors, cross-datasource JOINs, recursive CTEs, anomaly detection, a full dashboard platform, and production-grade security. 980 tests, p99 under 100ms at 100 concurrent queries.
>
> Try the live playground, check the docs at seraphjiang.github.io/fuse, or contribute on GitHub.

---

## Production Notes

- Record at 1920x1080, dark theme, browser zoom 110%
- Paste queries — pause 1s before clicking Run
- Pause 2s after Run to let results + charts render
- Terminal: 16pt+ font, pipe JSON through `python3 -m json.tool`
- For auth demo: use a demo API key that returns real data
- Total target: 4:30 (tight editing, no dead air)
- Thumbnail: split screen of dashboard + terminal with trace JSON
