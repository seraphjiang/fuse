# Fuse Demo Video Script

**Duration:** 2–3 minutes
**Format:** Screen recording of playground + voiceover
**URL:** https://fuse.huanji.profile.aws.dev

---

## Scene 1: Intro (0:00–0:20)

**[Screen: Playground landing page]**

> Fuse is a federated query engine for OpenSearch. It lets you write one SQL or PPL query that runs across multiple clusters and datasource types — OpenSearch, S3, Prometheus — and merges the results automatically.
>
> Let me show you how it works with a live playground.

---

## Scene 2: SQL Cross-Cluster Query (0:20–0:55)

**[Screen: Type query in editor]**

```sql
SELECT service, status, message FROM cluster_a.application_logs
UNION ALL
SELECT service, status, message FROM cluster_b.application_logs
WHERE status >= 500 LIMIT 20
```

**[Click ▶ Run]**

> Here I'm querying two separate OpenSearch Serverless clusters in a single statement. cluster_a has API gateway and auth logs, cluster_b has order and payment logs.
>
> Notice the `_datasource` column — that's data provenance. Each row is tagged with which cluster it came from. The provenance bar below shows row counts and latency per datasource.

**[Highlight provenance bar: "cluster_a (12 rows, 45ms) | cluster_b (8 rows, 67ms)"]**

---

## Scene 3: PPL with Cross-Cluster Aggregation (0:55–1:25)

**[Switch format toggle to PPL, type:]**

```
source = cluster_a.application_logs, cluster_b.application_logs
| where status >= 500
| stats count() as errors by service
| sort - errors
| head 10
```

**[Click ▶ Run]**

> Same data, PPL syntax. Multi-source queries fan out to both clusters, merge, then apply the pipeline. Here we're aggregating error counts by service across both clusters in one shot.
>
> This is the core value — no more running separate queries and merging in a spreadsheet.

---

## Scene 4: Explain + Analyze (1:25–2:00)

**[Check the "Analyze" checkbox, re-run the SQL query from Scene 2]**

> With Analyze enabled, Fuse shows the execution plan with actual timing. Each node in the tree shows the operation, row count, and latency.
>
> Green means fast — under 50 milliseconds. Yellow is moderate. Red means that node is a bottleneck.

**[Point to RemoteScan nodes]**

> These RemoteScan nodes show pushdown badges — filter, projection, limit. That means Fuse pushed those operations down to the connector instead of fetching everything and filtering locally. That's how it stays fast.

**[Click "Explain" button with a different query]**

> You can also Explain without executing — this shows the planned strategy before any data moves.

---

## Scene 5: Health + Schema Discovery (2:00–2:30)

**[Open Docs tab → API section, or use curl in a terminal]**

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/health
```

> The health endpoint shows each connector's status and latency. If a connector goes degraded, Fuse tells you which one and why.

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources/cluster_a/schemas/application_logs/fields
```

> Schema discovery lets you explore what's available — datasources, tables, field names and types — all through the API.

---

## Scene 6: Wrap-Up (2:30–2:50)

**[Screen: Playground with results visible]**

> That's Fuse — federated queries across OpenSearch clusters and beyond, with data provenance, execution profiling, and full pushdown optimization.
>
> Try it yourself at the playground link, check out the docs at seraphjiang.github.io/fuse, or dive into the source on GitHub.

---

## Production Notes

- Record at 1920x1080, dark theme (playground is already dark)
- Use slow, deliberate typing for queries (or paste + pause)
- Zoom browser to 110% so text is readable in video
- Pause 1–2 seconds after each Run to let results render
- Keep cursor movements smooth and intentional
