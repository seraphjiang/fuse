# Grafana Plugin Setup Guide

Use Fuse as a Grafana datasource to build dashboards over federated queries.

## Prerequisites

- Grafana 9.0+ (10.x recommended)
- Fuse server running and accessible from Grafana

## Install the Plugin

```bash
# Build the plugin
cd grafana-plugin/fuse-datasource
npm install
npm run build

# Copy to Grafana plugins directory
cp -r dist/ /var/lib/grafana/plugins/fuse-datasource/
cp plugin.json /var/lib/grafana/plugins/fuse-datasource/

# Allow unsigned plugins (development)
# In grafana.ini or via environment variable:
export GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=fuse-datasource

# Restart Grafana
sudo systemctl restart grafana-server
```

### Docker

```bash
docker run -d -p 3000:3000 \
  -v $(pwd)/grafana-plugin/fuse-datasource/dist:/var/lib/grafana/plugins/fuse-datasource \
  -e GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=fuse-datasource \
  grafana/grafana:latest
```

## Configure the Datasource

1. Open Grafana → **Configuration** → **Data Sources** → **Add data source**
2. Search for **Fuse**
3. Configure:

| Field | Value |
|-------|-------|
| URL | `http://localhost:9400` (or your Fuse server address) |
| API Key | Your Fuse API key (if auth is enabled) |

4. Click **Save & Test** — should show "Data source is working"

## Build a Dashboard

### Panel with SQL Query

1. Create a new dashboard → **Add panel**
2. Select **Fuse** as the datasource
3. Enter a SQL query:

```sql
SELECT DATE_TRUNC('hour', timestamp) as time, count(*) as requests
FROM cluster_a.application_logs
GROUP BY time
ORDER BY time
```

4. Grafana auto-detects the `time` column for the x-axis
5. Select visualization: Time series, Bar chart, Table, etc.

### Cross-Datasource Query

```sql
SELECT l.service, count(*) as errors, avg(l.response_time_ms) as avg_ms
FROM cluster_a.application_logs l
JOIN dynamodb.fuse_user_profiles u ON l.user_id = u.user_id
WHERE l.status >= 500
GROUP BY l.service
ORDER BY errors DESC
```

### UNION ALL with Provenance

```sql
SELECT _datasource, service, count(*) as n
FROM cluster_a.application_logs
UNION ALL
SELECT _datasource, service, count(*) as n
FROM cluster_b.application_logs
GROUP BY _datasource, service
ORDER BY n DESC
```

Use a **Stacked Bar** visualization with `_datasource` as the series field.

### PPL Query

Switch the query editor to PPL mode:

```
source = cluster_a.application_logs
| where status >= 500
| stats count() as errors by service
| sort - errors
```

## Variables

Create Grafana template variables backed by Fuse queries:

1. Dashboard Settings → **Variables** → **New**
2. Type: **Query**, Datasource: **Fuse**
3. Query:
   ```sql
   SELECT DISTINCT service FROM cluster_a.application_logs
   ```
4. Use in panels as `$service`:
   ```sql
   SELECT status, count(*) as n
   FROM cluster_a.application_logs
   WHERE service = '$service'
   GROUP BY status
   ```

## Alerting

The Fuse plugin supports Grafana alerting. Create alert rules on any Fuse query:

1. Edit a panel → **Alert** tab → **Create alert rule**
2. Set condition (e.g., `errors > 100`)
3. Configure notification channel

## Tips

- Always include `ORDER BY time` for time-series panels — Grafana expects sorted data
- Use `LIMIT` to keep dashboards responsive
- For large time ranges, use `DATE_TRUNC` to bucket data
- Cross-datasource JOINs work in Grafana panels just like in the Fuse playground
- Use Grafana's `$__timeFrom` and `$__timeTo` macros if the plugin supports them (check plugin README)

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Plugin not found | Check plugin is in `/var/lib/grafana/plugins/fuse-datasource/` with `plugin.json` |
| "Unsigned plugin" error | Set `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=fuse-datasource` |
| "Data source is not working" | Verify Fuse URL is reachable from Grafana host |
| 401 on Save & Test | Add API key in datasource config |
| Empty panels | Check query returns data in Fuse playground first |
| Time series not rendering | Ensure query has a `time` column with `ORDER BY time` |
