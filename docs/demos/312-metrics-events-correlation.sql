-- Demo #312: Correlate Prometheus metrics with OpenSearch error events
-- Shows cross-type federation: time-series metrics + log events.
--
-- Prerequisites:
--   - cluster_a (OpenSearch): application_logs index
--   - prometheus (Prometheus): http_requests_total, error_rate metrics
--
-- Scenario: CPU spike at 14:00 — correlate with error logs.

-- 1. JOIN: Enrich error logs with metric values at same timestamp
SELECT a.timestamp, a.host, a.status, a.message, p.value AS cpu_usage
FROM cluster_a.application_logs a
JOIN prometheus.node_cpu p ON a.host = p.instance
WHERE a.status >= 500;

-- 2. UNION ALL: Unified timeline of metrics + events
SELECT timestamp, host, 'log' AS source_type, message AS detail
FROM cluster_a.application_logs
WHERE status >= 400
UNION ALL
SELECT timestamp, instance AS host, 'metric' AS source_type,
  CONCAT('cpu=', value) AS detail
FROM prometheus.node_cpu
WHERE value > 80
ORDER BY timestamp DESC
LIMIT 50;

-- 3. PPL: Same correlation using PPL syntax
-- source = cluster_a.application_logs
--   | where status >= 500
--   | lookup prometheus.node_cpu ON host
--   | fields timestamp, host, status, message, value
