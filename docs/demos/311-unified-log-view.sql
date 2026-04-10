-- Demo #311: Unified log view across OpenSearch + S3 + CloudWatch
-- Shows UNION ALL federation across 3 different datasource types.
--
-- Prerequisites:
--   - cluster_a (OpenSearch): application_logs index
--   - s3_o11y (S3): access_logs table (Parquet)
--   - cloudwatch (CloudWatch): lambda_logs log group
--
-- All sources share: user_id, trace_id, timestamp, message fields.
-- Fuse adds _datasource column to identify origin.

-- Basic unified view: last 100 log entries across all sources
SELECT timestamp, user_id, trace_id, message
FROM cluster_a.application_logs
UNION ALL
SELECT timestamp, user_id, trace_id, message
FROM s3_o11y.access_logs
UNION ALL
SELECT timestamp, user_id, trace_id, message
FROM cloudwatch.lambda_logs
ORDER BY timestamp DESC
LIMIT 100;

-- Trace correlation: follow a single trace across all sources
SELECT timestamp, _datasource, user_id, message
FROM cluster_a.application_logs
UNION ALL
SELECT timestamp, _datasource, user_id, message
FROM s3_o11y.access_logs
UNION ALL
SELECT timestamp, _datasource, user_id, message
FROM cloudwatch.lambda_logs
WHERE trace_id = 'trace-001'
ORDER BY timestamp ASC;

-- Error aggregation: count errors per source
SELECT _datasource, COUNT(*) AS error_count
FROM cluster_a.application_logs
UNION ALL
SELECT _datasource, COUNT(*) AS error_count
FROM s3_o11y.access_logs
UNION ALL
SELECT _datasource, COUNT(*) AS error_count
FROM cloudwatch.lambda_logs
WHERE status >= 400;
