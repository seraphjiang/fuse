-- Demo #313: PPL lookup across datasources (log enrichment)
-- Shows the PPL lookup command (#235) for cross-datasource enrichment.
--
-- Prerequisites:
--   - cluster_a (OpenSearch): application_logs with user_id field
--   - dynamodb (DynamoDB): user_profiles table (depends: #300)
--   - s3_o11y (S3): access_logs with user_id field
--
-- Scenario: Enrich log events with user profile data.

-- PPL: Enrich OpenSearch logs with user profiles
-- source = cluster_a.application_logs
--   | where status >= 400
--   | lookup dynamodb.user_profiles ON user_id
--   | fields timestamp, user_id, name, email, status, message

-- PPL: Enrich S3 access logs with user profiles
-- source = s3_o11y.access_logs
--   | lookup dynamodb.user_profiles ON user_id
--   | stats count() AS requests BY name
--   | sort requests DESC
--   | head 10

-- PPL: Multi-step enrichment pipeline
-- source = cluster_a.application_logs
--   | where status >= 500
--   | lookup dynamodb.user_profiles ON user_id
--   | eval error_class = CASE WHEN status >= 500 THEN 'server' ELSE 'client' END
--   | stats count() AS errors BY name, error_class
--   | sort errors DESC

-- SQL equivalent: JOIN for user enrichment
SELECT a.timestamp, a.user_id, u.name, u.email, a.status, a.message
FROM cluster_a.application_logs a
JOIN dynamodb.user_profiles u ON a.user_id = u.user_id
WHERE a.status >= 400
ORDER BY a.timestamp DESC
LIMIT 20;
