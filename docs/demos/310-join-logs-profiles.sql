-- Demo #310: JOIN OpenSearch logs + DynamoDB user profiles
-- Enrich application log events with user metadata.
--
-- Prerequisites:
--   - cluster_a (OpenSearch): application_logs index (has user_id field)
--   - dynamodb (DynamoDB): fuse_user_profiles table (user_id, name, email, team)
--
-- All data shares user_id (user-001 to user-050).

-- 1. Basic enrichment: add user name/email to error logs
SELECT a.timestamp, a.user_id, d.name, d.email, a.status, a.message
FROM cluster_a.application_logs a
JOIN dynamodb.fuse_user_profiles d ON a.user_id = d.user_id
WHERE a.status >= 400
ORDER BY a.timestamp DESC
LIMIT 20;

-- 2. Error count per user (with names)
SELECT d.name, d.team, COUNT(*) AS error_count
FROM cluster_a.application_logs a
JOIN dynamodb.fuse_user_profiles d ON a.user_id = d.user_id
WHERE a.status >= 500
GROUP BY d.name, d.team
ORDER BY error_count DESC
LIMIT 10;

-- 3. PPL equivalent: lookup enrichment
-- source = cluster_a.application_logs
--   | where status >= 400
--   | lookup dynamodb.fuse_user_profiles ON user_id
--   | fields timestamp, user_id, name, email, status, message
--   | sort timestamp DESC
--   | head 20

-- 4. Correlated subquery: find logs for users on the "platform" team
SELECT * FROM cluster_a.application_logs
WHERE user_id IN (
    SELECT user_id FROM dynamodb.fuse_user_profiles
    WHERE team = 'platform'
)
ORDER BY timestamp DESC
LIMIT 50;

-- 5. Anti-join: find logs from unknown users (not in profiles)
-- Uses NOT EXISTS semantics via anti-join
SELECT a.timestamp, a.user_id, a.status, a.message
FROM cluster_a.application_logs a
LEFT JOIN dynamodb.fuse_user_profiles d ON a.user_id = d.user_id
WHERE d.user_id IS NULL;
