// SPDX-License-Identifier: Apache-2.0
//! #1601 — Cross-datasource JOIN integration tests.
//! Run with: FUSE_LIVE_URL=http://localhost:9400 cargo test -p fuse-server --test cross_join_live_test
//! Skipped if FUSE_LIVE_URL is not set.

use std::collections::HashMap;

fn fuse_url() -> Option<String> {
    std::env::var("FUSE_LIVE_URL").ok()
}

fn post_query(base: &str, sql: &str) -> Result<serde_json::Value, String> {
    let body = serde_json::json!({"query": sql, "format": "sql"});
    let client = reqwest::blocking::Client::new();
    let resp = client.post(format!("{}/api/fuse/query", base))
        .json(&body)
        .send()
        .map_err(|e| e.to_string())?;
    resp.json().map_err(|e| e.to_string())
}

#[test]
fn test_health() {
    let Some(url) = fuse_url() else { return };
    let resp = reqwest::blocking::get(format!("{}/api/fuse/health", url)).unwrap();
    assert!(resp.status().is_success(), "health check failed");
}

#[test]
fn test_cross_join_os_ddb() {
    let Some(url) = fuse_url() else { return };
    let result = post_query(&url,
        "SELECT l.service, u.name FROM cluster_b.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id LIMIT 10"
    ).unwrap();
    assert!(result.get("error").is_none(), "JOIN query returned error: {:?}", result);
    let rows = result["rows"].as_array().unwrap();
    assert!(!rows.is_empty(), "JOIN returned no rows");
}

#[test]
fn test_union_two_sources() {
    let Some(url) = fuse_url() else { return };
    let result = post_query(&url,
        "SELECT service, status FROM cluster_b.application_logs UNION ALL SELECT name AS service, role AS status FROM dynamodb.users LIMIT 20"
    ).unwrap();
    assert!(result.get("error").is_none(), "UNION query returned error: {:?}", result);
}

#[test]
fn test_join_with_group_by() {
    let Some(url) = fuse_url() else { return };
    let result = post_query(&url,
        "SELECT u.role, COUNT(*) as cnt FROM cluster_b.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id GROUP BY u.role"
    ).unwrap();
    assert!(result.get("error").is_none(), "JOIN+GROUP BY returned error: {:?}", result);
}

#[test]
fn test_join_with_window_function() {
    let Some(url) = fuse_url() else { return };
    let result = post_query(&url,
        "SELECT * FROM (SELECT l.service, u.name, ROW_NUMBER() OVER (PARTITION BY l.service ORDER BY l.timestamp DESC) as rn FROM cluster_b.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id) WHERE rn <= 3 LIMIT 15"
    ).unwrap();
    assert!(result.get("error").is_none(), "JOIN+WINDOW returned error: {:?}", result);
}
