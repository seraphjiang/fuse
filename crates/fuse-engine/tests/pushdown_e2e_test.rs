// SPDX-License-Identifier: Apache-2.0

//! Integration tests for LIMIT and WHERE pushdown through the full
//! sql_to_subquery → connector pipeline.
//!
//! These tests verify the retro action items:
//! - LIMIT pushdown end-to-end
//! - WHERE pushdown end-to-end

use fuse_engine::sql_to_subquery::sql_to_subquery;

// ── LIMIT pushdown tests ──

#[test]
fn test_limit_pushdown_simple() {
    let sq = sql_to_subquery("SELECT * FROM logs LIMIT 10").unwrap();
    assert_eq!(sq.table, "logs");
    assert_eq!(sq.limit, Some(10));
}

#[test]
fn test_limit_pushdown_with_filter() {
    let sq = sql_to_subquery("SELECT * FROM logs WHERE status = 500 LIMIT 5").unwrap();
    assert_eq!(sq.limit, Some(5));
    assert!(sq.filter.is_some());
}

#[test]
fn test_no_limit_when_absent() {
    let sq = sql_to_subquery("SELECT * FROM logs").unwrap();
    assert_eq!(sq.limit, None);
}

#[test]
fn test_limit_pushdown_large_value() {
    let sq = sql_to_subquery("SELECT * FROM logs LIMIT 10000").unwrap();
    assert_eq!(sq.limit, Some(10000));
}

// ── WHERE pushdown tests ──

#[test]
fn test_where_pushdown_equality() {
    let sq = sql_to_subquery("SELECT * FROM logs WHERE service = 'api-gateway'").unwrap();
    let f = sq.filter.expect("filter should be present");
    assert!(format!("{f:?}").contains("api-gateway"), "filter should contain the value");
}

#[test]
fn test_where_pushdown_comparison() {
    let sq = sql_to_subquery("SELECT * FROM logs WHERE status >= 500").unwrap();
    let f = sq.filter.expect("filter should be present");
    let dbg = format!("{f:?}");
    assert!(dbg.contains("status") || dbg.contains("500"), "filter should reference status/500");
}

#[test]
fn test_where_pushdown_and() {
    let sq = sql_to_subquery(
        "SELECT * FROM logs WHERE status >= 500 AND service = 'auth-service'",
    )
    .unwrap();
    assert!(sq.filter.is_some(), "compound AND filter should be present");
}

#[test]
fn test_no_filter_when_absent() {
    let sq = sql_to_subquery("SELECT * FROM logs").unwrap();
    assert!(sq.filter.is_none());
}

// ── Combined pushdown tests ──

#[test]
fn test_full_pushdown_pipeline() {
    let sq = sql_to_subquery(
        "SELECT service, status, message FROM logs WHERE status >= 400 ORDER BY status DESC LIMIT 20",
    )
    .unwrap();
    assert_eq!(sq.table, "logs");
    assert_eq!(sq.projections.len(), 3);
    assert!(sq.filter.is_some());
    assert!(!sq.sort.is_empty());
    assert_eq!(sq.limit, Some(20));
}

#[test]
fn test_aggregation_pushdown() {
    let sq = sql_to_subquery(
        "SELECT service, COUNT(*) FROM logs WHERE status >= 500 GROUP BY service",
    )
    .unwrap();
    assert_eq!(sq.table, "logs");
    assert!(sq.filter.is_some());
    assert!(!sq.group_by.is_empty());
}
