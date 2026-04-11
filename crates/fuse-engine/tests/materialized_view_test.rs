// SPDX-License-Identifier: Apache-2.0
//! #842 Materialized view lifecycle — create, query, refresh, drop.

use std::sync::Arc;
use std::time::Duration;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fuse_engine::materialized::{MaterializedViewDef, MaterializedView, MaterializedViewRegistry};

fn make_batch(val: i64) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::Int64, false)]));
    vec![RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![val])) as ArrayRef]).unwrap()]
}

#[test]
fn test_create_and_list() {
    let reg = MaterializedViewRegistry::new();
    reg.register(MaterializedViewDef { name: "error_summary".into(), query: "SELECT COUNT(*) FROM logs WHERE level='ERROR'".into(), refresh_interval: Duration::from_secs(60) });
    assert_eq!(reg.list(), vec!["error_summary".to_string()]);
}

#[test]
fn test_query_returns_cached_results() {
    let reg = MaterializedViewRegistry::new();
    reg.register(MaterializedViewDef { name: "v1".into(), query: "SELECT 1".into(), refresh_interval: Duration::from_secs(60) });

    // Set cached results
    let view = reg.get("v1").unwrap();
    view.write().unwrap().set_results(make_batch(42));

    let results = reg.get_results("v1").unwrap();
    assert_eq!(results.len(), 1);
    let col = results[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(col.value(0), 42);
}

#[test]
fn test_refresh_updates_results() {
    let reg = MaterializedViewRegistry::new();
    reg.register(MaterializedViewDef { name: "v1".into(), query: "SELECT 1".into(), refresh_interval: Duration::from_secs(60) });

    let view = reg.get("v1").unwrap();
    view.write().unwrap().set_results(make_batch(10));
    assert_eq!(reg.get_results("v1").unwrap()[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 10);

    // Refresh with new data
    view.write().unwrap().set_results(make_batch(99));
    assert_eq!(reg.get_results("v1").unwrap()[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 99);
}

#[test]
fn test_drop_removes_view() {
    let reg = MaterializedViewRegistry::new();
    reg.register(MaterializedViewDef { name: "v1".into(), query: "SELECT 1".into(), refresh_interval: Duration::from_secs(60) });
    assert!(reg.remove("v1"));
    assert!(reg.list().is_empty());
    assert!(reg.get_results("v1").is_none());
}

#[test]
fn test_drop_nonexistent_returns_false() {
    let reg = MaterializedViewRegistry::new();
    assert!(!reg.remove("nope"));
}

#[test]
fn test_needs_refresh_before_first_execution() {
    let reg = MaterializedViewRegistry::new();
    reg.register(MaterializedViewDef { name: "v1".into(), query: "SELECT 1".into(), refresh_interval: Duration::from_secs(0) });
    let view = reg.get("v1").unwrap();
    assert!(view.read().unwrap().needs_refresh(), "should need refresh before first execution");
}

#[test]
fn test_stale_views_detected() {
    let reg = MaterializedViewRegistry::new();
    reg.register(MaterializedViewDef { name: "v1".into(), query: "SELECT 1".into(), refresh_interval: Duration::from_secs(0) });
    let stale = reg.stale_views();
    assert!(stale.contains(&"v1".to_string()), "new view should be stale");
}

#[test]
fn test_error_state_recorded() {
    let reg = MaterializedViewRegistry::new();
    reg.register(MaterializedViewDef { name: "v1".into(), query: "SELECT 1".into(), refresh_interval: Duration::from_secs(60) });
    let view = reg.get("v1").unwrap();
    view.write().unwrap().set_error("connection refused".into());
    let v = view.read().unwrap();
    assert_eq!(v.error.as_deref(), Some("connection refused"));
}
