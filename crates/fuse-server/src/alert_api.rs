// SPDX-License-Identifier: Apache-2.0
//! #911 Alert rules management API handlers.
//!
//! CRUD endpoints for alert rules + acknowledge. Wire into router as:
//!   .route("/api/fuse/alert-rules", get(list_rules).post(create_rule))
//!   .route("/api/fuse/alert-rules/:id", delete(delete_rule))
//!   .route("/api/fuse/alert-rules/:id/acknowledge", post(acknowledge_alert))
//!   .route("/api/fuse/alert-rules/active", get(list_active_alerts))
//!   .route("/api/fuse/alert-rules/history", get(list_alert_history))

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;

use crate::alert_monitor::{AlertMonitor, AlertRule};

/// Shared state for alert endpoints.
pub type AlertState = Arc<AlertMonitor>;

/// GET /api/fuse/alert-rules
pub async fn list_rules(State(monitor): State<AlertState>) -> impl IntoResponse {
    Json(serde_json::json!({ "rules": monitor.list_rules() }))
}

/// POST /api/fuse/alert-rules
pub async fn create_rule(
    State(monitor): State<AlertState>,
    Json(rule): Json<AlertRule>,
) -> impl IntoResponse {
    if rule.id.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "rule id is required" }))).into_response();
    }
    // Check for duplicate
    if monitor.list_rules().iter().any(|r| r.id == rule.id) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": format!("rule '{}' already exists", rule.id) }))).into_response();
    }
    monitor.add_rule(rule.clone());
    (StatusCode::CREATED, Json(serde_json::json!({ "created": rule.id }))).into_response()
}

/// DELETE /api/fuse/alert-rules/:id
pub async fn delete_rule(
    State(monitor): State<AlertState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if monitor.remove_rule(&id) {
        Json(serde_json::json!({ "deleted": id }))
    } else {
        Json(serde_json::json!({ "error": format!("rule '{}' not found", id) }))
    }
}

/// POST /api/fuse/alert-rules/:id/acknowledge
pub async fn acknowledge_alert(
    State(monitor): State<AlertState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if monitor.acknowledge(&id) {
        Json(serde_json::json!({ "acknowledged": id }))
    } else {
        Json(serde_json::json!({ "error": format!("no active alert for rule '{}'", id) }))
    }
}

/// GET /api/fuse/alert-rules/active
pub async fn list_active_alerts(State(monitor): State<AlertState>) -> impl IntoResponse {
    Json(serde_json::json!({ "active": monitor.list_active() }))
}

#[derive(Deserialize)]
pub struct HistoryParams {
    #[serde(default = "default_max")]
    pub max: usize,
}
fn default_max() -> usize { 100 }

/// GET /api/fuse/alert-rules/history
pub async fn list_alert_history(
    State(monitor): State<AlertState>,
    Query(params): Query<HistoryParams>,
) -> impl IntoResponse {
    Json(serde_json::json!({ "history": monitor.list_history(params.max) }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn monitor() -> Arc<AlertMonitor> {
        Arc::new(AlertMonitor::new())
    }

    fn rule(id: &str) -> AlertRule {
        AlertRule {
            id: id.into(),
            name: format!("Rule {id}"),
            metric: "latency_p95".into(),
            threshold: 1000.0,
            window_secs: 60,
            webhook_url: None,
            enabled: true,
        }
    }

    #[tokio::test]
    async fn test_create_and_list() {
        let m = monitor();
        let resp = create_rule(State(m.clone()), Json(rule("r1"))).await.into_response();
        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(m.list_rules().len(), 1);
    }

    #[tokio::test]
    async fn test_create_duplicate() {
        let m = monitor();
        m.add_rule(rule("r1"));
        let resp = create_rule(State(m.clone()), Json(rule("r1"))).await.into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_create_empty_id() {
        let m = monitor();
        let mut r = rule("r1");
        r.id = String::new();
        let resp = create_rule(State(m), Json(r)).await.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_delete_existing() {
        let m = monitor();
        m.add_rule(rule("r1"));
        delete_rule(State(m.clone()), Path("r1".into())).await;
        assert!(m.list_rules().is_empty());
    }

    #[tokio::test]
    async fn test_acknowledge_active() {
        let m = monitor();
        m.add_rule(rule("r1"));
        let mut metrics = std::collections::HashMap::new();
        metrics.insert("latency_p95".into(), 2000.0);
        m.evaluate(&metrics);
        acknowledge_alert(State(m.clone()), Path("r1".into())).await;
        assert_eq!(m.list_active()[0].status, crate::alert_monitor::AlertStatus::Acknowledged);
    }
}
