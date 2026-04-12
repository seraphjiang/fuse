// SPDX-License-Identifier: Apache-2.0
//! Federated Materialized Views with CDC (#1852).
//!
//! Track change events from datasources and auto-refresh materialized views
//! whose source data has changed. Connectors emit `ChangeEvent`s; the
//! `CdcTracker` maps them to affected views and marks them for refresh.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

/// A change event from a datasource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub datasource: String,
    pub table: String,
    pub change_type: ChangeType,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
    SchemaChange,
}

/// Tracks which views depend on which datasource.table pairs,
/// and records change events to trigger auto-refresh.
pub struct CdcTracker {
    /// view_name → set of (datasource, table) dependencies
    pub(crate) dependencies: RwLock<HashMap<String, HashSet<(String, String)>>>,
    /// Recent change events (bounded ring buffer)
    events: RwLock<Vec<ChangeEvent>>,
    /// Views that need refresh due to CDC events
    pending_refresh: RwLock<HashSet<String>>,
    max_events: usize,
}

impl CdcTracker {
    pub fn new(max_events: usize) -> Self {
        Self {
            dependencies: RwLock::new(HashMap::new()),
            events: RwLock::new(Vec::new()),
            pending_refresh: RwLock::new(HashSet::new()),
            max_events,
        }
    }

    /// Register a view's datasource dependencies.
    pub fn register_view(&self, view_name: &str, sources: Vec<(String, String)>) {
        let set: HashSet<(String, String)> = sources.into_iter().collect();
        self.dependencies
            .write()
            .unwrap()
            .insert(view_name.to_string(), set);
    }

    /// Remove a view's dependency tracking.
    pub fn unregister_view(&self, view_name: &str) {
        self.dependencies.write().unwrap().remove(view_name);
        self.pending_refresh.write().unwrap().remove(view_name);
    }

    /// Record a change event and mark affected views for refresh.
    pub fn record_change(&self, event: ChangeEvent) -> Vec<String> {
        let source_key = (event.datasource.clone(), event.table.clone());
        let deps = self.dependencies.read().unwrap();
        let affected: Vec<String> = deps
            .iter()
            .filter(|(_, sources)| sources.contains(&source_key))
            .map(|(name, _)| name.clone())
            .collect();

        // Mark affected views for refresh
        {
            let mut pending = self.pending_refresh.write().unwrap();
            for name in &affected {
                pending.insert(name.clone());
            }
        }

        // Store event in ring buffer
        {
            let mut events = self.events.write().unwrap();
            events.push(event);
            if events.len() > self.max_events {
                let drain_count = events.len() - self.max_events;
                events.drain(..drain_count);
            }
        }

        affected
    }

    /// Get and clear views pending refresh.
    pub fn take_pending(&self) -> Vec<String> {
        let mut pending = self.pending_refresh.write().unwrap();
        let views: Vec<String> = pending.drain().collect();
        views
    }

    /// Peek at pending views without clearing.
    pub fn pending_views(&self) -> Vec<String> {
        self.pending_refresh
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    /// Get dependencies for a single view.
    pub fn dependencies_for(&self, view_name: &str) -> Option<Vec<(String, String)>> {
        self.dependencies
            .read()
            .unwrap()
            .get(view_name)
            .map(|s| s.iter().cloned().collect())
    }

    /// Recent change events.
    pub fn recent_events(&self, limit: usize) -> Vec<ChangeEvent> {
        let events = self.events.read().unwrap();
        events.iter().rev().take(limit).cloned().collect()
    }

    /// Stats for the CDC tracker.
    pub fn stats(&self) -> CdcStats {
        CdcStats {
            tracked_views: self.dependencies.read().unwrap().len(),
            pending_refreshes: self.pending_refresh.read().unwrap().len(),
            total_events: self.events.read().unwrap().len(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CdcStats {
    pub tracked_views: usize,
    pub pending_refreshes: usize,
    pub total_events: usize,
}

// REST handlers for CDC.

/// POST /api/fuse/cdc/events — ingest a change event (from connectors or external).
pub async fn ingest_event(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    auth_identity: Option<axum::extract::Extension<crate::auth::AuthIdentity>>,
    axum::Json(event): axum::Json<ChangeEvent>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if let Err(resp) = crate::auth::require_role(
        auth_identity.as_ref().map(|e| &e.0), crate::auth::Role::Editor, auth_identity.is_some(),
    ) { return resp.into_response(); }
    let affected = state.cdc_tracker.record_change(event);
    axum::Json(serde_json::json!({
        "accepted": true,
        "affected_views": affected,
    }))
    .into_response()
}

/// GET /api/fuse/cdc/status — CDC tracker stats and pending views.
pub async fn cdc_status(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let stats = state.cdc_tracker.stats();
    let pending = state.cdc_tracker.pending_views();
    let recent = state.cdc_tracker.recent_events(20);
    axum::Json(serde_json::json!({
        "stats": stats,
        "pending_views": pending,
        "recent_events": recent,
    }))
    .into_response()
}

/// Build CDC routes.
pub fn cdc_routes() -> axum::Router<Arc<crate::api::AppState>> {
    use axum::routing::{delete, get, post};
    axum::Router::new()
        .route("/events", post(ingest_event))
        .route("/events/batch", post(ingest_events_batch))
        .route("/status", get(cdc_status))
        .route("/views", get(list_dependencies).post(register_view))
        .route("/views/{name}", delete(unregister_view_handler))
        .route("/refresh", post(trigger_refresh))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(ds: &str, table: &str) -> ChangeEvent {
        ChangeEvent {
            datasource: ds.into(),
            table: table.into(),
            change_type: ChangeType::Insert,
            timestamp: 1000,
        }
    }

    #[test]
    fn test_register_and_record_change() {
        let tracker = CdcTracker::new(100);
        tracker.register_view(
            "error_summary",
            vec![("cluster_a".into(), "logs".into())],
        );

        let affected = tracker.record_change(make_event("cluster_a", "logs"));
        assert_eq!(affected, vec!["error_summary"]);
        assert_eq!(tracker.pending_views().len(), 1);
    }

    #[test]
    fn test_unrelated_change_no_affect() {
        let tracker = CdcTracker::new(100);
        tracker.register_view(
            "error_summary",
            vec![("cluster_a".into(), "logs".into())],
        );

        let affected = tracker.record_change(make_event("cluster_b", "metrics"));
        assert!(affected.is_empty());
        assert!(tracker.pending_views().is_empty());
    }

    #[test]
    fn test_take_pending_clears() {
        let tracker = CdcTracker::new(100);
        tracker.register_view("v1", vec![("ds".into(), "t".into())]);
        tracker.record_change(make_event("ds", "t"));

        let pending = tracker.take_pending();
        assert_eq!(pending, vec!["v1"]);
        assert!(tracker.take_pending().is_empty());
    }

    #[test]
    fn test_multiple_views_same_source() {
        let tracker = CdcTracker::new(100);
        tracker.register_view("v1", vec![("ds".into(), "t".into())]);
        tracker.register_view("v2", vec![("ds".into(), "t".into())]);

        let mut affected = tracker.record_change(make_event("ds", "t"));
        affected.sort();
        assert_eq!(affected, vec!["v1", "v2"]);
    }

    #[test]
    fn test_unregister_view() {
        let tracker = CdcTracker::new(100);
        tracker.register_view("v1", vec![("ds".into(), "t".into())]);
        tracker.unregister_view("v1");

        let affected = tracker.record_change(make_event("ds", "t"));
        assert!(affected.is_empty());
    }

    #[test]
    fn test_event_ring_buffer() {
        let tracker = CdcTracker::new(3);
        for i in 0..5 {
            tracker.record_change(ChangeEvent {
                datasource: "ds".into(),
                table: format!("t{}", i),
                change_type: ChangeType::Insert,
                timestamp: i as u64,
            });
        }
        let events = tracker.recent_events(10);
        assert_eq!(events.len(), 3);
        // Most recent first
        assert_eq!(events[0].table, "t4");
    }

    #[test]
    fn test_stats() {
        let tracker = CdcTracker::new(100);
        tracker.register_view("v1", vec![("ds".into(), "t".into())]);
        tracker.record_change(make_event("ds", "t"));

        let stats = tracker.stats();
        assert_eq!(stats.tracked_views, 1);
        assert_eq!(stats.pending_refreshes, 1);
        assert_eq!(stats.total_events, 1);
    }

    #[test]
    fn test_view_with_multiple_sources() {
        let tracker = CdcTracker::new(100);
        tracker.register_view(
            "joined_view",
            vec![
                ("ds_a".into(), "logs".into()),
                ("ds_b".into(), "users".into()),
            ],
        );

        // Change to either source triggers refresh
        let a1 = tracker.record_change(make_event("ds_a", "logs"));
        assert_eq!(a1, vec!["joined_view"]);

        tracker.take_pending(); // clear

        let a2 = tracker.record_change(make_event("ds_b", "users"));
        assert_eq!(a2, vec!["joined_view"]);
    }


    #[test]
    fn test_dependencies_for() {
        let tracker = CdcTracker::new(100);
        tracker.register_view(
            "v1",
            vec![("ds_a".into(), "logs".into()), ("ds_b".into(), "users".into())],
        );
        let deps = tracker.dependencies_for("v1").unwrap();
        assert_eq!(deps.len(), 2);
        assert!(tracker.dependencies_for("nonexistent").is_none());
    }

    #[test]
    fn test_batch_changes_affect_multiple_views() {
        let tracker = CdcTracker::new(100);
        tracker.register_view("v1", vec![("ds".into(), "t1".into())]);
        tracker.register_view("v2", vec![("ds".into(), "t2".into())]);
        tracker.register_view("v3", vec![("ds".into(), "t1".into()), ("ds".into(), "t2".into())]);

        // Change t1 affects v1 and v3
        let mut a1 = tracker.record_change(make_event("ds", "t1"));
        a1.sort();
        assert_eq!(a1, vec!["v1", "v3"]);

        // Change t2 affects v2 and v3
        let mut a2 = tracker.record_change(make_event("ds", "t2"));
        a2.sort();
        assert_eq!(a2, vec!["v2", "v3"]);

        // All 3 views pending
        let mut pending = tracker.pending_views();
        pending.sort();
        assert_eq!(pending, vec!["v1", "v2", "v3"]);
    }

    #[test]
    fn test_reregister_view_updates_deps() {
        let tracker = CdcTracker::new(100);
        tracker.register_view("v1", vec![("ds".into(), "old_table".into())]);
        tracker.register_view("v1", vec![("ds".into(), "new_table".into())]);

        let deps = tracker.dependencies_for("v1").unwrap();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0], ("ds".into(), "new_table".into()));

        // Old table no longer triggers
        let affected = tracker.record_change(make_event("ds", "old_table"));
        assert!(affected.is_empty());
    }
    #[test]
    fn test_change_type_serialization() {
        let event = ChangeEvent {
            datasource: "ds".into(),
            table: "t".into(),
            change_type: ChangeType::SchemaChange,
            timestamp: 1000,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("schema_change"));
    }
}

// --- Multi-table CDC improvements ---

/// Batch ingest multiple change events at once.
pub async fn ingest_events_batch(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    auth_identity: Option<axum::extract::Extension<crate::auth::AuthIdentity>>,
    axum::Json(events): axum::Json<Vec<ChangeEvent>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if let Err(resp) = crate::auth::require_role(
        auth_identity.as_ref().map(|e| &e.0), crate::auth::Role::Editor, auth_identity.is_some(),
    ) { return resp.into_response(); }
    let mut all_affected: HashSet<String> = HashSet::new();
    for event in events {
        let affected = state.cdc_tracker.record_change(event);
        all_affected.extend(affected);
    }
    axum::Json(serde_json::json!({
        "accepted": true,
        "affected_views": all_affected.into_iter().collect::<Vec<_>>(),
    }))
    .into_response()
}

/// Register a view's CDC dependencies from its query sources.
#[derive(Deserialize)]
pub struct RegisterViewRequest {
    pub view_name: String,
    /// List of (datasource, table) pairs this view depends on.
    pub sources: Vec<(String, String)>,
}

pub async fn register_view(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    axum::Json(req): axum::Json<RegisterViewRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    state.cdc_tracker.register_view(&req.view_name, req.sources.clone());
    axum::Json(serde_json::json!({
        "registered": true,
        "view": req.view_name,
        "sources": req.sources,
    }))
    .into_response()
}

/// Trigger refresh for all pending views and return which were refreshed.
pub async fn trigger_refresh(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    auth_identity: Option<axum::extract::Extension<crate::auth::AuthIdentity>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if let Err(resp) = crate::auth::require_role(
        auth_identity.as_ref().map(|e| &e.0), crate::auth::Role::Editor, auth_identity.is_some(),
    ) { return resp.into_response(); }
    let pending = state.cdc_tracker.take_pending();
    let mut refreshed = Vec::new();
    for view_name in &pending {
        if let Some(view_arc) = state.view_registry.get(view_name) {
            let mut view = view_arc.write().unwrap();
            view.last_refresh = None;
            refreshed.push(view_name.clone());
        }
    }
    axum::Json(serde_json::json!({
        "refreshed": refreshed,
        "not_found": pending.iter().filter(|v| !refreshed.contains(v)).collect::<Vec<_>>(),
    }))
    .into_response()
}

/// List all tracked view dependencies.
pub async fn list_dependencies(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let deps = state.cdc_tracker.dependencies.read().unwrap();
    let result: HashMap<String, Vec<(String, String)>> = deps
        .iter()
        .map(|(name, sources)| {
            (name.clone(), sources.iter().map(|(d, t)| (d.clone(), t.clone())).collect())
        })
        .collect();
    axum::Json(serde_json::json!(result)).into_response()
}

/// DELETE /api/fuse/cdc/views/{name} — unregister a view's CDC tracking.
async fn unregister_view_handler(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    state.cdc_tracker.unregister_view(&name);
    axum::Json(serde_json::json!({"unregistered": name})).into_response()
}

/// Auto-register CDC dependencies for a materialized view from its query.
pub fn auto_register_view_dependencies(
    cdc_tracker: &CdcTracker,
    view_name: &str,
    query: &str,
    format: &str,
) {
    let refs = match format {
        "ppl" => crate::api::parse_ppl_sources(query),
        _ => crate::api::parse_sql_sources(query),
    };
    if let Ok(sources) = refs {
        let deps: Vec<(String, String)> = sources
            .into_iter()
            .map(|(ds, table)| (ds, table))
            .collect();
        if !deps.is_empty() {
            cdc_tracker.register_view(view_name, deps);
        }
    }
}
