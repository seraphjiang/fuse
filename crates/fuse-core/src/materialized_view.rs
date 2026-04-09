// SPDX-License-Identifier: Apache-2.0

//! Materialized views — pre-computed cross-datasource aggregations on schedule.
//!
//! A materialized view runs a federated query on a schedule and caches the
//! result. Subsequent queries against the view name are served from cache
//! instead of re-executing the expensive federated query.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

// ── Config ──

/// Materialized view configuration from fuse.toml `[[view]]` sections.
#[derive(Debug, Clone, Deserialize)]
pub struct ViewConfig {
    #[serde(default)]
    pub views: Vec<MaterializedViewDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MaterializedViewDef {
    /// View name — used as the table reference in queries (e.g., `SELECT * FROM view.error_summary`).
    pub name: String,
    /// The federated query to pre-compute.
    pub query: String,
    #[serde(default = "default_format")]
    pub format: String,
    /// Refresh interval in seconds.
    #[serde(default = "default_refresh")]
    pub refresh_secs: u64,
    /// Maximum age before the view is considered stale (seconds). Default: 2x refresh.
    #[serde(default)]
    pub max_age_secs: Option<u64>,
}

fn default_format() -> String { "sql".to_string() }
fn default_refresh() -> u64 { 300 }

// ── View state ──

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ViewStatus {
    /// Never been computed.
    Uninitialized,
    /// Currently being refreshed.
    Refreshing,
    /// Up to date.
    Fresh,
    /// Older than max_age but still usable.
    Stale,
    /// Last refresh failed.
    Error(String),
}

#[derive(Debug)]
struct ViewEntry {
    def: MaterializedViewDef,
    batches: Option<Vec<RecordBatch>>,
    last_refreshed: Option<Instant>,
    status: ViewStatus,
}

impl ViewEntry {
    fn new(def: MaterializedViewDef) -> Self {
        Self {
            def,
            batches: None,
            last_refreshed: None,
            status: ViewStatus::Uninitialized,
        }
    }

    fn is_stale(&self) -> bool {
        match self.last_refreshed {
            None => true,
            Some(t) => {
                let max_age = self.def.max_age_secs
                    .unwrap_or(self.def.refresh_secs * 2);
                t.elapsed() > Duration::from_secs(max_age)
            }
        }
    }

    fn needs_refresh(&self) -> bool {
        match self.last_refreshed {
            None => true,
            Some(t) => t.elapsed() > Duration::from_secs(self.def.refresh_secs),
        }
    }
}

// ── View registry ──

/// Registry of materialized views with their cached results.
pub struct MaterializedViewRegistry {
    views: RwLock<HashMap<String, ViewEntry>>,
}

impl MaterializedViewRegistry {
    pub fn new() -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
        }
    }

    /// Register views from config.
    pub fn register_from_config(&self, config: &ViewConfig) {
        let mut views = self.views.write().unwrap();
        for def in &config.views {
            views.insert(def.name.clone(), ViewEntry::new(def.clone()));
        }
    }

    /// Check if a view exists.
    pub fn has_view(&self, name: &str) -> bool {
        self.views.read().unwrap().contains_key(name)
    }

    /// Get cached batches for a view if fresh enough.
    pub fn get(&self, name: &str) -> Option<Vec<RecordBatch>> {
        let views = self.views.read().unwrap();
        let entry = views.get(name)?;
        if entry.is_stale() || entry.batches.is_none() {
            return None;
        }
        entry.batches.clone()
    }

    /// Get cached batches even if stale (for degraded mode).
    pub fn get_stale_ok(&self, name: &str) -> Option<Vec<RecordBatch>> {
        let views = self.views.read().unwrap();
        views.get(name)?.batches.clone()
    }

    /// Store refreshed results for a view.
    pub fn put(&self, name: &str, batches: Vec<RecordBatch>) {
        let mut views = self.views.write().unwrap();
        if let Some(entry) = views.get_mut(name) {
            entry.batches = Some(batches);
            entry.last_refreshed = Some(Instant::now());
            entry.status = ViewStatus::Fresh;
        }
    }

    /// Mark a view as having failed to refresh.
    pub fn mark_error(&self, name: &str, error: String) {
        let mut views = self.views.write().unwrap();
        if let Some(entry) = views.get_mut(name) {
            entry.status = ViewStatus::Error(error);
        }
    }

    /// Return names of views that need refreshing.
    pub fn views_needing_refresh(&self) -> Vec<(String, MaterializedViewDef)> {
        let views = self.views.read().unwrap();
        views
            .values()
            .filter(|e| e.needs_refresh() && e.status != ViewStatus::Refreshing)
            .map(|e| (e.def.name.clone(), e.def.clone()))
            .collect()
    }

    /// Return status of all views.
    pub fn status_all(&self) -> HashMap<String, ViewStatus> {
        let views = self.views.read().unwrap();
        views
            .iter()
            .map(|(name, entry)| {
                let status = if entry.is_stale() && entry.batches.is_some() {
                    ViewStatus::Stale
                } else {
                    entry.status.clone()
                };
                (name.clone(), status)
            })
            .collect()
    }
}

impl Default for MaterializedViewRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap()
    }

    fn def(name: &str, refresh_secs: u64) -> MaterializedViewDef {
        MaterializedViewDef {
            name: name.into(),
            query: "SELECT 1".into(),
            format: "sql".into(),
            refresh_secs,
            max_age_secs: None,
        }
    }

    #[test]
    fn test_uninitialized_returns_none() {
        let reg = MaterializedViewRegistry::new();
        reg.register_from_config(&ViewConfig { views: vec![def("v1", 60)] });
        assert!(reg.get("v1").is_none());
    }

    #[test]
    fn test_put_and_get() {
        let reg = MaterializedViewRegistry::new();
        reg.register_from_config(&ViewConfig { views: vec![def("v1", 3600)] });
        reg.put("v1", vec![make_batch()]);
        assert!(reg.get("v1").is_some());
    }

    #[test]
    fn test_needs_refresh_when_uninitialized() {
        let reg = MaterializedViewRegistry::new();
        reg.register_from_config(&ViewConfig { views: vec![def("v1", 60)] });
        let pending = reg.views_needing_refresh();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, "v1");
    }

    #[test]
    fn test_no_refresh_needed_after_put() {
        let reg = MaterializedViewRegistry::new();
        reg.register_from_config(&ViewConfig { views: vec![def("v1", 3600)] });
        reg.put("v1", vec![make_batch()]);
        assert!(reg.views_needing_refresh().is_empty());
    }

    #[test]
    fn test_has_view() {
        let reg = MaterializedViewRegistry::new();
        assert!(!reg.has_view("v1"));
        reg.register_from_config(&ViewConfig { views: vec![def("v1", 60)] });
        assert!(reg.has_view("v1"));
    }

    #[test]
    fn test_get_stale_ok_returns_data_even_when_stale() {
        let reg = MaterializedViewRegistry::new();
        reg.register_from_config(&ViewConfig { views: vec![def("v1", 3600)] });
        reg.put("v1", vec![make_batch()]);
        // get_stale_ok should return data regardless of staleness
        assert!(reg.get_stale_ok("v1").is_some());
    }

    #[test]
    fn test_mark_error_and_status_all() {
        let reg = MaterializedViewRegistry::new();
        reg.register_from_config(&ViewConfig { views: vec![def("v1", 60)] });
        reg.mark_error("v1", "connection refused".into());
        let statuses = reg.status_all();
        assert!(statuses.contains_key("v1"));
        let s = &statuses["v1"];
        assert!(s.last_error.as_deref() == Some("connection refused"));
    }

    #[test]
    fn test_status_all_multiple_views() {
        let reg = MaterializedViewRegistry::new();
        reg.register_from_config(&ViewConfig { views: vec![def("a", 60), def("b", 120)] });
        reg.put("a", vec![make_batch()]);
        let statuses = reg.status_all();
        assert_eq!(statuses.len(), 2);
        assert!(statuses["a"].last_refreshed.is_some());
        assert!(statuses["b"].last_refreshed.is_none());
    }
}
