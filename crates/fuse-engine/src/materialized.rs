// SPDX-License-Identifier: Apache-2.0

//! Materialized views — pre-computed query results that are periodically refreshed.
//!
//! A materialized view stores the results of a SQL or PPL query as cached
//! RecordBatches. On read, the cached data is returned directly. A background
//! refresh re-executes the query at a configurable interval.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;

/// Definition of a materialized view.
#[derive(Debug, Clone)]
pub struct MaterializedViewDef {
    /// Unique name used to reference this view (e.g. `error_summary`).
    pub name: String,
    /// The query to execute (SQL or PPL).
    pub query: String,
    /// How often to refresh the cached results.
    pub refresh_interval: Duration,
}

/// A materialized view with its cached state.
#[derive(Debug, Clone)]
pub struct MaterializedView {
    pub def: MaterializedViewDef,
    pub batches: Vec<RecordBatch>,
    pub last_refresh: Option<Instant>,
    pub error: Option<String>,
}

impl MaterializedView {
    pub fn new(def: MaterializedViewDef) -> Self {
        Self {
            def,
            batches: vec![],
            last_refresh: None,
            error: None,
        }
    }

    /// Whether the cached data is stale and needs refresh.
    pub fn needs_refresh(&self) -> bool {
        match self.last_refresh {
            None => true,
            Some(t) => t.elapsed() > self.def.refresh_interval,
        }
    }

    /// Update the cached results after a successful refresh.
    pub fn set_results(&mut self, batches: Vec<RecordBatch>) {
        self.batches = batches;
        self.last_refresh = Some(Instant::now());
        self.error = None;
    }

    /// Record a refresh failure.
    pub fn set_error(&mut self, msg: String) {
        self.error = Some(msg);
        // Keep stale data — better than nothing
    }
}

/// Registry of materialized views.
#[derive(Debug)]
pub struct MaterializedViewRegistry {
    views: RwLock<HashMap<String, Arc<RwLock<MaterializedView>>>>,
}

impl MaterializedViewRegistry {
    pub fn new() -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new materialized view. Replaces any existing view with the same name.
    pub fn register(&self, def: MaterializedViewDef) {
        let name = def.name.clone();
        let view = Arc::new(RwLock::new(MaterializedView::new(def)));
        self.views.write().unwrap().insert(name, view);
    }

    /// Remove a materialized view by name. Returns true if it existed.
    pub fn remove(&self, name: &str) -> bool {
        self.views.write().unwrap().remove(name).is_some()
    }

    /// Get a view by name.
    pub fn get(&self, name: &str) -> Option<Arc<RwLock<MaterializedView>>> {
        self.views.read().unwrap().get(name).cloned()
    }

    /// List all view names.
    pub fn list(&self) -> Vec<String> {
        self.views.read().unwrap().keys().cloned().collect()
    }

    /// Get cached results for a view. Returns None if the view doesn't exist.
    pub fn get_results(&self, name: &str) -> Option<Vec<RecordBatch>> {
        let view_arc = self.get(name)?;
        let view = view_arc.read().unwrap();
        if view.batches.is_empty() && view.last_refresh.is_none() {
            None // Never refreshed
        } else {
            Some(view.batches.clone())
        }
    }

    /// Return names of views that need refresh.
    pub fn stale_views(&self) -> Vec<String> {
        self.views
            .read()
            .unwrap()
            .iter()
            .filter(|(_, v)| v.read().unwrap().needs_refresh())
            .map(|(name, _)| name.clone())
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
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_def(name: &str) -> MaterializedViewDef {
        MaterializedViewDef {
            name: name.into(),
            query: "SELECT count(*) FROM logs".into(),
            refresh_interval: Duration::from_secs(60),
        }
    }

    fn test_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("cnt", DataType::Int64, false)]));
        vec![RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap()]
    }

    #[test]
    fn test_new_view_needs_refresh() {
        let view = MaterializedView::new(test_def("v1"));
        assert!(view.needs_refresh());
        assert!(view.batches.is_empty());
        assert!(view.last_refresh.is_none());
        assert!(view.error.is_none());
    }

    #[test]
    fn test_set_results_clears_stale() {
        let mut view = MaterializedView::new(test_def("v1"));
        view.set_results(test_batches());
        assert!(!view.needs_refresh());
        assert_eq!(view.batches.len(), 1);
        assert!(view.last_refresh.is_some());
        assert!(view.error.is_none());
    }

    #[test]
    fn test_set_error_keeps_stale_data() {
        let mut view = MaterializedView::new(test_def("v1"));
        view.set_results(test_batches());
        view.set_error("connection timeout".into());
        assert_eq!(view.batches.len(), 1); // stale data preserved
        assert_eq!(view.error.as_deref(), Some("connection timeout"));
    }

    #[test]
    fn test_needs_refresh_after_interval() {
        let def = MaterializedViewDef {
            refresh_interval: Duration::from_millis(0),
            ..test_def("v1")
        };
        let mut view = MaterializedView::new(def);
        view.set_results(test_batches());
        std::thread::sleep(Duration::from_millis(1));
        assert!(view.needs_refresh());
    }

    #[test]
    fn test_registry_register_and_get() {
        let reg = MaterializedViewRegistry::new();
        reg.register(test_def("v1"));
        assert!(reg.get("v1").is_some());
        assert!(reg.get("nonexistent").is_none());
    }

    #[test]
    fn test_registry_remove() {
        let reg = MaterializedViewRegistry::new();
        reg.register(test_def("v1"));
        assert!(reg.remove("v1"));
        assert!(!reg.remove("v1")); // already removed
        assert!(reg.get("v1").is_none());
    }

    #[test]
    fn test_registry_list() {
        let reg = MaterializedViewRegistry::new();
        reg.register(test_def("a"));
        reg.register(test_def("b"));
        let mut names = reg.list();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn test_registry_get_results_none_before_refresh() {
        let reg = MaterializedViewRegistry::new();
        reg.register(test_def("v1"));
        assert!(reg.get_results("v1").is_none());
    }

    #[test]
    fn test_registry_get_results_after_refresh() {
        let reg = MaterializedViewRegistry::new();
        reg.register(test_def("v1"));
        {
            let view_arc = reg.get("v1").unwrap();
            let mut view = view_arc.write().unwrap();
            view.set_results(test_batches());
        }
        let results = reg.get_results("v1").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[test]
    fn test_registry_stale_views() {
        let reg = MaterializedViewRegistry::new();
        reg.register(test_def("fresh"));
        reg.register(test_def("stale"));

        // Refresh "fresh" only
        {
            let view_arc = reg.get("fresh").unwrap();
            let mut view = view_arc.write().unwrap();
            view.set_results(test_batches());
        }

        let stale = reg.stale_views();
        assert_eq!(stale, vec!["stale"]);
    }

    #[test]
    fn test_registry_replace_existing() {
        let reg = MaterializedViewRegistry::new();
        reg.register(MaterializedViewDef {
            query: "SELECT 1".into(),
            ..test_def("v1")
        });
        reg.register(MaterializedViewDef {
            query: "SELECT 2".into(),
            ..test_def("v1")
        });
        let view_arc = reg.get("v1").unwrap();
        let view = view_arc.read().unwrap();
        assert_eq!(view.def.query, "SELECT 2");
    }

    #[test]
    fn test_registry_get_results_nonexistent() {
        let reg = MaterializedViewRegistry::new();
        assert!(reg.get_results("nope").is_none());
    }
}
