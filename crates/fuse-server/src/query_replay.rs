//! Query Replay & Regression Testing (#1812)
//!
//! Record production queries, replay against staging, diff results.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedQuery {
    pub id: String,
    pub query: String,
    pub format: String,
    pub datasources: Vec<String>,
    pub recorded_at: u64,
    pub duration_ms: u64,
    pub row_count: usize,
    pub column_names: Vec<String>,
    pub result_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    pub query_id: String,
    pub original_hash: String,
    pub replay_hash: String,
    pub matched: bool,
    pub original_rows: usize,
    pub replay_rows: usize,
    pub diff: Option<ReplayDiff>,
    pub replay_duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDiff {
    pub added_rows: usize,
    pub removed_rows: usize,
    pub changed_rows: usize,
    pub column_diffs: Vec<ColumnDiff>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDiff {
    pub column: String,
    pub diff_type: ColumnDiffType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnDiffType {
    Added,
    Removed,
    TypeChanged,
    ValuesChanged,
}

pub struct QueryRecorder {
    recordings: Mutex<Vec<RecordedQuery>>,
    max_recordings: usize,
}

impl QueryRecorder {
    pub fn new(max: usize) -> Self {
        Self { recordings: Mutex::new(Vec::new()), max_recordings: max }
    }

    pub fn record(&self, q: RecordedQuery) {
        let mut recs = self.recordings.lock().unwrap();
        if recs.len() >= self.max_recordings {
            recs.remove(0);
        }
        recs.push(q);
    }

    pub fn recordings(&self) -> Vec<RecordedQuery> {
        self.recordings.lock().unwrap().clone()
    }

    pub fn count(&self) -> usize {
        self.recordings.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.recordings.lock().unwrap().clear();
    }

    pub fn find_by_datasource(&self, ds: &str) -> Vec<RecordedQuery> {
        self.recordings.lock().unwrap().iter()
            .filter(|r| r.datasources.iter().any(|d| d == ds))
            .cloned().collect()
    }
}

/// Compute a stable hash for result comparison.
pub fn hash_results(columns: &[String], rows: &[Vec<serde_json::Value>]) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for c in columns { c.hash(&mut hasher); }
    for row in rows {
        for v in row { v.to_string().hash(&mut hasher); }
    }
    format!("{:016x}", hasher.finish())
}

/// Diff two result sets.
pub fn diff_results(
    orig_cols: &[String], orig_rows: &[Vec<serde_json::Value>],
    replay_cols: &[String], replay_rows: &[Vec<serde_json::Value>],
) -> ReplayDiff {
    let col_diffs: Vec<ColumnDiff> = {
        let mut d = Vec::new();
        for c in replay_cols {
            if !orig_cols.contains(c) {
                d.push(ColumnDiff { column: c.clone(), diff_type: ColumnDiffType::Added });
            }
        }
        for c in orig_cols {
            if !replay_cols.contains(c) {
                d.push(ColumnDiff { column: c.clone(), diff_type: ColumnDiffType::Removed });
            }
        }
        d
    };

    let orig_set: std::collections::HashSet<String> = orig_rows.iter().map(|r| format!("{:?}", r)).collect();
    let replay_set: std::collections::HashSet<String> = replay_rows.iter().map(|r| format!("{:?}", r)).collect();

    let added = replay_set.difference(&orig_set).count();
    let removed = orig_set.difference(&replay_set).count();

    ReplayDiff { added_rows: added, removed_rows: removed, changed_rows: 0, column_diffs: col_diffs }
}

/// Summary of a replay session.
pub fn replay_summary(results: &[ReplayResult]) -> HashMap<String, usize> {
    let mut m = HashMap::new();
    m.insert("total".into(), results.len());
    m.insert("matched".into(), results.iter().filter(|r| r.matched).count());
    m.insert("mismatched".into(), results.iter().filter(|r| !r.matched).count());
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(id: &str) -> RecordedQuery {
        RecordedQuery { id: id.into(), query: "SELECT 1".into(), format: "sql".into(),
            datasources: vec!["pg".into()], recorded_at: 0, duration_ms: 10,
            row_count: 1, column_names: vec!["x".into()], result_hash: "abc".into() }
    }

    #[test] fn test_record_and_count() { let r = QueryRecorder::new(10); r.record(rec("q1")); assert_eq!(r.count(), 1); }
    #[test] fn test_max_eviction() { let r = QueryRecorder::new(2); r.record(rec("q1")); r.record(rec("q2")); r.record(rec("q3")); assert_eq!(r.count(), 2); assert_eq!(r.recordings()[0].id, "q2"); }
    #[test] fn test_clear() { let r = QueryRecorder::new(10); r.record(rec("q1")); r.clear(); assert_eq!(r.count(), 0); }
    #[test] fn test_find_by_ds() { let r = QueryRecorder::new(10); r.record(rec("q1")); assert_eq!(r.find_by_datasource("pg").len(), 1); assert_eq!(r.find_by_datasource("es").len(), 0); }
    #[test] fn test_hash_deterministic() { let c = vec!["x".into()]; let r = vec![vec![json!(1)]]; assert_eq!(hash_results(&c, &r), hash_results(&c, &r)); }
    #[test] fn test_hash_different() { let c = vec!["x".into()]; assert_ne!(hash_results(&c, &[vec![json!(1)]]), hash_results(&c, &[vec![json!(2)]])); }
    #[test] fn test_diff_identical() { let c = vec!["x".into()]; let r = vec![vec![json!(1)]]; let d = diff_results(&c, &r, &c, &r); assert_eq!(d.added_rows, 0); assert_eq!(d.removed_rows, 0); }
    #[test] fn test_diff_added_row() { let c = vec!["x".into()]; let d = diff_results(&c, &[vec![json!(1)]], &c, &[vec![json!(1)], vec![json!(2)]]); assert_eq!(d.added_rows, 1); }
    #[test] fn test_diff_removed_col() { let d = diff_results(&["a".into(), "b".into()], &[], &["a".into()], &[]); assert_eq!(d.column_diffs.len(), 1); assert_eq!(d.column_diffs[0].diff_type, ColumnDiffType::Removed); }
    #[test] fn test_replay_summary() { let r = vec![ReplayResult { query_id: "q1".into(), original_hash: "a".into(), replay_hash: "a".into(), matched: true, original_rows: 1, replay_rows: 1, diff: None, replay_duration_ms: 5 }]; let s = replay_summary(&r); assert_eq!(s["matched"], 1); }
}
