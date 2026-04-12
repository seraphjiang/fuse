// SPDX-License-Identifier: Apache-2.0
//! Predictive Query Performance — estimate latency before execution.
//!
//! Uses historical query data to predict execution time for new queries
//! by matching datasource patterns and query structure (single vs join vs union).

use std::collections::HashMap;
use std::sync::Arc;

use serde::Serialize;

use crate::history::QueryHistory;

/// Predicted performance for a query.
#[derive(Debug, Clone, Serialize)]
pub struct Prediction {
    /// Estimated latency in milliseconds.
    pub estimated_ms: u64,
    /// Confidence: "high" (10+ similar), "medium" (3-9), "low" (1-2), "none" (0).
    pub confidence: String,
    /// Number of similar past queries used for the estimate.
    pub sample_count: usize,
    /// Breakdown by datasource if available.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub datasource_estimates: Vec<DatasourceEstimate>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatasourceEstimate {
    pub datasource: String,
    pub avg_ms: u64,
    pub p95_ms: u64,
    pub sample_count: usize,
}

/// Query structure category for matching.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum QueryShape {
    SingleSource,
    Join,
    Union,
}

fn classify_query(query: &str) -> QueryShape {
    let lower = query.to_lowercase();
    let stripped = strip_strings(&lower);
    if stripped.contains(" join ") {
        QueryShape::Join
    } else if stripped.contains(" union ") {
        QueryShape::Union
    } else {
        QueryShape::SingleSource
    }
}

/// Extract datasource names from a query (simple pattern: word.word).
fn extract_datasources(query: &str) -> Vec<String> {
    let cleaned = strip_strings(query);
    let mut ds = Vec::new();
    for word in cleaned.split_whitespace() {
        let clean = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '.' && c != '_');
        if let Some(dot) = clean.find('.') {
            let left = &clean[..dot];
            if !left.is_empty() {
                ds.push(left.to_string());
            }
        }
    }
    ds.sort();
    ds.dedup();
    ds
}

/// Strip string literals to avoid false matches on keywords inside strings.
fn strip_strings(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_quote = false;
    let mut quote_char = ' ';
    for c in s.chars() {
        if in_quote {
            if c == quote_char {
                in_quote = false;
            }
        } else if c == '\'' || c == '"' {
            in_quote = true;
            quote_char = c;
        } else {
            out.push(c);
        }
    }
    out
}

/// Predict query performance based on history.
pub fn predict(history: &Arc<QueryHistory>, query: &str) -> Prediction {
    let entries = history.list();
    if entries.is_empty() {
        return Prediction {
            estimated_ms: 0,
            confidence: "none".into(),
            sample_count: 0,
            datasource_estimates: vec![],
        };
    }

    let target_shape = classify_query(query);
    let target_ds = extract_datasources(query);

    // Score each historical entry by similarity
    let mut scored: Vec<(u64, Vec<String>)> = Vec::new(); // (latency, datasources)
    let mut ds_latencies: HashMap<String, Vec<u64>> = HashMap::new();

    for entry in &entries {
        if entry.error.is_some() {
            continue;
        }
        let shape = classify_query(&entry.query);
        let ds = extract_datasources(&entry.query);

        // Must match query shape
        if shape != target_shape {
            continue;
        }

        // Prefer entries with overlapping datasources
        let overlap = target_ds.iter().filter(|d| ds.contains(d)).count();
        if !target_ds.is_empty() && overlap == 0 {
            continue;
        }

        scored.push((entry.latency_ms, ds.clone()));
        for d in &ds {
            ds_latencies.entry(d.clone()).or_default().push(entry.latency_ms);
        }
    }

    if scored.is_empty() {
        // Fall back to global average
        let successful: Vec<u64> = entries
            .iter()
            .filter(|e| e.error.is_none())
            .map(|e| e.latency_ms)
            .collect();
        if successful.is_empty() {
            return Prediction {
                estimated_ms: 0,
                confidence: "none".into(),
                sample_count: 0,
                datasource_estimates: vec![],
            };
        }
        let avg = successful.iter().sum::<u64>() / successful.len() as u64;
        return Prediction {
            estimated_ms: avg,
            confidence: "low".into(),
            sample_count: successful.len(),
            datasource_estimates: vec![],
        };
    }

    let latencies: Vec<u64> = scored.iter().map(|(l, _)| *l).collect();
    let avg = latencies.iter().sum::<u64>() / latencies.len() as u64;
    let confidence = match latencies.len() {
        0 => "none",
        1..=2 => "low",
        3..=9 => "medium",
        _ => "high",
    };

    let datasource_estimates: Vec<DatasourceEstimate> = target_ds
        .iter()
        .filter_map(|ds| {
            let lats = ds_latencies.get(ds)?;
            let avg_ms = lats.iter().sum::<u64>() / lats.len() as u64;
            let mut sorted = lats.clone();
            sorted.sort_unstable();
            let p95_idx = ((sorted.len() as f64) * 0.95).ceil() as usize;
            let p95_ms = sorted[p95_idx.min(sorted.len() - 1)];
            Some(DatasourceEstimate {
                datasource: ds.clone(),
                avg_ms,
                p95_ms,
                sample_count: lats.len(),
            })
        })
        .collect();

    Prediction {
        estimated_ms: avg,
        confidence: confidence.into(),
        sample_count: latencies.len(),
        datasource_estimates,
    }
}

/// GET /api/fuse/predict?query=...
pub async fn predict_handler(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let query = match params.get("query") {
        Some(q) => q,
        None => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({"error": "missing 'query' parameter"})),
            )
                .into_response()
        }
    };
    let prediction = predict(&state.history, query);
    axum::Json(prediction).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{HistoryEntry, QueryHistory};

    fn history_with(entries: Vec<(&str, u64)>) -> Arc<QueryHistory> {
        let h = Arc::new(QueryHistory::new());
        for (q, ms) in entries {
            h.push(HistoryEntry {
                query: q.to_string(),
                format: "sql".into(),
                timestamp: 1000,
                latency_ms: ms,
                row_count: 10,
                error: None,
            });
        }
        h
    }

    #[test]
    fn test_predict_empty_history() {
        let h = Arc::new(QueryHistory::new());
        let p = predict(&h, "SELECT * FROM ds.logs");
        assert_eq!(p.confidence, "none");
        assert_eq!(p.estimated_ms, 0);
    }

    #[test]
    fn test_predict_single_source_match() {
        let h = history_with(vec![
            ("SELECT * FROM ds.logs", 100),
            ("SELECT * FROM ds.logs WHERE x = 1", 120),
            ("SELECT * FROM ds.logs LIMIT 10", 80),
        ]);
        let p = predict(&h, "SELECT * FROM ds.logs WHERE y = 2");
        assert_eq!(p.confidence, "medium");
        assert_eq!(p.sample_count, 3);
        assert_eq!(p.estimated_ms, 100); // (100+120+80)/3
    }

    #[test]
    fn test_predict_join_vs_single() {
        let h = history_with(vec![
            ("SELECT * FROM a.t1 JOIN b.t2 ON a.t1.id = b.t2.id", 500),
            ("SELECT * FROM a.t1", 50),
        ]);
        // Predicting a join should only match the join entry
        let p = predict(&h, "SELECT * FROM a.t1 JOIN b.t2 ON a.t1.x = b.t2.x");
        assert_eq!(p.sample_count, 1);
        assert_eq!(p.estimated_ms, 500);
    }

    #[test]
    fn test_predict_datasource_estimates() {
        let h = history_with(vec![
            ("SELECT * FROM cluster_a.logs", 100),
            ("SELECT * FROM cluster_a.logs", 200),
        ]);
        let p = predict(&h, "SELECT * FROM cluster_a.events");
        assert_eq!(p.datasource_estimates.len(), 1);
        assert_eq!(p.datasource_estimates[0].datasource, "cluster_a");
        assert_eq!(p.datasource_estimates[0].avg_ms, 150);
    }

    #[test]
    fn test_predict_errors_excluded() {
        let h = Arc::new(QueryHistory::new());
        h.push(HistoryEntry {
            query: "SELECT * FROM ds.logs".into(),
            format: "sql".into(),
            timestamp: 1000,
            latency_ms: 5000,
            row_count: 0,
            error: Some("timeout".into()),
        });
        h.push(HistoryEntry {
            query: "SELECT * FROM ds.logs".into(),
            format: "sql".into(),
            timestamp: 1001,
            latency_ms: 100,
            row_count: 10,
            error: None,
        });
        let p = predict(&h, "SELECT * FROM ds.logs");
        assert_eq!(p.sample_count, 1);
        assert_eq!(p.estimated_ms, 100); // error entry excluded
    }

    #[test]
    fn test_classify_query() {
        assert_eq!(classify_query("SELECT * FROM a.t"), QueryShape::SingleSource);
        assert_eq!(classify_query("SELECT * FROM a.t JOIN b.t ON x"), QueryShape::Join);
        assert_eq!(classify_query("SELECT * FROM a.t UNION ALL SELECT * FROM b.t"), QueryShape::Union);
    }

    #[test]
    fn test_extract_datasources() {
        let ds = extract_datasources("SELECT * FROM cluster_a.logs JOIN ddb.users ON x");
        assert_eq!(ds, vec!["cluster_a", "ddb"]);
    }

    #[test]
    fn test_extract_datasources_ignores_urls() {
        let ds = extract_datasources("SELECT * FROM ds.logs WHERE url = 'https://example.com'");
        assert_eq!(ds, vec!["ds"]);
    }

    #[test]
    fn test_high_confidence() {
        let entries: Vec<(&str, u64)> = (0..15)
            .map(|i| ("SELECT * FROM ds.logs", 100 + i * 10))
            .collect();
        let h = history_with(entries);
        let p = predict(&h, "SELECT * FROM ds.logs WHERE x = 1");
        assert_eq!(p.confidence, "high");
        assert!(p.sample_count >= 10);
    }
}
