// SPDX-License-Identifier: Apache-2.0

//! Predictive Query Performance — estimate latency from historical data.
//!
//! Uses query fingerprints to match past executions and compute percentile-based
//! latency predictions. No ML dependencies — pure statistical estimation.

use serde::Serialize;

use crate::fingerprint::fingerprint;
use crate::history::HistoryEntry;

#[derive(Debug, Clone, Serialize)]
pub struct LatencyPrediction {
    pub fingerprint: String,
    pub sample_count: usize,
    pub predicted_ms: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub confidence: PredictionConfidence,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PredictionConfidence {
    High,   // 10+ samples
    Medium, // 3–9 samples
    Low,    // 1–2 samples
    None,   // no history
}

/// Predict latency for a query based on historical executions.
pub fn predict(query: &str, history: &[HistoryEntry]) -> LatencyPrediction {
    let fp = fingerprint(query);

    let mut latencies: Vec<u64> = history
        .iter()
        .filter(|e| e.error.is_none() && fingerprint(&e.query) == fp)
        .map(|e| e.latency_ms)
        .collect();

    if latencies.is_empty() {
        return LatencyPrediction {
            fingerprint: fp,
            sample_count: 0,
            predicted_ms: 0,
            p50_ms: 0,
            p95_ms: 0,
            confidence: PredictionConfidence::None,
        };
    }

    latencies.sort_unstable();
    let n = latencies.len();
    let p50 = latencies[n / 2];
    let p95_idx = ((n as f64 * 0.95).ceil() as usize).min(n - 1);
    let p95 = latencies[p95_idx];

    let confidence = match n {
        0 => PredictionConfidence::None,
        1..=2 => PredictionConfidence::Low,
        3..=9 => PredictionConfidence::Medium,
        _ => PredictionConfidence::High,
    };

    LatencyPrediction {
        fingerprint: fp,
        sample_count: n,
        predicted_ms: p50, // median as primary prediction
        p50_ms: p50,
        p95_ms: p95,
        confidence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(query: &str, latency_ms: u64) -> HistoryEntry {
        HistoryEntry {
            query: query.into(),
            format: "sql".into(),
            timestamp: 0,
            latency_ms,
            row_count: 0,
            error: None,
        }
    }

    fn err_entry(query: &str) -> HistoryEntry {
        HistoryEntry {
            query: query.into(),
            format: "sql".into(),
            timestamp: 0,
            latency_ms: 5000,
            row_count: 0,
            error: Some("timeout".into()),
        }
    }

    #[test]
    fn test_no_history() {
        let p = predict("SELECT * FROM ds.t", &[]);
        assert_eq!(p.confidence, PredictionConfidence::None);
        assert_eq!(p.sample_count, 0);
        assert_eq!(p.predicted_ms, 0);
    }

    #[test]
    fn test_single_sample() {
        let h = vec![entry("SELECT * FROM ds.t WHERE id = 1", 150)];
        let p = predict("SELECT * FROM ds.t WHERE id = 99", &h);
        assert_eq!(p.confidence, PredictionConfidence::Low);
        assert_eq!(p.sample_count, 1);
        assert_eq!(p.predicted_ms, 150);
    }

    #[test]
    fn test_median_prediction() {
        let h = vec![
            entry("SELECT * FROM ds.t WHERE x = 1", 100),
            entry("SELECT * FROM ds.t WHERE x = 2", 200),
            entry("SELECT * FROM ds.t WHERE x = 3", 300),
            entry("SELECT * FROM ds.t WHERE x = 4", 400),
            entry("SELECT * FROM ds.t WHERE x = 5", 500),
        ];
        let p = predict("SELECT * FROM ds.t WHERE x = 99", &h);
        assert_eq!(p.confidence, PredictionConfidence::Medium);
        assert_eq!(p.sample_count, 5);
        assert_eq!(p.predicted_ms, 300); // median
    }

    #[test]
    fn test_high_confidence() {
        let h: Vec<_> = (0..15)
            .map(|i| entry(&format!("SELECT * FROM ds.t WHERE id = {}", i), 100 + i * 10))
            .collect();
        let p = predict("SELECT * FROM ds.t WHERE id = 999", &h);
        assert_eq!(p.confidence, PredictionConfidence::High);
        assert_eq!(p.sample_count, 15);
    }

    #[test]
    fn test_errors_excluded() {
        let h = vec![
            entry("SELECT * FROM ds.t WHERE id = 1", 100),
            err_entry("SELECT * FROM ds.t WHERE id = 2"),
        ];
        let p = predict("SELECT * FROM ds.t WHERE id = 3", &h);
        assert_eq!(p.sample_count, 1); // error entry excluded
    }

    #[test]
    fn test_different_fingerprint_no_match() {
        let h = vec![entry("SELECT * FROM ds.other_table", 500)];
        let p = predict("SELECT * FROM ds.t WHERE id = 1", &h);
        assert_eq!(p.confidence, PredictionConfidence::None);
    }

    #[test]
    fn test_p95_calculation() {
        let h: Vec<_> = (1..=20)
            .map(|i| entry(&format!("SELECT * FROM ds.t WHERE x = {}", i), i * 100))
            .collect();
        let p = predict("SELECT * FROM ds.t WHERE x = 99", &h);
        assert!(p.p95_ms >= 1900); // 95th percentile of 100..2000
        assert!(p.p50_ms <= p.p95_ms);
    }

    #[test]
    fn test_serialization() {
        let p = predict("SELECT 1", &[]);
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"confidence\":\"none\""));
        assert!(json.contains("\"fingerprint\""));
    }
}
