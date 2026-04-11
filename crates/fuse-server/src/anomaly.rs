// SPDX-License-Identifier: Apache-2.0

//! Anomaly detection for query results (#1610).
//!
//! Detects unusual patterns: sudden spikes/drops in numeric values,
//! unexpected null rates, and cardinality changes. Integrates with
//! the alert system to trigger notifications.

use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Anomaly {
    pub kind: AnomalyKind,
    pub column: String,
    pub message: String,
    pub severity: AnomalySeverity,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyKind {
    Spike,
    Drop,
    HighNullRate,
    CardinalityChange,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AnomalySeverity {
    Low,
    Medium,
    High,
}

/// Baseline statistics for a column, computed from historical data.
#[derive(Debug, Clone)]
pub struct ColumnBaseline {
    pub column: String,
    pub mean: f64,
    pub stddev: f64,
    pub null_rate: f64,
    pub distinct_count: u64,
}

/// Detect anomalies by comparing current values against a baseline.
pub fn detect(current: &CurrentSnapshot, baseline: &ColumnBaseline) -> Vec<Anomaly> {
    let mut anomalies = Vec::new();

    // Z-score spike/drop detection
    if baseline.stddev > 0.0 {
        let z = (current.mean - baseline.mean) / baseline.stddev;
        if z > 3.0 {
            anomalies.push(Anomaly {
                kind: AnomalyKind::Spike,
                column: baseline.column.clone(),
                message: format!("Value spike: current mean {:.2} is {:.1} std devs above baseline {:.2}", current.mean, z, baseline.mean),
                severity: if z > 5.0 { AnomalySeverity::High } else { AnomalySeverity::Medium },
            });
        } else if z < -3.0 {
            anomalies.push(Anomaly {
                kind: AnomalyKind::Drop,
                column: baseline.column.clone(),
                message: format!("Value drop: current mean {:.2} is {:.1} std devs below baseline {:.2}", current.mean, z.abs(), baseline.mean),
                severity: if z < -5.0 { AnomalySeverity::High } else { AnomalySeverity::Medium },
            });
        }
    }

    // Null rate anomaly
    if current.null_rate > baseline.null_rate + 0.1 && current.null_rate > 0.05 {
        anomalies.push(Anomaly {
            kind: AnomalyKind::HighNullRate,
            column: baseline.column.clone(),
            message: format!("Null rate increased: {:.1}% → {:.1}%", baseline.null_rate * 100.0, current.null_rate * 100.0),
            severity: if current.null_rate > 0.5 { AnomalySeverity::High } else { AnomalySeverity::Medium },
        });
    }

    // Cardinality change
    if baseline.distinct_count > 0 {
        let ratio = current.distinct_count as f64 / baseline.distinct_count as f64;
        if ratio > 2.0 {
            anomalies.push(Anomaly {
                kind: AnomalyKind::CardinalityChange,
                column: baseline.column.clone(),
                message: format!("Cardinality spike: {} → {} distinct values", baseline.distinct_count, current.distinct_count),
                severity: AnomalySeverity::Low,
            });
        } else if ratio < 0.5 && baseline.distinct_count > 10 {
            anomalies.push(Anomaly {
                kind: AnomalyKind::CardinalityChange,
                column: baseline.column.clone(),
                message: format!("Cardinality drop: {} → {} distinct values", baseline.distinct_count, current.distinct_count),
                severity: AnomalySeverity::Medium,
            });
        }
    }

    anomalies
}

/// Current snapshot of a column's statistics.
#[derive(Debug, Clone)]
pub struct CurrentSnapshot {
    pub mean: f64,
    pub null_rate: f64,
    pub distinct_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline() -> ColumnBaseline {
        ColumnBaseline { column: "latency".into(), mean: 100.0, stddev: 10.0, null_rate: 0.01, distinct_count: 50 }
    }

    #[test]
    fn test_no_anomaly_normal_values() {
        let snap = CurrentSnapshot { mean: 105.0, null_rate: 0.02, distinct_count: 48 };
        assert!(detect(&snap, &baseline()).is_empty());
    }

    #[test]
    fn test_spike_detected() {
        let snap = CurrentSnapshot { mean: 150.0, null_rate: 0.01, distinct_count: 50 };
        let a = detect(&snap, &baseline());
        assert!(a.iter().any(|a| a.kind == AnomalyKind::Spike));
    }

    #[test]
    fn test_drop_detected() {
        let snap = CurrentSnapshot { mean: 50.0, null_rate: 0.01, distinct_count: 50 };
        let a = detect(&snap, &baseline());
        assert!(a.iter().any(|a| a.kind == AnomalyKind::Drop));
    }

    #[test]
    fn test_high_null_rate() {
        let snap = CurrentSnapshot { mean: 100.0, null_rate: 0.30, distinct_count: 50 };
        let a = detect(&snap, &baseline());
        assert!(a.iter().any(|a| a.kind == AnomalyKind::HighNullRate));
    }

    #[test]
    fn test_cardinality_spike() {
        let snap = CurrentSnapshot { mean: 100.0, null_rate: 0.01, distinct_count: 120 };
        let a = detect(&snap, &baseline());
        assert!(a.iter().any(|a| a.kind == AnomalyKind::CardinalityChange));
    }

    #[test]
    fn test_cardinality_drop() {
        let snap = CurrentSnapshot { mean: 100.0, null_rate: 0.01, distinct_count: 20 };
        let a = detect(&snap, &baseline());
        assert!(a.iter().any(|a| a.kind == AnomalyKind::CardinalityChange));
    }

    #[test]
    fn test_severe_spike_is_high() {
        let snap = CurrentSnapshot { mean: 200.0, null_rate: 0.01, distinct_count: 50 }; // z=10
        let a = detect(&snap, &baseline());
        assert!(a.iter().any(|a| a.severity == AnomalySeverity::High));
    }

    #[test]
    fn test_zero_stddev_no_panic() {
        let b = ColumnBaseline { column: "x".into(), mean: 5.0, stddev: 0.0, null_rate: 0.0, distinct_count: 1 };
        let snap = CurrentSnapshot { mean: 100.0, null_rate: 0.0, distinct_count: 1 };
        let a = detect(&snap, &b);
        assert!(a.is_empty()); // can't compute z-score with zero stddev
    }
}
