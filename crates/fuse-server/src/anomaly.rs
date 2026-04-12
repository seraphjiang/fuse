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
    SeasonalDeviation,
    TrendBreak,
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

// --- Seasonal pattern & trend detection ---

/// A time-series data point for trend/seasonal analysis.
#[derive(Debug, Clone)]
pub struct TimeSeriesPoint {
    pub timestamp: u64,
    pub value: f64,
}

/// Detect seasonal deviations by comparing current value against the same
/// time-of-day/day-of-week historical average.
pub fn detect_seasonal(
    column: &str,
    current_value: f64,
    historical: &[TimeSeriesPoint],
    tolerance_factor: f64,
) -> Vec<Anomaly> {
    if historical.len() < 7 {
        return vec![];
    }
    let mean: f64 = historical.iter().map(|p| p.value).sum::<f64>() / historical.len() as f64;
    let variance: f64 = historical.iter().map(|p| (p.value - mean).powi(2)).sum::<f64>() / historical.len() as f64;
    let stddev = variance.sqrt();
    if stddev < f64::EPSILON {
        return vec![];
    }
    let z = (current_value - mean) / stddev;
    if z.abs() > tolerance_factor {
        vec![Anomaly {
            kind: AnomalyKind::SeasonalDeviation,
            column: column.to_string(),
            message: format!(
                "Seasonal deviation: current {:.2} vs historical mean {:.2} (z={:.1}, window={})",
                current_value, mean, z, historical.len()
            ),
            severity: if z.abs() > 4.0 { AnomalySeverity::High } else { AnomalySeverity::Medium },
        }]
    } else {
        vec![]
    }
}

/// Detect trend breaks using simple linear regression.
/// If the current value deviates significantly from the projected trend, flag it.
pub fn detect_trend(
    column: &str,
    points: &[TimeSeriesPoint],
    current_value: f64,
    tolerance_stddevs: f64,
) -> Vec<Anomaly> {
    if points.len() < 5 {
        return vec![];
    }
    // Simple linear regression: y = slope * x + intercept
    let n = points.len() as f64;
    let xs: Vec<f64> = (0..points.len()).map(|i| i as f64).collect();
    let ys: Vec<f64> = points.iter().map(|p| p.value).collect();
    let x_mean = xs.iter().sum::<f64>() / n;
    let y_mean = ys.iter().sum::<f64>() / n;
    let num: f64 = xs.iter().zip(ys.iter()).map(|(x, y)| (x - x_mean) * (y - y_mean)).sum();
    let den: f64 = xs.iter().map(|x| (x - x_mean).powi(2)).sum();
    if den.abs() < f64::EPSILON {
        return vec![];
    }
    let slope = num / den;
    let intercept = y_mean - slope * x_mean;

    // Projected value at next point
    let projected = slope * n + intercept;

    // Residual standard deviation
    let residuals: Vec<f64> = xs.iter().zip(ys.iter())
        .map(|(x, y)| y - (slope * x + intercept))
        .collect();
    let res_var = residuals.iter().map(|r| r.powi(2)).sum::<f64>() / n;
    let res_std = res_var.sqrt();

    // Perfect trend (zero residuals): any deviation from projected is a break
    if res_std < f64::EPSILON {
        if (current_value - projected).abs() > f64::EPSILON {
            return vec![Anomaly {
                kind: AnomalyKind::TrendBreak,
                column: column.to_string(),
                message: format!(
                    "Trend break: current {:.2} deviates from perfect trend projected {:.2} (slope={:.3})",
                    current_value, projected, slope
                ),
                severity: AnomalySeverity::High,
            }];
        }
        return vec![];
    }

    let deviation = (current_value - projected) / res_std;
    if deviation.abs() > tolerance_stddevs {
        let direction = if deviation > 0.0 { "above" } else { "below" };
        vec![Anomaly {
            kind: AnomalyKind::TrendBreak,
            column: column.to_string(),
            message: format!(
                "Trend break: current {:.2} is {:.1} std devs {} projected {:.2} (slope={:.3})",
                current_value, deviation.abs(), direction, projected, slope
            ),
            severity: if deviation.abs() > 4.0 { AnomalySeverity::High } else { AnomalySeverity::Medium },
        }]
    } else {
        vec![]
    }
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

    #[test]
    fn test_seasonal_deviation_detected() {
        let historical: Vec<TimeSeriesPoint> = (0..20).map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 + (i as f64) * 0.1 }).collect();
        let anomalies = detect_seasonal("latency", 200.0, &historical, 3.0);
        assert!(!anomalies.is_empty());
        assert!(anomalies[0].kind == AnomalyKind::SeasonalDeviation);
    }

    #[test]
    fn test_seasonal_no_deviation() {
        let historical: Vec<TimeSeriesPoint> = (0..20).map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 }).collect();
        let anomalies = detect_seasonal("latency", 100.0, &historical, 3.0);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn test_trend_break_detected() {
        // Linear trend: 100, 110, 120, 130, 140 — then suddenly 300
        let points: Vec<TimeSeriesPoint> = (0..5).map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 + 10.0 * i as f64 }).collect();
        let anomalies = detect_trend("latency", &points, 300.0, 3.0);
        assert!(!anomalies.is_empty());
        assert!(anomalies[0].kind == AnomalyKind::TrendBreak);
    }

    #[test]
    fn test_trend_no_break() {
        let points: Vec<TimeSeriesPoint> = (0..5).map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 + 10.0 * i as f64 }).collect();
        // Next expected ~150
        let anomalies = detect_trend("latency", &points, 150.0, 3.0);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn test_insufficient_data_no_seasonal() {
        let historical: Vec<TimeSeriesPoint> = (0..3).map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 }).collect();
        let anomalies = detect_seasonal("x", 999.0, &historical, 3.0);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn test_seasonal_high_severity() {
        // z > 4.0 should be High severity — need variance in historical data
        let historical: Vec<TimeSeriesPoint> = (0..20).map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 + (i as f64 % 3.0) }).collect();
        let anomalies = detect_seasonal("latency", 500.0, &historical, 2.0);
        assert!(!anomalies.is_empty());
        assert_eq!(anomalies[0].severity, AnomalySeverity::High);
    }

    #[test]
    fn test_trend_below_projected() {
        // Noisy upward trend, then sudden drop
        let points: Vec<TimeSeriesPoint> = (0..10).map(|i| TimeSeriesPoint {
            timestamp: i,
            value: 100.0 + 10.0 * i as f64 + if i % 2 == 0 { 3.0 } else { -3.0 },
        }).collect();
        let anomalies = detect_trend("latency", &points, 10.0, 3.0);
        assert!(!anomalies.is_empty());
        assert_eq!(anomalies[0].kind, AnomalyKind::TrendBreak);
    }

    #[test]
    fn test_multiple_anomaly_types_simultaneously() {
        let baseline = ColumnBaseline { column: "x".into(), mean: 100.0, stddev: 10.0, null_rate: 0.01, distinct_count: 50 };
        // Spike + high null rate + cardinality change all at once
        let snap = CurrentSnapshot { mean: 200.0, null_rate: 0.60, distinct_count: 200 };
        let anomalies = detect(&snap, &baseline);
        let kinds: Vec<_> = anomalies.iter().map(|a| a.kind).collect();
        assert!(kinds.contains(&AnomalyKind::Spike));
        assert!(kinds.contains(&AnomalyKind::HighNullRate));
        assert!(kinds.contains(&AnomalyKind::CardinalityChange));
    }

    #[test]
    fn test_trend_insufficient_data() {
        let points: Vec<TimeSeriesPoint> = (0..3).map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 }).collect();
        let anomalies = detect_trend("x", &points, 999.0, 3.0);
        assert!(anomalies.is_empty());
    }
}
