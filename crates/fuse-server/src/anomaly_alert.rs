// SPDX-License-Identifier: Apache-2.0

//! Anomaly-based alerting — bridges anomaly detection into the alert system.
//!
//! Monitors query results over time, maintains rolling baselines, and
//! fires alerts when anomalies are detected (spikes, drops, null rate
//! changes, cardinality shifts).

use std::collections::HashMap;
use std::sync::Mutex;

use serde::Serialize;

use crate::anomaly::{self, Anomaly, AnomalySeverity, ColumnBaseline, CurrentSnapshot};

/// An anomaly-based alert rule.
#[derive(Debug, Clone, Serialize)]
pub struct AnomalyAlertRule {
    pub id: String,
    pub name: String,
    pub query: String,
    pub column: String,
    pub min_severity: AnomalySeverity,
    pub enabled: bool,
}

/// Fired anomaly alert.
#[derive(Debug, Clone, Serialize)]
pub struct AnomalyAlert {
    pub rule_id: String,
    pub rule_name: String,
    pub anomaly: Anomaly,
    pub fired_at: u64,
}

/// Rolling baseline tracker — maintains exponential moving averages.
pub struct BaselineTracker {
    baselines: Mutex<HashMap<String, ColumnBaseline>>,
    alpha: f64, // smoothing factor (0.0–1.0), higher = more weight on recent
}

impl BaselineTracker {
    pub fn new(alpha: f64) -> Self {
        Self {
            baselines: Mutex::new(HashMap::new()),
            alpha: alpha.clamp(0.01, 0.99),
        }
    }

    /// Update baseline with new observation using exponential moving average.
    pub fn update(&self, key: &str, snapshot: &CurrentSnapshot) {
        let mut baselines = self.baselines.lock().unwrap();
        let entry = baselines
            .entry(key.to_string())
            .or_insert_with(|| ColumnBaseline {
                column: key.to_string(),
                mean: snapshot.mean,
                stddev: 0.0,
                null_rate: snapshot.null_rate,
                distinct_count: snapshot.distinct_count,
            });

        // EMA update
        entry.mean = self.alpha * snapshot.mean + (1.0 - self.alpha) * entry.mean;
        let diff = (snapshot.mean - entry.mean).abs();
        entry.stddev = self.alpha * diff + (1.0 - self.alpha) * entry.stddev;
        entry.null_rate = self.alpha * snapshot.null_rate + (1.0 - self.alpha) * entry.null_rate;
        entry.distinct_count = snapshot.distinct_count; // use latest
    }

    /// Get current baseline for a key.
    pub fn get(&self, key: &str) -> Option<ColumnBaseline> {
        self.baselines.lock().unwrap().get(key).cloned()
    }

    /// Check a snapshot against baseline, return anomalies.
    pub fn check(&self, key: &str, snapshot: &CurrentSnapshot) -> Vec<Anomaly> {
        if let Some(baseline) = self.get(key) {
            anomaly::detect(snapshot, &baseline)
        } else {
            vec![]
        }
    }

    pub fn baseline_count(&self) -> usize {
        self.baselines.lock().unwrap().len()
    }
}

fn now_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Evaluate anomaly alert rules against current snapshots.
pub fn evaluate_anomaly_rules(
    rules: &[AnomalyAlertRule],
    tracker: &BaselineTracker,
    snapshots: &HashMap<String, CurrentSnapshot>,
) -> Vec<AnomalyAlert> {
    let mut alerts = Vec::new();
    for rule in rules {
        if !rule.enabled {
            continue;
        }
        if let Some(snap) = snapshots.get(&rule.column) {
            let anomalies = tracker.check(&rule.column, snap);
            for a in anomalies {
                if severity_gte(a.severity, rule.min_severity) {
                    alerts.push(AnomalyAlert {
                        rule_id: rule.id.clone(),
                        rule_name: rule.name.clone(),
                        anomaly: a,
                        fired_at: now_epoch(),
                    });
                }
            }
        }
    }
    alerts
}

fn severity_gte(a: AnomalySeverity, min: AnomalySeverity) -> bool {
    let rank = |s: AnomalySeverity| match s {
        AnomalySeverity::Low => 0,
        AnomalySeverity::Medium => 1,
        AnomalySeverity::High => 2,
    };
    rank(a) >= rank(min)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_baseline_tracker_first_observation() {
        let tracker = BaselineTracker::new(0.3);
        let snap = CurrentSnapshot {
            mean: 100.0,
            null_rate: 0.01,
            distinct_count: 50,
        };
        tracker.update("latency", &snap);
        let b = tracker.get("latency").unwrap();
        assert!((b.mean - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_baseline_tracker_ema_update() {
        let tracker = BaselineTracker::new(0.5);
        tracker.update(
            "x",
            &CurrentSnapshot {
                mean: 100.0,
                null_rate: 0.0,
                distinct_count: 10,
            },
        );
        tracker.update(
            "x",
            &CurrentSnapshot {
                mean: 200.0,
                null_rate: 0.0,
                distinct_count: 10,
            },
        );
        let b = tracker.get("x").unwrap();
        // EMA: 0.5 * 200 + 0.5 * 100 = 150
        assert!((b.mean - 150.0).abs() < 1.0);
    }

    #[test]
    fn test_check_no_baseline_returns_empty() {
        let tracker = BaselineTracker::new(0.3);
        let snap = CurrentSnapshot {
            mean: 999.0,
            null_rate: 0.0,
            distinct_count: 1,
        };
        assert!(tracker.check("unknown", &snap).is_empty());
    }

    #[test]
    fn test_check_detects_spike() {
        let tracker = BaselineTracker::new(0.1);
        // Build stable baseline
        for _ in 0..50 {
            tracker.update(
                "lat",
                &CurrentSnapshot {
                    mean: 100.0,
                    null_rate: 0.01,
                    distinct_count: 50,
                },
            );
        }
        // Baseline stddev should be ~0 after stable input, so inject some variance
        for i in 0..50 {
            let mean = 100.0 + (i as f64 % 5.0); // slight variance
            tracker.update(
                "lat",
                &CurrentSnapshot {
                    mean,
                    null_rate: 0.01,
                    distinct_count: 50,
                },
            );
        }
        // Now check with a massive spike — use null rate anomaly which is simpler
        let spike = CurrentSnapshot {
            mean: 100.0,
            null_rate: 0.80,
            distinct_count: 50,
        };
        let anomalies = tracker.check("lat", &spike);
        assert!(!anomalies.is_empty(), "should detect null rate anomaly");
    }

    #[test]
    fn test_evaluate_rules_fires_alert() {
        let tracker = BaselineTracker::new(0.1);
        for _ in 0..50 {
            tracker.update(
                "latency",
                &CurrentSnapshot {
                    mean: 100.0,
                    null_rate: 0.01,
                    distinct_count: 50,
                },
            );
        }
        let rules = vec![AnomalyAlertRule {
            id: "r1".into(),
            name: "Null rate spike".into(),
            query: "SELECT avg(latency) FROM logs".into(),
            column: "latency".into(),
            min_severity: AnomalySeverity::Medium,
            enabled: true,
        }];
        let mut snaps = HashMap::new();
        snaps.insert(
            "latency".into(),
            CurrentSnapshot {
                mean: 100.0,
                null_rate: 0.80,
                distinct_count: 50,
            },
        );
        let alerts = evaluate_anomaly_rules(&rules, &tracker, &snaps);
        assert!(!alerts.is_empty());
        assert_eq!(alerts[0].rule_id, "r1");
    }

    #[test]
    fn test_evaluate_rules_disabled_skipped() {
        let tracker = BaselineTracker::new(0.1);
        let rules = vec![AnomalyAlertRule {
            id: "r1".into(),
            name: "test".into(),
            query: "".into(),
            column: "x".into(),
            min_severity: AnomalySeverity::Low,
            enabled: false,
        }];
        let alerts = evaluate_anomaly_rules(&rules, &tracker, &HashMap::new());
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_severity_filter() {
        assert!(severity_gte(AnomalySeverity::High, AnomalySeverity::Medium));
        assert!(severity_gte(
            AnomalySeverity::Medium,
            AnomalySeverity::Medium
        ));
        assert!(!severity_gte(AnomalySeverity::Low, AnomalySeverity::Medium));
    }
}
