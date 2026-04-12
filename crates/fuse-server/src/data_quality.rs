// SPDX-License-Identifier: Apache-2.0

//! Data quality rules engine (#1801).
//!
//! Define expectations per datasource/table (null rate, cardinality,
//! freshness, row count bounds). Evaluate rules and report violations.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityRule {
    pub id: String,
    pub datasource: String,
    pub table: String,
    pub check: QualityCheck,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QualityCheck {
    NullRate { column: String, max_rate: f64 },
    RowCount { min: Option<u64>, max: Option<u64> },
    Freshness { column: String, max_age_secs: u64 },
    Cardinality { column: String, min: Option<u64>, max: Option<u64> },
    UniqueRate { column: String, min_rate: f64 },
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Violation {
    pub rule_id: String,
    pub check: String,
    pub message: String,
    pub actual: f64,
    pub threshold: f64,
}

/// Observed metrics for evaluation.
#[derive(Debug, Clone, Default)]
pub struct TableMetrics {
    pub row_count: u64,
    pub null_rates: std::collections::HashMap<String, f64>,
    pub cardinalities: std::collections::HashMap<String, u64>,
    pub freshness_secs: std::collections::HashMap<String, u64>,
}

/// Evaluate a rule against observed metrics.
pub fn evaluate(rule: &QualityRule, metrics: &TableMetrics) -> Option<Violation> {
    if !rule.enabled { return None; }
    match &rule.check {
        QualityCheck::NullRate { column, max_rate } => {
            if let Some(&rate) = metrics.null_rates.get(column) {
                if rate > *max_rate {
                    return Some(Violation {
                        rule_id: rule.id.clone(), check: "null_rate".into(),
                        message: format!("{}.{} null rate {:.1}% exceeds {:.1}%", rule.table, column, rate * 100.0, max_rate * 100.0),
                        actual: rate, threshold: *max_rate,
                    });
                }
            }
        }
        QualityCheck::RowCount { min, max } => {
            let count = metrics.row_count as f64;
            if let Some(m) = min { if metrics.row_count < *m {
                return Some(Violation { rule_id: rule.id.clone(), check: "row_count_min".into(),
                    message: format!("{} has {} rows, expected >= {}", rule.table, metrics.row_count, m),
                    actual: count, threshold: *m as f64 });
            }}
            if let Some(m) = max { if metrics.row_count > *m {
                return Some(Violation { rule_id: rule.id.clone(), check: "row_count_max".into(),
                    message: format!("{} has {} rows, expected <= {}", rule.table, metrics.row_count, m),
                    actual: count, threshold: *m as f64 });
            }}
        }
        QualityCheck::Freshness { column, max_age_secs } => {
            if let Some(&age) = metrics.freshness_secs.get(column) {
                if age > *max_age_secs {
                    return Some(Violation { rule_id: rule.id.clone(), check: "freshness".into(),
                        message: format!("{}.{} is {}s stale, max {}s", rule.table, column, age, max_age_secs),
                        actual: age as f64, threshold: *max_age_secs as f64 });
                }
            }
        }
        QualityCheck::Cardinality { column, min, max } => {
            if let Some(&card) = metrics.cardinalities.get(column) {
                if let Some(m) = min { if card < *m {
                    return Some(Violation { rule_id: rule.id.clone(), check: "cardinality_min".into(),
                        message: format!("{}.{} has {} distinct values, expected >= {}", rule.table, column, card, m),
                        actual: card as f64, threshold: *m as f64 });
                }}
                if let Some(m) = max { if card > *m {
                    return Some(Violation { rule_id: rule.id.clone(), check: "cardinality_max".into(),
                        message: format!("{}.{} has {} distinct values, expected <= {}", rule.table, column, card, m),
                        actual: card as f64, threshold: *m as f64 });
                }}
            }
        }
        QualityCheck::UniqueRate { column, min_rate } => {
            if let Some(&card) = metrics.cardinalities.get(column) {
                if metrics.row_count > 0 {
                    let rate = card as f64 / metrics.row_count as f64;
                    if rate < *min_rate {
                        return Some(Violation { rule_id: rule.id.clone(), check: "unique_rate".into(),
                            message: format!("{}.{} unique rate {:.1}%, expected >= {:.1}%", rule.table, column, rate * 100.0, min_rate * 100.0),
                            actual: rate, threshold: *min_rate });
                    }
                }
            }
        }
    }
    None
}

/// Evaluate all rules, return violations.
pub fn evaluate_all(rules: &[QualityRule], metrics: &TableMetrics) -> Vec<Violation> {
    rules.iter().filter_map(|r| evaluate(r, metrics)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(id: &str, check: QualityCheck) -> QualityRule {
        QualityRule { id: id.into(), datasource: "ds".into(), table: "t".into(), check, enabled: true }
    }

    #[test]
    fn test_null_rate_pass() {
        let r = rule("r1", QualityCheck::NullRate { column: "x".into(), max_rate: 0.05 });
        let mut m = TableMetrics::default();
        m.null_rates.insert("x".into(), 0.02);
        assert!(evaluate(&r, &m).is_none());
    }

    #[test]
    fn test_null_rate_fail() {
        let r = rule("r1", QualityCheck::NullRate { column: "x".into(), max_rate: 0.05 });
        let mut m = TableMetrics::default();
        m.null_rates.insert("x".into(), 0.10);
        let v = evaluate(&r, &m).unwrap();
        assert_eq!(v.check, "null_rate");
    }

    #[test]
    fn test_row_count_min_fail() {
        let r = rule("r1", QualityCheck::RowCount { min: Some(100), max: None });
        let m = TableMetrics { row_count: 50, ..Default::default() };
        assert!(evaluate(&r, &m).is_some());
    }

    #[test]
    fn test_row_count_max_fail() {
        let r = rule("r1", QualityCheck::RowCount { min: None, max: Some(1000) });
        let m = TableMetrics { row_count: 5000, ..Default::default() };
        assert!(evaluate(&r, &m).is_some());
    }

    #[test]
    fn test_row_count_pass() {
        let r = rule("r1", QualityCheck::RowCount { min: Some(10), max: Some(1000) });
        let m = TableMetrics { row_count: 500, ..Default::default() };
        assert!(evaluate(&r, &m).is_none());
    }

    #[test]
    fn test_freshness_fail() {
        let r = rule("r1", QualityCheck::Freshness { column: "ts".into(), max_age_secs: 3600 });
        let mut m = TableMetrics::default();
        m.freshness_secs.insert("ts".into(), 7200);
        assert!(evaluate(&r, &m).is_some());
    }

    #[test]
    fn test_freshness_pass() {
        let r = rule("r1", QualityCheck::Freshness { column: "ts".into(), max_age_secs: 3600 });
        let mut m = TableMetrics::default();
        m.freshness_secs.insert("ts".into(), 1800);
        assert!(evaluate(&r, &m).is_none());
    }

    #[test]
    fn test_cardinality_fail() {
        let r = rule("r1", QualityCheck::Cardinality { column: "status".into(), min: None, max: Some(10) });
        let mut m = TableMetrics::default();
        m.cardinalities.insert("status".into(), 50);
        assert!(evaluate(&r, &m).is_some());
    }

    #[test]
    fn test_unique_rate_fail() {
        let r = rule("r1", QualityCheck::UniqueRate { column: "id".into(), min_rate: 0.95 });
        let mut m = TableMetrics { row_count: 100, ..Default::default() };
        m.cardinalities.insert("id".into(), 80); // 80% unique
        assert!(evaluate(&r, &m).is_some());
    }

    #[test]
    fn test_disabled_rule_skipped() {
        let mut r = rule("r1", QualityCheck::RowCount { min: Some(1000), max: None });
        r.enabled = false;
        let m = TableMetrics { row_count: 1, ..Default::default() };
        assert!(evaluate(&r, &m).is_none());
    }

    #[test]
    fn test_evaluate_all() {
        let rules = vec![
            rule("r1", QualityCheck::RowCount { min: Some(100), max: None }),
            rule("r2", QualityCheck::RowCount { min: None, max: Some(1000) }),
        ];
        let m = TableMetrics { row_count: 50, ..Default::default() };
        let violations = evaluate_all(&rules, &m);
        assert_eq!(violations.len(), 1); // only min fails
    }
}
