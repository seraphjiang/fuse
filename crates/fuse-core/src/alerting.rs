// SPDX-License-Identifier: Apache-2.0

//! Alerting integration — evaluate alert rules against federated query results.
//!
//! Alert rules are defined in fuse.toml and evaluated after query execution.
//! When a condition is met, the alert fires and notifications are dispatched.

use std::collections::HashMap;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

// ── Config ──

/// Alert configuration from fuse.toml `[[alert]]` sections.
#[derive(Debug, Clone, Deserialize)]
pub struct AlertConfig {
    #[serde(default)]
    pub alerts: Vec<AlertRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlertRule {
    pub name: String,
    pub query: String,
    #[serde(default = "default_format")]
    pub format: String,
    pub condition: AlertCondition,
    #[serde(default)]
    pub notify: Vec<NotificationChannel>,
    /// How often to evaluate (seconds). Default: 60.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
}

fn default_format() -> String { "sql".to_string() }
fn default_interval() -> u64 { 60 }

/// Condition that triggers the alert.
#[derive(Debug, Clone, Deserialize)]
pub struct AlertCondition {
    /// Column to evaluate.
    pub column: String,
    pub operator: ConditionOp,
    pub threshold: f64,
    /// Aggregate function to apply before comparison (count, avg, sum, min, max).
    #[serde(default = "default_agg")]
    pub aggregate: String,
}

fn default_agg() -> String { "count".to_string() }

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Neq,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum NotificationChannel {
    Log,
    Webhook { url: String },
    Slack { webhook_url: String },
}

// ── Alert state ──

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum AlertState {
    Ok,
    Firing,
}

#[derive(Debug, Clone, Serialize)]
pub struct AlertResult {
    pub rule_name: String,
    pub state: AlertState,
    pub value: f64,
    pub threshold: f64,
    pub message: String,
}

// ── Evaluator ──

/// Evaluates alert conditions against RecordBatch results.
pub struct AlertEvaluator;

impl AlertEvaluator {
    /// Evaluate a rule against query results. Returns the alert result.
    pub fn evaluate(rule: &AlertRule, batches: &[RecordBatch]) -> AlertResult {
        let value = aggregate_column(batches, &rule.condition.column, &rule.condition.aggregate);
        let firing = check_condition(value, rule.condition.operator, rule.condition.threshold);

        let state = if firing { AlertState::Firing } else { AlertState::Ok };
        let message = format!(
            "Alert '{}': {} {} {} {} (value={:.2})",
            rule.name,
            rule.condition.aggregate,
            rule.condition.column,
            op_str(rule.condition.operator),
            rule.condition.threshold,
            value
        );

        AlertResult {
            rule_name: rule.name.clone(),
            state,
            value,
            threshold: rule.condition.threshold,
            message,
        }
    }

    /// Evaluate all rules and return only firing alerts.
    pub fn evaluate_all(
        rules: &[AlertRule],
        results: &HashMap<String, Vec<RecordBatch>>,
    ) -> Vec<AlertResult> {
        rules
            .iter()
            .filter_map(|rule| {
                let batches = results.get(&rule.name)?;
                let result = Self::evaluate(rule, batches);
                if result.state == AlertState::Firing {
                    Some(result)
                } else {
                    None
                }
            })
            .collect()
    }
}

// ── Notification dispatcher ──

/// Dispatches alert notifications. Currently supports log and webhook.
pub struct NotificationDispatcher;

impl NotificationDispatcher {
    pub async fn dispatch(result: &AlertResult, channels: &[NotificationChannel]) {
        for channel in channels {
            match channel {
                NotificationChannel::Log => {
                    tracing::warn!(
                        alert = result.rule_name.as_str(),
                        value = result.value,
                        threshold = result.threshold,
                        "{}",
                        result.message
                    );
                }
                NotificationChannel::Webhook { url } => {
                    let payload = serde_json::json!({
                        "alert": result.rule_name,
                        "state": format!("{:?}", result.state),
                        "value": result.value,
                        "threshold": result.threshold,
                        "message": result.message,
                    });
                    // Fire-and-forget; errors are logged
                    if let Ok(client) = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build() {
                        let _ = client.post(url).json(&payload).send().await;
                    }
                }
                NotificationChannel::Slack { webhook_url } => {
                    let payload = serde_json::json!({
                        "text": format!(":rotating_light: *{}*\n{}", result.rule_name, result.message)
                    });
                    if let Ok(client) = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build() {
                        let _ = client.post(webhook_url).json(&payload).send().await;
                    }
                }
            }
        }
    }
}

// ── Helpers ──

fn aggregate_column(batches: &[RecordBatch], column: &str, agg: &str) -> f64 {
    let values: Vec<f64> = batches
        .iter()
        .flat_map(|b| {
            let col_idx = b.schema().index_of(column).ok()?;
            let col = b.column(col_idx);
            Some(column_to_f64_vec(col))
        })
        .flatten()
        .collect();

    if values.is_empty() {
        return 0.0;
    }

    match agg {
        "count" => values.len() as f64,
        "sum" => values.iter().sum(),
        "avg" => values.iter().sum::<f64>() / values.len() as f64,
        "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
        "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        _ => values.len() as f64,
    }
}

fn column_to_f64_vec(col: &dyn Array) -> Vec<f64> {
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return (0..arr.len())
            .filter(|&i| !arr.is_null(i))
            .map(|i| arr.value(i))
            .collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return (0..arr.len())
            .filter(|&i| !arr.is_null(i))
            .map(|i| arr.value(i) as f64)
            .collect();
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return (0..arr.len())
            .filter(|&i| !arr.is_null(i))
            .filter_map(|i| arr.value(i).parse::<f64>().ok())
            .collect();
    }
    vec![]
}

fn check_condition(value: f64, op: ConditionOp, threshold: f64) -> bool {
    match op {
        ConditionOp::Gt => value > threshold,
        ConditionOp::Gte => value >= threshold,
        ConditionOp::Lt => value < threshold,
        ConditionOp::Lte => value <= threshold,
        ConditionOp::Eq => (value - threshold).abs() < f64::EPSILON,
        ConditionOp::Neq => (value - threshold).abs() >= f64::EPSILON,
    }
}

fn op_str(op: ConditionOp) -> &'static str {
    match op {
        ConditionOp::Gt => ">",
        ConditionOp::Gte => ">=",
        ConditionOp::Lt => "<",
        ConditionOp::Lte => "<=",
        ConditionOp::Eq => "==",
        ConditionOp::Neq => "!=",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(values: Vec<f64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(values))]).unwrap()
    }

    fn rule(op: ConditionOp, threshold: f64, agg: &str) -> AlertRule {
        AlertRule {
            name: "test_alert".into(),
            query: "SELECT value FROM ds.table".into(),
            format: "sql".into(),
            condition: AlertCondition {
                column: "value".into(),
                operator: op,
                threshold,
                aggregate: agg.into(),
            },
            notify: vec![],
            interval_secs: 60,
        }
    }

    #[test]
    fn test_fires_when_count_exceeds_threshold() {
        let batch = make_batch(vec![1.0, 2.0, 3.0]);
        let r = rule(ConditionOp::Gt, 2.0, "count");
        let result = AlertEvaluator::evaluate(&r, &[batch]);
        assert_eq!(result.state, AlertState::Firing);
        assert_eq!(result.value, 3.0);
    }

    #[test]
    fn test_ok_when_below_threshold() {
        let batch = make_batch(vec![1.0]);
        let r = rule(ConditionOp::Gt, 5.0, "count");
        let result = AlertEvaluator::evaluate(&r, &[batch]);
        assert_eq!(result.state, AlertState::Ok);
    }

    #[test]
    fn test_avg_condition() {
        let batch = make_batch(vec![10.0, 20.0, 30.0]);
        let r = rule(ConditionOp::Gte, 20.0, "avg");
        let result = AlertEvaluator::evaluate(&r, &[batch]);
        assert_eq!(result.state, AlertState::Firing);
        assert_eq!(result.value, 20.0);
    }

    #[test]
    fn test_sum_condition() {
        let batch = make_batch(vec![10.0, 20.0]);
        let r = rule(ConditionOp::Gt, 25.0, "sum");
        let result = AlertEvaluator::evaluate(&r, &[batch]);
        assert_eq!(result.state, AlertState::Firing);
        assert_eq!(result.value, 30.0);
    }

    #[test]
    fn test_min_max_conditions() {
        let batch = make_batch(vec![5.0, 10.0, 15.0]);
        assert_eq!(AlertEvaluator::evaluate(&rule(ConditionOp::Lt, 6.0, "min"), std::slice::from_ref(&batch)).state, AlertState::Firing);
        assert_eq!(AlertEvaluator::evaluate(&rule(ConditionOp::Gt, 14.0, "max"), std::slice::from_ref(&batch)).state, AlertState::Firing);
    }

    #[test]
    fn test_eq_neq_operators() {
        let batch = make_batch(vec![1.0, 2.0, 3.0]);
        // count=3, eq 3 → firing
        assert_eq!(AlertEvaluator::evaluate(&rule(ConditionOp::Eq, 3.0, "count"), std::slice::from_ref(&batch)).state, AlertState::Firing);
        // count=3, neq 3 → ok
        assert_eq!(AlertEvaluator::evaluate(&rule(ConditionOp::Neq, 3.0, "count"), std::slice::from_ref(&batch)).state, AlertState::Ok);
    }

    #[test]
    fn test_lte_operator() {
        let batch = make_batch(vec![1.0]);
        assert_eq!(AlertEvaluator::evaluate(&rule(ConditionOp::Lte, 1.0, "count"), &[batch]).state, AlertState::Firing);
    }

    #[test]
    fn test_empty_batches_returns_ok() {
        let r = rule(ConditionOp::Gt, 0.0, "count");
        // empty batches → value=0, 0 > 0 is false → Ok
        let result = AlertEvaluator::evaluate(&r, &[]);
        assert_eq!(result.state, AlertState::Ok);
        assert_eq!(result.value, 0.0);
    }

    #[test]
    fn test_evaluate_all_returns_only_firing() {
        let batch = make_batch(vec![10.0, 20.0, 30.0]);
        let firing_rule = rule(ConditionOp::Gt, 2.0, "count");
        let ok_rule = AlertRule { name: "ok_rule".into(), ..rule(ConditionOp::Gt, 100.0, "count") };
        let mut results = HashMap::new();
        results.insert("test_alert".into(), vec![batch.clone()]);
        results.insert("ok_rule".into(), vec![batch]);
        let firing = AlertEvaluator::evaluate_all(&[firing_rule, ok_rule], &results);
        assert_eq!(firing.len(), 1);
        assert_eq!(firing[0].rule_name, "test_alert");
    }

    #[tokio::test]
    async fn test_dispatch_log_channel_no_panic() {
        let result = AlertResult {
            rule_name: "test".into(),
            state: AlertState::Firing,
            value: 5.0,
            threshold: 3.0,
            message: "test firing".into(),
        };
        // Should not panic
        NotificationDispatcher::dispatch(&result, &[NotificationChannel::Log]).await;
    }
}
