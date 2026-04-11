// SPDX-License-Identifier: Apache-2.0
//! #910 Continuous anomaly alert monitor.
//!
//! Periodically evaluates alert rules against query history and fires
//! notifications (webhook) when thresholds are breached.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};

/// An alert rule definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    /// Metric to monitor: "latency_p95", "error_rate", "query_count"
    pub metric: String,
    /// Threshold value — alert fires when metric exceeds this
    pub threshold: f64,
    /// Evaluation window in seconds
    pub window_secs: u64,
    /// Webhook URL for notifications (optional)
    pub webhook_url: Option<String>,
    /// Whether the rule is active
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool { true }

/// A fired alert instance.
#[derive(Debug, Clone, Serialize)]
pub struct FiredAlert {
    pub rule_id: String,
    pub rule_name: String,
    pub metric: String,
    pub value: f64,
    pub threshold: f64,
    pub fired_at: u64,
    pub status: AlertStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertStatus {
    Firing,
    Resolved,
    Acknowledged,
}

/// In-memory alert state — tracks fired alerts and history.
pub struct AlertMonitor {
    rules: Mutex<Vec<AlertRule>>,
    history: Mutex<Vec<FiredAlert>>,
    /// Currently firing alerts by rule_id
    active: Mutex<HashMap<String, FiredAlert>>,
}

impl Default for AlertMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl AlertMonitor {
    pub fn new() -> Self {
        Self {
            rules: Mutex::new(Vec::new()),
            history: Mutex::new(Vec::new()),
            active: Mutex::new(HashMap::new()),
        }
    }

    pub fn from_rules(rules: Vec<AlertRule>) -> Self {
        Self {
            rules: Mutex::new(rules),
            history: Mutex::new(Vec::new()),
            active: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_rule(&self, rule: AlertRule) {
        self.rules.lock().unwrap().push(rule);
    }

    pub fn remove_rule(&self, rule_id: &str) -> bool {
        let mut rules = self.rules.lock().unwrap();
        let before = rules.len();
        rules.retain(|r| r.id != rule_id);
        rules.len() < before
    }

    pub fn list_rules(&self) -> Vec<AlertRule> {
        self.rules.lock().unwrap().clone()
    }

    pub fn list_active(&self) -> Vec<FiredAlert> {
        self.active.lock().unwrap().values().cloned().collect()
    }

    pub fn list_history(&self, max: usize) -> Vec<FiredAlert> {
        let h = self.history.lock().unwrap();
        h.iter().rev().take(max).cloned().collect()
    }

    pub fn acknowledge(&self, rule_id: &str) -> bool {
        let mut active = self.active.lock().unwrap();
        if let Some(alert) = active.get_mut(rule_id) {
            alert.status = AlertStatus::Acknowledged;
            true
        } else {
            false
        }
    }

    /// Evaluate all rules against current metrics. Returns newly fired alerts.
    pub fn evaluate(&self, metrics: &HashMap<String, f64>) -> Vec<FiredAlert> {
        let rules = self.rules.lock().unwrap().clone();
        let mut active = self.active.lock().unwrap();
        let mut history = self.history.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut newly_fired = Vec::new();

        for rule in &rules {
            if !rule.enabled {
                continue;
            }
            let value = metrics.get(&rule.metric).copied().unwrap_or(0.0);

            if value > rule.threshold {
                if !active.contains_key(&rule.id) {
                    let alert = FiredAlert {
                        rule_id: rule.id.clone(),
                        rule_name: rule.name.clone(),
                        metric: rule.metric.clone(),
                        value,
                        threshold: rule.threshold,
                        fired_at: now,
                        status: AlertStatus::Firing,
                    };
                    active.insert(rule.id.clone(), alert.clone());
                    history.push(alert.clone());
                    newly_fired.push(alert);
                }
            } else if active.contains_key(&rule.id) {
                // Resolved
                let mut resolved = active.remove(&rule.id).unwrap();
                resolved.status = AlertStatus::Resolved;
                history.push(resolved);
            }
        }

        // Cap history
        if history.len() > 1000 {
            let start = history.len() - 1000;
            let trimmed = history[start..].to_vec();
            *history = trimmed;
        }

        newly_fired
    }
}

/// Send a webhook notification for a fired alert.
pub async fn send_webhook(url: &str, alert: &FiredAlert) -> Result<(), String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .json(alert)
        .send()
        .await
        .map_err(|e| format!("webhook failed: {e}"))?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!("webhook returned {}", resp.status()))
    }
}

/// Spawn a background alert evaluation loop.
pub fn spawn_alert_loop(
    monitor: Arc<AlertMonitor>,
    metrics_fn: Arc<dyn Fn() -> HashMap<String, f64> + Send + Sync>,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(
            tokio::time::Duration::from_secs(interval_secs),
        );
        loop {
            interval.tick().await;
            let metrics = metrics_fn();
            let fired = monitor.evaluate(&metrics);
            for alert in &fired {
                let rules = monitor.list_rules();
                if let Some(rule) = rules.iter().find(|r| r.id == alert.rule_id) {
                    if let Some(ref url) = rule.webhook_url {
                        if let Err(e) = send_webhook(url, alert).await {
                            tracing::warn!(rule_id = %alert.rule_id, error = %e, "Alert webhook failed");
                        }
                    }
                }
                tracing::warn!(
                    rule = %alert.rule_name,
                    metric = %alert.metric,
                    value = alert.value,
                    threshold = alert.threshold,
                    "Alert fired"
                );
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn monitor_with_rule() -> AlertMonitor {
        let m = AlertMonitor::new();
        m.add_rule(AlertRule {
            id: "r1".into(),
            name: "High latency".into(),
            metric: "latency_p95".into(),
            threshold: 1000.0,
            window_secs: 60,
            webhook_url: None,
            enabled: true,
        });
        m
    }

    #[test]
    fn test_evaluate_fires_alert() {
        let m = monitor_with_rule();
        let mut metrics = HashMap::new();
        metrics.insert("latency_p95".into(), 1500.0);
        let fired = m.evaluate(&metrics);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].rule_id, "r1");
        assert_eq!(fired[0].status, AlertStatus::Firing);
    }

    #[test]
    fn test_evaluate_no_fire_below_threshold() {
        let m = monitor_with_rule();
        let mut metrics = HashMap::new();
        metrics.insert("latency_p95".into(), 500.0);
        let fired = m.evaluate(&metrics);
        assert!(fired.is_empty());
    }

    #[test]
    fn test_evaluate_deduplicates() {
        let m = monitor_with_rule();
        let mut metrics = HashMap::new();
        metrics.insert("latency_p95".into(), 1500.0);
        assert_eq!(m.evaluate(&metrics).len(), 1);
        // Second evaluation — already firing, no new alert
        assert_eq!(m.evaluate(&metrics).len(), 0);
    }

    #[test]
    fn test_evaluate_resolves() {
        let m = monitor_with_rule();
        let mut metrics = HashMap::new();
        metrics.insert("latency_p95".into(), 1500.0);
        m.evaluate(&metrics);
        assert_eq!(m.list_active().len(), 1);

        // Drop below threshold — resolves
        metrics.insert("latency_p95".into(), 500.0);
        m.evaluate(&metrics);
        assert_eq!(m.list_active().len(), 0);
        // History has 2 entries: fired + resolved
        assert_eq!(m.list_history(10).len(), 2);
    }

    #[test]
    fn test_acknowledge() {
        let m = monitor_with_rule();
        let mut metrics = HashMap::new();
        metrics.insert("latency_p95".into(), 1500.0);
        m.evaluate(&metrics);
        assert!(m.acknowledge("r1"));
        assert_eq!(m.list_active()[0].status, AlertStatus::Acknowledged);
    }

    #[test]
    fn test_acknowledge_nonexistent() {
        let m = AlertMonitor::new();
        assert!(!m.acknowledge("nope"));
    }

    #[test]
    fn test_add_remove_rules() {
        let m = monitor_with_rule();
        assert_eq!(m.list_rules().len(), 1);
        assert!(m.remove_rule("r1"));
        assert_eq!(m.list_rules().len(), 0);
        assert!(!m.remove_rule("r1")); // already removed
    }

    #[test]
    fn test_disabled_rule_skipped() {
        let m = AlertMonitor::new();
        m.add_rule(AlertRule {
            id: "d1".into(),
            name: "Disabled".into(),
            metric: "latency_p95".into(),
            threshold: 100.0,
            window_secs: 60,
            webhook_url: None,
            enabled: false,
        });
        let mut metrics = HashMap::new();
        metrics.insert("latency_p95".into(), 9999.0);
        assert!(m.evaluate(&metrics).is_empty());
    }

    #[test]
    fn test_history_capped() {
        let m = AlertMonitor::new();
        for i in 0..1100 {
            m.add_rule(AlertRule {
                id: format!("r{i}"),
                name: format!("Rule {i}"),
                metric: "x".into(),
                threshold: 0.0,
                window_secs: 60,
                webhook_url: None,
                enabled: true,
            });
        }
        let mut metrics = HashMap::new();
        metrics.insert("x".into(), 1.0);
        m.evaluate(&metrics);
        assert!(m.list_history(2000).len() <= 1000);
    }
}
