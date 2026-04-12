// SPDX-License-Identifier: Apache-2.0

//! Scheduled queries — run queries on a cron schedule (#1800).
//!
//! Define recurring queries that execute automatically, store results,
//! and optionally trigger alerts on changes. Builds on async_query
//! and anomaly_alert modules.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// A scheduled query definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledQuery {
    pub id: String,
    pub name: String,
    pub query: String,
    pub format: String,
    pub cron: String,
    pub enabled: bool,
    #[serde(default)]
    pub alert_on_change: bool,
    #[serde(default)]
    pub alert_on_error: bool,
    pub created_at: u64,
}

/// Result of a scheduled execution.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleExecution {
    pub schedule_id: String,
    pub executed_at: u64,
    pub row_count: u64,
    pub latency_ms: u64,
    pub status: ExecStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExecStatus {
    Success,
    Failed,
    Changed,
}

/// Parse a simple cron expression and check if it should run now.
/// Supports: `*/N` (every N), `*` (every), and literal values.
/// Format: `minute hour day_of_month month day_of_week`
pub fn cron_matches(cron: &str, minute: u32, hour: u32) -> bool {
    let parts: Vec<&str> = cron.split_whitespace().collect();
    if parts.len() < 2 { return false; }
    field_matches(parts[0], minute) && field_matches(parts[1], hour)
}

fn field_matches(field: &str, value: u32) -> bool {
    if field == "*" { return true; }
    if let Some(interval) = field.strip_prefix("*/") {
        if let Ok(n) = interval.parse::<u32>() {
            return n > 0 && value % n == 0;
        }
    }
    if let Ok(n) = field.parse::<u32>() {
        return n == value;
    }
    // Comma-separated values
    field.split(',').any(|v| v.trim().parse::<u32>().ok() == Some(value))
}

/// Schedule registry — stores and manages scheduled queries.
pub struct ScheduleRegistry {
    schedules: Mutex<HashMap<String, ScheduledQuery>>,
    history: Mutex<Vec<ScheduleExecution>>,
    max_history: usize,
}

impl ScheduleRegistry {
    pub fn new(max_history: usize) -> Self {
        Self {
            schedules: Mutex::new(HashMap::new()),
            history: Mutex::new(Vec::new()),
            max_history,
        }
    }

    pub fn add(&self, schedule: ScheduledQuery) {
        self.schedules.lock().unwrap().insert(schedule.id.clone(), schedule);
    }

    pub fn remove(&self, id: &str) -> bool {
        self.schedules.lock().unwrap().remove(id).is_some()
    }

    pub fn get(&self, id: &str) -> Option<ScheduledQuery> {
        self.schedules.lock().unwrap().get(id).cloned()
    }

    pub fn list(&self) -> Vec<ScheduledQuery> {
        self.schedules.lock().unwrap().values().cloned().collect()
    }

    /// Get schedules that should run at the given minute/hour.
    pub fn due(&self, minute: u32, hour: u32) -> Vec<ScheduledQuery> {
        self.schedules.lock().unwrap().values()
            .filter(|s| s.enabled && cron_matches(&s.cron, minute, hour))
            .cloned().collect()
    }

    pub fn record_execution(&self, exec: ScheduleExecution) {
        let mut history = self.history.lock().unwrap();
        history.push(exec);
        if history.len() > self.max_history {
            let drain_count = history.len() - self.max_history;
            history.drain(0..drain_count);
        }
    }

    pub fn history(&self, schedule_id: Option<&str>) -> Vec<ScheduleExecution> {
        let history = self.history.lock().unwrap();
        match schedule_id {
            Some(id) => history.iter().filter(|e| e.schedule_id == id).cloned().collect(),
            None => history.clone(),
        }
    }

    pub fn len(&self) -> usize {
        self.schedules.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sq(id: &str, cron: &str) -> ScheduledQuery {
        ScheduledQuery {
            id: id.into(), name: id.into(), query: "SELECT 1".into(),
            format: "sql".into(), cron: cron.into(), enabled: true,
            alert_on_change: false, alert_on_error: false, created_at: 0,
        }
    }

    #[test]
    fn test_cron_every_minute() {
        assert!(cron_matches("* *", 0, 0));
        assert!(cron_matches("* *", 30, 12));
    }

    #[test]
    fn test_cron_every_5_minutes() {
        assert!(cron_matches("*/5 *", 0, 0));
        assert!(cron_matches("*/5 *", 15, 3));
        assert!(!cron_matches("*/5 *", 7, 3));
    }

    #[test]
    fn test_cron_specific_time() {
        assert!(cron_matches("30 9", 30, 9));
        assert!(!cron_matches("30 9", 31, 9));
        assert!(!cron_matches("30 9", 30, 10));
    }

    #[test]
    fn test_cron_comma_values() {
        assert!(cron_matches("0,15,30,45 *", 15, 0));
        assert!(!cron_matches("0,15,30,45 *", 10, 0));
    }

    #[test]
    fn test_cron_invalid() {
        assert!(!cron_matches("", 0, 0));
        assert!(!cron_matches("x", 0, 0));
    }

    #[test]
    fn test_registry_add_and_list() {
        let reg = ScheduleRegistry::new(100);
        reg.add(sq("s1", "*/5 *"));
        reg.add(sq("s2", "0 9"));
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn test_registry_due() {
        let reg = ScheduleRegistry::new(100);
        reg.add(sq("every5", "*/5 *"));
        reg.add(sq("nine_am", "0 9"));
        let due = reg.due(0, 9);
        assert_eq!(due.len(), 2); // both match at 09:00
        let due = reg.due(5, 10);
        assert_eq!(due.len(), 1); // only every5
    }

    #[test]
    fn test_registry_disabled_skipped() {
        let reg = ScheduleRegistry::new(100);
        let mut s = sq("s1", "* *");
        s.enabled = false;
        reg.add(s);
        assert!(reg.due(0, 0).is_empty());
    }

    #[test]
    fn test_registry_remove() {
        let reg = ScheduleRegistry::new(100);
        reg.add(sq("s1", "* *"));
        assert!(reg.remove("s1"));
        assert!(!reg.remove("s1"));
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_execution_history() {
        let reg = ScheduleRegistry::new(100);
        reg.record_execution(ScheduleExecution {
            schedule_id: "s1".into(), executed_at: 1000, row_count: 5,
            latency_ms: 42, status: ExecStatus::Success, error: None,
        });
        assert_eq!(reg.history(None).len(), 1);
        assert_eq!(reg.history(Some("s1")).len(), 1);
        assert_eq!(reg.history(Some("s2")).len(), 0);
    }

    #[test]
    fn test_history_cap() {
        let reg = ScheduleRegistry::new(3);
        for i in 0..5 {
            reg.record_execution(ScheduleExecution {
                schedule_id: format!("s{}", i), executed_at: i as u64,
                row_count: 0, latency_ms: 0, status: ExecStatus::Success, error: None,
            });
        }
        assert_eq!(reg.history(None).len(), 3);
    }
}
