// SPDX-License-Identifier: Apache-2.0
//! Query scheduling — cron-based recurring queries.
//!
//! Schedule queries to run periodically and store results
//! in materialized views or export to external storage.

use std::collections::HashMap;
use std::sync::Mutex;

/// A scheduled query definition.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScheduledQuery {
    pub id: String,
    pub name: String,
    pub query: String,
    pub format: String,
    /// Cron expression (e.g. "0 */5 * * * *" for every 5 minutes).
    pub cron: String,
    pub enabled: bool,
    #[serde(default)]
    pub last_run: Option<u64>,
    #[serde(default)]
    pub last_status: Option<String>,
    #[serde(default)]
    pub run_count: u64,
}

/// Registry of scheduled queries.
pub struct ScheduleRegistry {
    schedules: Mutex<HashMap<String, ScheduledQuery>>,
}

impl ScheduleRegistry {
    pub fn new() -> Self {
        Self { schedules: Mutex::new(HashMap::new()) }
    }

    pub fn add(&self, schedule: ScheduledQuery) {
        self.schedules.lock().unwrap().insert(schedule.id.clone(), schedule);
    }

    pub fn remove(&self, id: &str) -> bool {
        self.schedules.lock().unwrap().remove(id).is_some()
    }

    pub fn list(&self) -> Vec<ScheduledQuery> {
        self.schedules.lock().unwrap().values().cloned().collect()
    }

    pub fn get(&self, id: &str) -> Option<ScheduledQuery> {
        self.schedules.lock().unwrap().get(id).cloned()
    }

    pub fn set_enabled(&self, id: &str, enabled: bool) -> bool {
        if let Some(s) = self.schedules.lock().unwrap().get_mut(id) {
            s.enabled = enabled;
            true
        } else {
            false
        }
    }

    pub fn record_run(&self, id: &str, status: &str) {
        if let Some(s) = self.schedules.lock().unwrap().get_mut(id) {
            s.last_run = Some(crate::audit::now_secs());
            s.last_status = Some(status.to_string());
            s.run_count += 1;
        }
    }

    pub fn due_schedules(&self) -> Vec<ScheduledQuery> {
        self.schedules.lock().unwrap()
            .values()
            .filter(|s| s.enabled)
            .cloned()
            .collect()
    }

    pub fn count(&self) -> usize {
        self.schedules.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(id: &str) -> ScheduledQuery {
        ScheduledQuery {
            id: id.into(), name: format!("Schedule {}", id),
            query: "SELECT 1".into(), format: "sql".into(),
            cron: "0 */5 * * * *".into(), enabled: true,
            last_run: None, last_status: None, run_count: 0,
        }
    }

    #[test]
    fn test_add_and_list() {
        let reg = ScheduleRegistry::new();
        reg.add(sample("s1"));
        reg.add(sample("s2"));
        assert_eq!(reg.count(), 2);
        assert_eq!(reg.list().len(), 2);
    }

    #[test]
    fn test_remove() {
        let reg = ScheduleRegistry::new();
        reg.add(sample("s1"));
        assert!(reg.remove("s1"));
        assert!(!reg.remove("s1"));
        assert_eq!(reg.count(), 0);
    }

    #[test]
    fn test_enable_disable() {
        let reg = ScheduleRegistry::new();
        reg.add(sample("s1"));
        reg.set_enabled("s1", false);
        assert!(!reg.get("s1").unwrap().enabled);
        assert!(reg.due_schedules().is_empty());
    }

    #[test]
    fn test_record_run() {
        let reg = ScheduleRegistry::new();
        reg.add(sample("s1"));
        reg.record_run("s1", "ok");
        let s = reg.get("s1").unwrap();
        assert_eq!(s.run_count, 1);
        assert_eq!(s.last_status.as_deref(), Some("ok"));
    }

    #[test]
    fn test_due_schedules_filters_disabled() {
        let reg = ScheduleRegistry::new();
        reg.add(sample("s1"));
        let mut s2 = sample("s2");
        s2.enabled = false;
        reg.add(s2);
        assert_eq!(reg.due_schedules().len(), 1);
    }
}
