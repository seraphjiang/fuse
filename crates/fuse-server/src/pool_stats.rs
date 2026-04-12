// SPDX-License-Identifier: Apache-2.0
//! Connection pooling stats — expose pool utilization per connector.

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize)]
pub struct PoolStats {
    pub connector_id: String,
    pub active: u32,
    pub idle: u32,
    pub max_size: u32,
    pub utilization_pct: f64,
    pub total_acquired: u64,
    pub total_timeouts: u64,
}

pub struct PoolStatsTracker {
    stats: Mutex<HashMap<String, PoolState>>,
}

struct PoolState {
    active: u32,
    max_size: u32,
    total_acquired: u64,
    total_timeouts: u64,
}

impl PoolStatsTracker {
    pub fn new() -> Self {
        Self { stats: Mutex::new(HashMap::new()) }
    }

    pub fn register(&self, connector_id: &str, max_size: u32) {
        self.stats.lock().unwrap().insert(connector_id.to_string(), PoolState {
            active: 0, max_size, total_acquired: 0, total_timeouts: 0,
        });
    }

    pub fn acquire(&self, connector_id: &str) {
        if let Some(s) = self.stats.lock().unwrap().get_mut(connector_id) {
            s.active += 1;
            s.total_acquired += 1;
        }
    }

    pub fn release(&self, connector_id: &str) {
        if let Some(s) = self.stats.lock().unwrap().get_mut(connector_id) {
            s.active = s.active.saturating_sub(1);
        }
    }

    pub fn timeout(&self, connector_id: &str) {
        if let Some(s) = self.stats.lock().unwrap().get_mut(connector_id) {
            s.total_timeouts += 1;
        }
    }

    pub fn get(&self, connector_id: &str) -> Option<PoolStats> {
        let map = self.stats.lock().unwrap();
        let s = map.get(connector_id)?;
        let idle = s.max_size.saturating_sub(s.active);
        Some(PoolStats {
            connector_id: connector_id.to_string(),
            active: s.active,
            idle,
            max_size: s.max_size,
            utilization_pct: if s.max_size > 0 { s.active as f64 / s.max_size as f64 * 100.0 } else { 0.0 },
            total_acquired: s.total_acquired,
            total_timeouts: s.total_timeouts,
        })
    }

    pub fn all(&self) -> Vec<PoolStats> {
        let ids: Vec<String> = self.stats.lock().unwrap().keys().cloned().collect();
        ids.iter().filter_map(|id| self.get(id)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get() {
        let t = PoolStatsTracker::new();
        t.register("pg1", 10);
        let s = t.get("pg1").unwrap();
        assert_eq!(s.max_size, 10);
        assert_eq!(s.active, 0);
        assert_eq!(s.idle, 10);
        assert!((s.utilization_pct - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_acquire_release() {
        let t = PoolStatsTracker::new();
        t.register("pg1", 5);
        t.acquire("pg1");
        t.acquire("pg1");
        let s = t.get("pg1").unwrap();
        assert_eq!(s.active, 2);
        assert_eq!(s.idle, 3);
        assert!((s.utilization_pct - 40.0).abs() < 0.01);
        t.release("pg1");
        let s = t.get("pg1").unwrap();
        assert_eq!(s.active, 1);
        assert_eq!(s.total_acquired, 2);
    }

    #[test]
    fn test_timeout() {
        let t = PoolStatsTracker::new();
        t.register("pg1", 5);
        t.timeout("pg1");
        t.timeout("pg1");
        assert_eq!(t.get("pg1").unwrap().total_timeouts, 2);
    }

    #[test]
    fn test_missing_connector() {
        let t = PoolStatsTracker::new();
        assert!(t.get("nonexistent").is_none());
    }
}
