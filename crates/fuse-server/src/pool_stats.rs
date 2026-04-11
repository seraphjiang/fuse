// SPDX-License-Identifier: Apache-2.0
//! Connection pool statistics — track utilization per connector.

use std::collections::HashMap;
use std::sync::Mutex;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct PoolStats {
    pub active: u32,
    pub idle: u32,
    pub total_acquired: u64,
    pub total_released: u64,
    pub total_timeouts: u64,
}

impl Default for PoolStats {
    fn default() -> Self {
        Self { active: 0, idle: 0, total_acquired: 0, total_released: 0, total_timeouts: 0 }
    }
}

pub struct PoolTracker {
    stats: Mutex<HashMap<String, PoolStats>>,
}

impl PoolTracker {
    pub fn new() -> Self {
        Self { stats: Mutex::new(HashMap::new()) }
    }

    pub fn acquire(&self, connector: &str) {
        let mut map = self.stats.lock().unwrap();
        let s = map.entry(connector.to_string()).or_default();
        s.active += 1;
        s.total_acquired += 1;
    }

    pub fn release(&self, connector: &str) {
        let mut map = self.stats.lock().unwrap();
        if let Some(s) = map.get_mut(connector) {
            s.active = s.active.saturating_sub(1);
            s.idle += 1;
            s.total_released += 1;
        }
    }

    pub fn timeout(&self, connector: &str) {
        let mut map = self.stats.lock().unwrap();
        let s = map.entry(connector.to_string()).or_default();
        s.total_timeouts += 1;
    }

    pub fn snapshot(&self) -> HashMap<String, PoolStats> {
        self.stats.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_release() {
        let t = PoolTracker::new();
        t.acquire("pg");
        t.acquire("pg");
        t.release("pg");
        let s = t.snapshot();
        assert_eq!(s["pg"].active, 1);
        assert_eq!(s["pg"].total_acquired, 2);
        assert_eq!(s["pg"].total_released, 1);
    }

    #[test]
    fn test_timeout() {
        let t = PoolTracker::new();
        t.timeout("slow_ds");
        assert_eq!(t.snapshot()["slow_ds"].total_timeouts, 1);
    }

    #[test]
    fn test_multiple_connectors() {
        let t = PoolTracker::new();
        t.acquire("pg");
        t.acquire("es");
        let s = t.snapshot();
        assert_eq!(s.len(), 2);
    }
}
