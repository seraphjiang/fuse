// SPDX-License-Identifier: Apache-2.0
//! Query deduplication — coalesce identical concurrent queries.
//!
//! When multiple clients submit the same query simultaneously,
//! only one execution runs and all waiters receive the same result.

use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 1;

/// Deduplication result.
pub enum DedupeResult {
    /// This is the first query — caller should execute it.
    Execute,
    /// Duplicate detected — wait for the first query's result.
    Wait(broadcast::Receiver<serde_json::Value>),
}

/// Query deduplicator.
pub struct QueryDedup {
    inflight: Mutex<HashMap<String, broadcast::Sender<serde_json::Value>>>,
}

impl Default for QueryDedup {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryDedup {
    pub fn new() -> Self {
        Self { inflight: Mutex::new(HashMap::new()) }
    }

    /// Check if a query is already in-flight. Returns Execute or Wait.
    pub fn check(&self, key: &str) -> DedupeResult {
        let mut map = self.inflight.lock().unwrap();
        if let Some(tx) = map.get(key) {
            DedupeResult::Wait(tx.subscribe())
        } else {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            map.insert(key.to_string(), tx);
            DedupeResult::Execute
        }
    }

    /// Complete a query and notify all waiters.
    pub fn complete(&self, key: &str, result: serde_json::Value) {
        let mut map = self.inflight.lock().unwrap();
        if let Some(tx) = map.remove(key) {
            let _ = tx.send(result);
        }
    }

    /// Cancel/remove an in-flight query without sending a result.
    pub fn cancel(&self, key: &str) {
        self.inflight.lock().unwrap().remove(key);
    }

    pub fn inflight_count(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_first_query_executes() {
        let dedup = QueryDedup::new();
        assert!(matches!(dedup.check("SELECT 1"), DedupeResult::Execute));
        assert_eq!(dedup.inflight_count(), 1);
    }

    #[test]
    fn test_duplicate_waits() {
        let dedup = QueryDedup::new();
        assert!(matches!(dedup.check("SELECT 1"), DedupeResult::Execute));
        assert!(matches!(dedup.check("SELECT 1"), DedupeResult::Wait(_)));
    }

    #[tokio::test]
    async fn test_complete_notifies_waiter() {
        let dedup = QueryDedup::new();
        let _ = dedup.check("q1");
        if let DedupeResult::Wait(mut rx) = dedup.check("q1") {
            dedup.complete("q1", serde_json::json!({"rows": []}));
            let result = rx.recv().await.unwrap();
            assert!(result["rows"].is_array());
        } else {
            panic!("expected Wait");
        }
        assert_eq!(dedup.inflight_count(), 0);
    }

    #[test]
    fn test_cancel_removes() {
        let dedup = QueryDedup::new();
        let _ = dedup.check("q1");
        dedup.cancel("q1");
        assert_eq!(dedup.inflight_count(), 0);
    }
}
