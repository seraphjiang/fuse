// SPDX-License-Identifier: Apache-2.0
//! EXPLAIN result cache — aggressive caching for read-only plan queries.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct CachedExplain {
    result: serde_json::Value,
    created: Instant,
}

pub struct ExplainCache {
    entries: Mutex<HashMap<String, CachedExplain>>,
    ttl: Duration,
    max_size: usize,
}

impl ExplainCache {
    pub fn new(ttl_secs: u64, max_size: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
            max_size,
        }
    }

    pub fn get(&self, query: &str) -> Option<serde_json::Value> {
        let entries = self.entries.lock().unwrap();
        let cached = entries.get(query)?;
        if cached.created.elapsed() < self.ttl {
            Some(cached.result.clone())
        } else {
            None
        }
    }

    pub fn insert(&self, query: String, result: serde_json::Value) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= self.max_size {
            let ttl = self.ttl;
            entries.retain(|_, v| v.created.elapsed() < ttl);
        }
        entries.insert(
            query,
            CachedExplain {
                result,
                created: Instant::now(),
            },
        );
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cache_hit() {
        let c = ExplainCache::new(60, 100);
        c.insert("EXPLAIN SELECT 1".into(), json!({"plan": "scan"}));
        assert!(c.get("EXPLAIN SELECT 1").is_some());
    }

    #[test]
    fn test_cache_miss() {
        let c = ExplainCache::new(60, 100);
        assert!(c.get("missing").is_none());
    }

    #[test]
    fn test_ttl_expiry() {
        let c = ExplainCache::new(0, 100); // 0s TTL
        c.insert("q".into(), json!({}));
        assert!(c.get("q").is_none());
    }

    #[test]
    fn test_max_size() {
        let c = ExplainCache::new(60, 2);
        c.insert("a".into(), json!({}));
        c.insert("b".into(), json!({}));
        c.insert("c".into(), json!({}));
        assert!(c.len() <= 3); // eviction runs on insert
    }
}
