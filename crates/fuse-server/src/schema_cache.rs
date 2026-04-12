// SPDX-License-Identifier: Apache-2.0
//! Schema discovery cache — prevents repeated expensive schema lookups.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

pub struct SchemaCache {
    entries: RwLock<HashMap<String, CacheEntry>>,
    ttl: Duration,
}

struct CacheEntry {
    value: serde_json::Value,
    inserted: Instant,
}

impl SchemaCache {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    pub fn get(&self, key: &str) -> Option<serde_json::Value> {
        let entries = self.entries.read().unwrap();
        let entry = entries.get(key)?;
        if entry.inserted.elapsed() < self.ttl {
            Some(entry.value.clone())
        } else {
            None
        }
    }

    pub fn set(&self, key: String, value: serde_json::Value) {
        self.entries.write().unwrap().insert(
            key,
            CacheEntry {
                value,
                inserted: Instant::now(),
            },
        );
    }

    pub fn invalidate(&self, key: &str) {
        self.entries.write().unwrap().remove(key);
    }

    pub fn clear(&self) {
        self.entries.write().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_hit() {
        let cache = SchemaCache::new(60);
        cache.set("key".into(), serde_json::json!(["table1"]));
        assert!(cache.get("key").is_some());
    }

    #[test]
    fn test_cache_miss() {
        let cache = SchemaCache::new(60);
        assert!(cache.get("missing").is_none());
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = SchemaCache::new(60);
        cache.set("key".into(), serde_json::json!(1));
        cache.invalidate("key");
        assert!(cache.get("key").is_none());
    }

    #[test]
    fn test_cache_overwrite() {
        let cache = SchemaCache::new(60);
        cache.set("k".into(), serde_json::json!(1));
        cache.set("k".into(), serde_json::json!(2));
        assert_eq!(cache.get("k").unwrap(), serde_json::json!(2));
    }

    #[test]
    fn test_cache_clear() {
        let cache = SchemaCache::new(60);
        cache.set("a".into(), serde_json::json!(1));
        cache.set("b".into(), serde_json::json!(2));
        cache.clear();
        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_none());
    }
}
