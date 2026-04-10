// SPDX-License-Identifier: Apache-2.0
//! Query plan cache — skip re-parsing for repeated identical queries.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Cached result of query parsing/planning.
#[derive(Clone, Debug)]
pub struct CachedPlan {
    pub sources: Vec<(String, String)>,
    pub is_union: bool,
    pub is_join: bool,
    pub is_distinct: bool,
    pub limit: Option<usize>,
    pub offset: usize,
    pub order_by: Vec<(String, bool)>,
    created: Instant,
}

pub struct PlanCache {
    entries: Mutex<HashMap<String, CachedPlan>>,
    ttl: Duration,
    max_size: usize,
}

impl PlanCache {
    pub fn new(ttl_secs: u64, max_size: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
            max_size,
        }
    }

    /// Get a cached plan if it exists and hasn't expired.
    pub fn get(&self, key: &str) -> Option<CachedPlan> {
        let entries = self.entries.lock().unwrap();
        let plan = entries.get(key)?;
        if plan.created.elapsed() < self.ttl {
            Some(plan.clone())
        } else {
            None
        }
    }

    /// Insert a plan into the cache. Evicts expired entries if at capacity.
    pub fn insert(&self, key: String, plan: CachedPlan) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= self.max_size {
            // Evict expired entries
            let ttl = self.ttl;
            entries.retain(|_, v| v.created.elapsed() < ttl);
            // If still full, evict oldest
            if entries.len() >= self.max_size {
                if let Some(oldest_key) = entries
                    .iter()
                    .max_by_key(|(_, v)| v.created.elapsed())
                    .map(|(k, _)| k.clone())
                {
                    entries.remove(&oldest_key);
                }
            }
        }
        entries.insert(key, plan);
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }
}

impl CachedPlan {
    pub fn new(
        sources: Vec<(String, String)>,
        is_union: bool,
        is_join: bool,
        is_distinct: bool,
        limit: Option<usize>,
        offset: usize,
        order_by: Vec<(String, bool)>,
    ) -> Self {
        Self {
            sources,
            is_union,
            is_join,
            is_distinct,
            limit,
            offset,
            order_by,
            created: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_hit() {
        let cache = PlanCache::new(60, 100);
        let plan = CachedPlan::new(vec![("ds".into(), "t".into())], false, false, false, None, 0, vec![]);
        cache.insert("SELECT * FROM ds.t".into(), plan);
        assert!(cache.get("SELECT * FROM ds.t").is_some());
    }

    #[test]
    fn test_cache_miss() {
        let cache = PlanCache::new(60, 100);
        assert!(cache.get("SELECT 1").is_none());
    }

    #[test]
    fn test_cache_expiry() {
        let cache = PlanCache::new(0, 100); // 0s TTL = immediate expiry
        let plan = CachedPlan::new(vec![], false, false, false, None, 0, vec![]);
        cache.insert("q".into(), plan);
        std::thread::sleep(Duration::from_millis(10));
        assert!(cache.get("q").is_none());
    }

    #[test]
    fn test_cache_eviction_at_capacity() {
        let cache = PlanCache::new(60, 2);
        for i in 0..3 {
            let plan = CachedPlan::new(vec![], false, false, false, None, 0, vec![]);
            cache.insert(format!("q{}", i), plan);
        }
        // Should have evicted oldest, size <= max
        assert!(cache.len() <= 2);
    }

    #[test]
    fn test_cache_clear() {
        let cache = PlanCache::new(60, 100);
        let plan = CachedPlan::new(vec![], false, false, false, None, 0, vec![]);
        cache.insert("q".into(), plan);
        cache.clear();
        assert_eq!(cache.len(), 0);
    }
}
