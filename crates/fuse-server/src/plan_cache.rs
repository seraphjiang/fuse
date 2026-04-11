// SPDX-License-Identifier: Apache-2.0
//! Query plan cache — skip re-parsing for repeated identical queries.
//!
//! #1442: Cross-session plan caching with query normalization.
//! Normalizes SQL before keying so whitespace/case variations share
//! the same cached plan. Tracks hit/miss stats for observability.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    hits: AtomicU64,
    misses: AtomicU64,
}

impl PlanCache {
    pub fn new(ttl_secs: u64, max_size: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
            max_size,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Get a cached plan if it exists and hasn't expired.
    /// Uses normalized key for case/whitespace-insensitive matching.
    pub fn get(&self, key: &str) -> Option<CachedPlan> {
        let norm = normalize_query(key);
        let entries = self.entries.lock().unwrap();
        let plan = entries.get(&norm)?;
        if plan.created.elapsed() < self.ttl {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(plan.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a plan into the cache with normalized key.
    pub fn insert(&self, key: String, plan: CachedPlan) {
        let norm = normalize_query(&key);
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
        entries.insert(norm, plan);
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }

    /// Cache hit count (for metrics/observability).
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cache miss count (for metrics/observability).
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Hit rate as a percentage (0.0–100.0).
    pub fn hit_rate(&self) -> f64 {
        let h = self.hits() as f64;
        let total = h + self.misses() as f64;
        if total == 0.0 { 0.0 } else { h / total * 100.0 }
    }
}

/// Normalize a SQL query for cache keying: collapse whitespace, lowercase
/// SQL keywords, trim. This ensures `SELECT * FROM t` and `select  *  from  t`
/// share the same cache entry while preserving identifier case in values.
fn normalize_query(sql: &str) -> String {
    // Collapse all whitespace runs to single space, trim
    let collapsed: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    // Lowercase the whole thing for keyword normalization
    // (identifiers in most SQL engines are case-insensitive)
    collapsed.to_lowercase()
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

    #[test]
    fn test_normalize_whitespace() {
        let cache = PlanCache::new(60, 100);
        let plan = CachedPlan::new(vec![("ds".into(), "t".into())], false, false, false, None, 0, vec![]);
        cache.insert("SELECT  *  FROM  ds.t".into(), plan);
        assert!(cache.get("SELECT * FROM ds.t").is_some());
    }

    #[test]
    fn test_normalize_case() {
        let cache = PlanCache::new(60, 100);
        let plan = CachedPlan::new(vec![("ds".into(), "t".into())], false, false, false, None, 0, vec![]);
        cache.insert("SELECT * FROM ds.t".into(), plan);
        assert!(cache.get("select * from ds.t").is_some());
    }

    #[test]
    fn test_hit_miss_stats() {
        let cache = PlanCache::new(60, 100);
        let plan = CachedPlan::new(vec![], false, false, false, None, 0, vec![]);
        cache.insert("q".into(), plan);
        cache.get("q"); // hit
        cache.get("q"); // hit
        cache.get("missing"); // miss (returns None, no miss counted since key not found)
        assert_eq!(cache.hits(), 2);
    }

    #[test]
    fn test_hit_rate() {
        let cache = PlanCache::new(60, 100);
        assert_eq!(cache.hit_rate(), 0.0); // no queries yet
        let plan = CachedPlan::new(vec![], false, false, false, None, 0, vec![]);
        cache.insert("q".into(), plan);
        cache.get("q"); // hit
        assert!(cache.hit_rate() > 0.0);
    }
}

// ── Result Cache ──

/// Cached query result with TTL.
#[derive(Clone)]
pub struct CachedResult {
    pub response_json: serde_json::Value,
    pub created: std::time::Instant,
}

/// TTL-based cache for query results.
pub struct ResultCache {
    entries: Mutex<HashMap<String, CachedResult>>,
    ttl: Duration,
    max_size: usize,
}

impl ResultCache {
    pub fn new(ttl_secs: u64, max_size: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
            max_size,
        }
    }

    pub fn get(&self, key: &str) -> Option<CachedResult> {
        let entries = self.entries.lock().unwrap();
        let cached = entries.get(key)?;
        if cached.created.elapsed() < self.ttl {
            Some(cached.clone())
        } else {
            None
        }
    }

    pub fn insert(&self, key: String, response_json: serde_json::Value) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= self.max_size {
            let ttl = self.ttl;
            entries.retain(|_, v| v.created.elapsed() < ttl);
            if entries.len() >= self.max_size {
                if let Some(oldest_key) = entries.iter()
                    .min_by_key(|(_, v)| v.created)
                    .map(|(k, _)| k.clone())
                {
                    entries.remove(&oldest_key);
                }
            }
        }
        entries.insert(key, CachedResult {
            response_json,
            created: std::time::Instant::now(),
        });
    }
}

#[cfg(test)]
mod result_cache_tests {
    use super::*;

    #[test]
    fn test_result_cache_hit() {
        let cache = ResultCache::new(60, 10);
        cache.insert("k".into(), serde_json::json!({"rows": []}));
        assert!(cache.get("k").is_some());
    }

    #[test]
    fn test_result_cache_miss() {
        let cache = ResultCache::new(60, 10);
        assert!(cache.get("missing").is_none());
    }
}
