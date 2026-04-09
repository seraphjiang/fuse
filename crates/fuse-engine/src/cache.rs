// SPDX-License-Identifier: Apache-2.0

//! Query result cache with per-connector TTL expiry.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use serde::Serialize;

/// Cached query result with TTL.
struct CachedResult {
    batches: Vec<RecordBatch>,
    created: Instant,
    ttl: Duration,
}

impl CachedResult {
    fn is_expired(&self) -> bool {
        self.created.elapsed() > self.ttl
    }
}

/// Cache statistics.
#[derive(Debug, Clone, Serialize)]
pub struct CacheStats {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// Default TTLs by connector type.
pub fn default_ttl(connector_type: &str) -> Duration {
    match connector_type {
        "opensearch" => Duration::from_secs(30),
        "s3" => Duration::from_secs(300),
        "prometheus" => Duration::from_secs(60),
        _ => Duration::from_secs(30),
    }
}

/// Compute cache key from connector ID and query string.
pub fn cache_key(connector_id: &str, query: &str) -> u64 {
    let mut h = DefaultHasher::new();
    connector_id.hash(&mut h);
    query.hash(&mut h);
    h.finish()
}

/// Thread-safe query result cache.
pub struct QueryCache {
    inner: RwLock<CacheInner>,
}

struct CacheInner {
    entries: HashMap<u64, CachedResult>,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl QueryCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(CacheInner {
                entries: HashMap::new(),
                hits: 0,
                misses: 0,
                evictions: 0,
            }),
        }
    }

    /// Get cached result if present and not expired.
    pub fn get(&self, key: u64) -> Option<Vec<RecordBatch>> {
        let mut inner = self.inner.write().unwrap();
        match inner.entries.get(&key) {
            Some(entry) if !entry.is_expired() => {
                let batches = entry.batches.clone();
                inner.hits += 1;
                Some(batches)
            }
            Some(_) => {
                // Expired
                inner.entries.remove(&key);
                inner.evictions += 1;
                inner.misses += 1;
                None
            }
            None => {
                inner.misses += 1;
                None
            }
        }
    }

    /// Store result with given TTL.
    pub fn put(&self, key: u64, batches: Vec<RecordBatch>, ttl: Duration) {
        let mut inner = self.inner.write().unwrap();
        inner.entries.insert(
            key,
            CachedResult {
                batches,
                created: Instant::now(),
                ttl,
            },
        );
    }

    /// Remove all expired entries.
    pub fn evict_expired(&self) -> usize {
        let mut inner = self.inner.write().unwrap();
        let before = inner.entries.len();
        inner.entries.retain(|_, v| !v.is_expired());
        let removed = before - inner.entries.len();
        inner.evictions += removed as u64;
        removed
    }

    /// Clear all entries.
    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.entries.clear();
    }

    /// Return cache statistics.
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.read().unwrap();
        CacheStats {
            entries: inner.entries.len(),
            hits: inner.hits,
            misses: inner.misses,
            evictions: inner.evictions,
        }
    }
}

impl Default for QueryCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_batch(n: i64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        vec![
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![n]))]).unwrap(),
        ]
    }

    #[test]
    fn test_default_ttl_known_types() {
        assert_eq!(default_ttl("opensearch"), Duration::from_secs(30));
        assert_eq!(default_ttl("s3"), Duration::from_secs(300));
        assert_eq!(default_ttl("prometheus"), Duration::from_secs(60));
    }

    #[test]
    fn test_default_ttl_unknown_type() {
        assert_eq!(default_ttl("postgres"), Duration::from_secs(30));
    }

    #[test]
    fn test_cache_key_deterministic() {
        let k1 = cache_key("conn1", "SELECT * FROM t");
        let k2 = cache_key("conn1", "SELECT * FROM t");
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_cache_key_differs_by_connector() {
        let k1 = cache_key("conn1", "SELECT 1");
        let k2 = cache_key("conn2", "SELECT 1");
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_cache_key_differs_by_query() {
        let k1 = cache_key("conn1", "SELECT 1");
        let k2 = cache_key("conn1", "SELECT 2");
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_put_and_get() {
        let cache = QueryCache::new();
        let batches = test_batch(42);
        cache.put(1, batches.clone(), Duration::from_secs(60));

        let result = cache.get(1);
        assert!(result.is_some());
        assert_eq!(result.unwrap()[0].num_rows(), 1);
    }

    #[test]
    fn test_get_miss() {
        let cache = QueryCache::new();
        assert!(cache.get(999).is_none());
    }

    #[test]
    fn test_get_expired_entry() {
        let cache = QueryCache::new();
        cache.put(1, test_batch(1), Duration::from_millis(0));
        // TTL of 0ms means it's already expired
        std::thread::sleep(Duration::from_millis(1));
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_stats_initial() {
        let cache = QueryCache::new();
        let s = cache.stats();
        assert_eq!(s.entries, 0);
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 0);
        assert_eq!(s.evictions, 0);
    }

    #[test]
    fn test_stats_hit_and_miss() {
        let cache = QueryCache::new();
        cache.put(1, test_batch(1), Duration::from_secs(60));

        cache.get(1); // hit
        cache.get(2); // miss

        let s = cache.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.entries, 1);
    }

    #[test]
    fn test_stats_expired_counts_as_eviction() {
        let cache = QueryCache::new();
        cache.put(1, test_batch(1), Duration::from_millis(0));
        std::thread::sleep(Duration::from_millis(1));
        cache.get(1); // expired → eviction + miss

        let s = cache.stats();
        assert_eq!(s.evictions, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.hits, 0);
        assert_eq!(s.entries, 0);
    }

    #[test]
    fn test_evict_expired() {
        let cache = QueryCache::new();
        cache.put(1, test_batch(1), Duration::from_millis(0));
        cache.put(2, test_batch(2), Duration::from_secs(60));
        std::thread::sleep(Duration::from_millis(1));

        let removed = cache.evict_expired();
        assert_eq!(removed, 1);
        assert_eq!(cache.stats().entries, 1);
        assert!(cache.get(2).is_some());
    }

    #[test]
    fn test_clear() {
        let cache = QueryCache::new();
        cache.put(1, test_batch(1), Duration::from_secs(60));
        cache.put(2, test_batch(2), Duration::from_secs(60));
        assert_eq!(cache.stats().entries, 2);

        cache.clear();
        assert_eq!(cache.stats().entries, 0);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_overwrite_existing_key() {
        let cache = QueryCache::new();
        cache.put(1, test_batch(10), Duration::from_secs(60));
        cache.put(1, test_batch(20), Duration::from_secs(60));

        assert_eq!(cache.stats().entries, 1);
        let batches = cache.get(1).unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 20);
    }

    #[test]
    fn test_thread_safety() {
        let cache = Arc::new(QueryCache::new());
        let mut handles = vec![];

        for i in 0..10 {
            let c = cache.clone();
            handles.push(std::thread::spawn(move || {
                c.put(i, test_batch(i as i64), Duration::from_secs(60));
                c.get(i);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(cache.stats().entries, 10);
        assert_eq!(cache.stats().hits, 10);
    }
}
