// SPDX-License-Identifier: Apache-2.0

//! Query result cache with per-connector TTL expiry, LRU eviction, and memory budget.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use serde::Serialize;

/// Default maximum number of cache entries.
const DEFAULT_MAX_ENTRIES: usize = 1024;

/// Default memory budget: 256 MB.
const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Estimate memory usage of a single Arrow array (buffer sizes).
fn array_bytes(arr: &ArrayRef) -> usize {
    arr.get_buffer_memory_size()
}

/// Estimate memory usage of a list of RecordBatches.
fn batches_bytes(batches: &[RecordBatch]) -> usize {
    batches
        .iter()
        .map(|b| {
            b.columns()
                .iter()
                .map(|c| array_bytes(c))
                .sum::<usize>()
        })
        .sum()
}

/// Cached query result with TTL and memory tracking.
struct CachedResult {
    batches: Vec<RecordBatch>,
    created: Instant,
    ttl: Duration,
    estimated_bytes: usize,
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
    pub max_entries: usize,
    pub total_bytes: usize,
    pub max_bytes: usize,
}

/// Default TTLs by connector type.
pub fn default_ttl(connector_type: &str) -> Duration {
    match connector_type {
        // Real-time / streaming — short TTL
        "opensearch" | "elasticsearch" => Duration::from_secs(30),
        "prometheus" | "influxdb" | "timestream" => Duration::from_secs(60),
        "cloudwatch" => Duration::from_secs(60),
        "kafka" => Duration::from_secs(10),
        "redis" => Duration::from_secs(15),
        // Databases — moderate TTL
        "postgres" | "mysql" | "clickhouse" | "cassandra" => Duration::from_secs(60),
        "dynamodb" | "mongodb" => Duration::from_secs(30),
        "duckdb" => Duration::from_secs(120),
        // Object storage / warehouses — longer TTL
        "s3" | "s3-o11y" | "csv-json" => Duration::from_secs(300),
        "athena" | "bigquery" | "snowflake" | "spark" => Duration::from_secs(300),
        "delta-lake" | "iceberg" => Duration::from_secs(300),
        // Network protocols
        "arrow-flight" => Duration::from_secs(60),
        "fuse" => Duration::from_secs(30),
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

/// Thread-safe query result cache with LRU eviction and memory budget.
pub struct QueryCache {
    inner: RwLock<CacheInner>,
}

struct CacheInner {
    entries: HashMap<u64, CachedResult>,
    /// LRU order: front = oldest access, back = most recent.
    lru: VecDeque<u64>,
    max_entries: usize,
    max_bytes: usize,
    total_bytes: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl CacheInner {
    fn touch(&mut self, key: u64) {
        self.lru.retain(|k| *k != key);
        self.lru.push_back(key);
    }

    fn evict_one_lru(&mut self) -> bool {
        if let Some(oldest) = self.lru.pop_front() {
            if let Some(removed) = self.entries.remove(&oldest) {
                self.total_bytes = self.total_bytes.saturating_sub(removed.estimated_bytes);
                self.evictions += 1;
                return true;
            }
        }
        false
    }

    fn evict_until_fits(&mut self, new_bytes: usize) {
        // Evict for entry count
        while self.entries.len() >= self.max_entries {
            if !self.evict_one_lru() {
                break;
            }
        }
        // Evict for memory budget
        while self.total_bytes + new_bytes > self.max_bytes && !self.entries.is_empty() {
            if !self.evict_one_lru() {
                break;
            }
        }
    }
}

impl QueryCache {
    pub fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_ENTRIES, DEFAULT_MAX_BYTES)
    }

    pub fn with_capacity(max_entries: usize) -> Self {
        Self::with_limits(max_entries, DEFAULT_MAX_BYTES)
    }

    pub fn with_limits(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            inner: RwLock::new(CacheInner {
                entries: HashMap::new(),
                lru: VecDeque::new(),
                max_entries: max_entries.max(1),
                max_bytes: max_bytes.max(1),
                total_bytes: 0,
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
                inner.touch(key);
                Some(batches)
            }
            Some(_) => {
                let removed = inner.entries.remove(&key).unwrap();
                inner.total_bytes = inner.total_bytes.saturating_sub(removed.estimated_bytes);
                inner.lru.retain(|k| *k != key);
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

    /// Store result with given TTL. Evicts LRU entries if at capacity or over memory budget.
    pub fn put(&self, key: u64, batches: Vec<RecordBatch>, ttl: Duration) {
        let new_bytes = batches_bytes(&batches);
        let mut inner = self.inner.write().unwrap();

        // Remove old entry if overwriting
        if let Some(old) = inner.entries.remove(&key) {
            inner.total_bytes = inner.total_bytes.saturating_sub(old.estimated_bytes);
        } else {
            inner.evict_until_fits(new_bytes);
        }

        inner.total_bytes += new_bytes;
        inner.entries.insert(
            key,
            CachedResult {
                batches,
                created: Instant::now(),
                ttl,
                estimated_bytes: new_bytes,
            },
        );
        inner.touch(key);
    }

    /// Remove all expired entries.
    pub fn evict_expired(&self) -> usize {
        let mut inner = self.inner.write().unwrap();
        let before = inner.entries.len();
        let expired_keys: Vec<u64> = inner
            .entries
            .iter()
            .filter(|(_, v)| v.is_expired())
            .map(|(k, _)| *k)
            .collect();
        for k in &expired_keys {
            if let Some(removed) = inner.entries.remove(k) {
                inner.total_bytes = inner.total_bytes.saturating_sub(removed.estimated_bytes);
            }
        }
        inner.lru.retain(|k| !expired_keys.contains(k));
        let removed = before - inner.entries.len();
        inner.evictions += removed as u64;
        removed
    }

    /// Clear all entries.
    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.entries.clear();
        inner.lru.clear();
        inner.total_bytes = 0;
    }

    /// Return cache statistics.
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.read().unwrap();
        CacheStats {
            entries: inner.entries.len(),
            hits: inner.hits,
            misses: inner.misses,
            evictions: inner.evictions,
            max_entries: inner.max_entries,
            total_bytes: inner.total_bytes,
            max_bytes: inner.max_bytes,
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

    fn big_batch(rows: usize) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let data: Vec<i64> = (0..rows as i64).collect();
        vec![
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(data))]).unwrap(),
        ]
    }

    #[test]
    fn test_default_ttl_known_types() {
        assert_eq!(default_ttl("opensearch"), Duration::from_secs(30));
        assert_eq!(default_ttl("s3"), Duration::from_secs(300));
        assert_eq!(default_ttl("prometheus"), Duration::from_secs(60));
    }

    #[test]
    fn test_default_ttl_all_connectors() {
        let connectors = [
            "opensearch", "elasticsearch", "postgres", "mysql", "dynamodb",
            "s3", "s3-o11y", "prometheus", "cloudwatch", "redis", "csv-json",
            "mongodb", "influxdb", "clickhouse", "kafka", "athena", "timestream",
            "snowflake", "bigquery", "cassandra", "duckdb", "arrow-flight",
            "fuse", "delta-lake", "iceberg",
        ];
        for c in connectors {
            let ttl = default_ttl(c);
            assert!(ttl.as_secs() > 0, "connector {c} should have a positive TTL");
        }
    }

    #[test]
    fn test_default_ttl_unknown_type() {
        assert_eq!(default_ttl("unknown_db"), Duration::from_secs(30));
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
        assert_eq!(s.max_entries, DEFAULT_MAX_ENTRIES);
        assert_eq!(s.total_bytes, 0);
        assert_eq!(s.max_bytes, DEFAULT_MAX_BYTES);
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
        assert_eq!(s.total_bytes, 0);
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
        assert_eq!(cache.stats().total_bytes, 0);
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

    #[test]
    fn test_lru_eviction_at_capacity() {
        let cache = QueryCache::with_capacity(3);
        cache.put(1, test_batch(1), Duration::from_secs(60));
        cache.put(2, test_batch(2), Duration::from_secs(60));
        cache.put(3, test_batch(3), Duration::from_secs(60));

        // Cache full — inserting 4 should evict key 1 (oldest)
        cache.put(4, test_batch(4), Duration::from_secs(60));

        assert!(cache.get(1).is_none(), "key 1 should be evicted");
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
        assert!(cache.get(4).is_some());
        assert_eq!(cache.stats().entries, 3);
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn test_lru_access_refreshes_order() {
        let cache = QueryCache::with_capacity(3);
        cache.put(1, test_batch(1), Duration::from_secs(60));
        cache.put(2, test_batch(2), Duration::from_secs(60));
        cache.put(3, test_batch(3), Duration::from_secs(60));

        // Access key 1 — moves it to most-recent
        cache.get(1);

        // Insert key 4 — should evict key 2 (now oldest)
        cache.put(4, test_batch(4), Duration::from_secs(60));

        assert!(cache.get(1).is_some(), "key 1 was accessed, should survive");
        assert!(cache.get(2).is_none(), "key 2 should be evicted");
        assert!(cache.get(3).is_some());
        assert!(cache.get(4).is_some());
    }

    #[test]
    fn test_lru_overwrite_does_not_evict() {
        let cache = QueryCache::with_capacity(2);
        cache.put(1, test_batch(1), Duration::from_secs(60));
        cache.put(2, test_batch(2), Duration::from_secs(60));

        // Overwrite key 1 — should NOT evict anything
        cache.put(1, test_batch(10), Duration::from_secs(60));

        assert_eq!(cache.stats().entries, 2);
        assert_eq!(cache.stats().evictions, 0);
        let batches = cache.get(1).unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 10);
    }

    #[test]
    fn test_with_capacity_minimum_one() {
        let cache = QueryCache::with_capacity(0);
        assert_eq!(cache.stats().max_entries, 1);
    }

    #[test]
    fn test_memory_tracking_increases_on_put() {
        let cache = QueryCache::new();
        assert_eq!(cache.stats().total_bytes, 0);

        cache.put(1, test_batch(1), Duration::from_secs(60));
        let after = cache.stats().total_bytes;
        assert!(after > 0, "should track memory after put");

        cache.put(2, test_batch(2), Duration::from_secs(60));
        assert!(cache.stats().total_bytes > after, "should increase with more entries");
    }

    #[test]
    fn test_memory_tracking_decreases_on_eviction() {
        // Tiny byte budget forces eviction of large entries
        let small_budget = 64; // very small
        let cache = QueryCache::with_limits(100, small_budget);

        cache.put(1, big_batch(100), Duration::from_secs(60));
        // Entry exceeds budget, but we allow at least 1 entry
        assert_eq!(cache.stats().entries, 1);

        // Second put should evict the first to stay within budget
        cache.put(2, big_batch(100), Duration::from_secs(60));
        assert_eq!(cache.stats().entries, 1);
        assert!(cache.get(1).is_none(), "key 1 should be evicted for memory");
        assert!(cache.get(2).is_some());
    }

    #[test]
    fn test_memory_tracking_overwrite_updates_bytes() {
        let cache = QueryCache::new();
        cache.put(1, test_batch(1), Duration::from_secs(60));
        let bytes_small = cache.stats().total_bytes;

        cache.put(1, big_batch(1000), Duration::from_secs(60));
        let bytes_big = cache.stats().total_bytes;
        assert!(bytes_big > bytes_small, "overwrite with bigger data should increase bytes");
    }

    #[test]
    fn test_memory_tracking_clear_resets_bytes() {
        let cache = QueryCache::new();
        cache.put(1, big_batch(1000), Duration::from_secs(60));
        assert!(cache.stats().total_bytes > 0);

        cache.clear();
        assert_eq!(cache.stats().total_bytes, 0);
    }

    #[test]
    fn test_batches_bytes_helper() {
        let b = test_batch(1);
        assert!(batches_bytes(&b) > 0);

        let big = big_batch(10_000);
        assert!(batches_bytes(&big) > batches_bytes(&b));
    }
}
