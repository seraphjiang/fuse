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
