// SPDX-License-Identifier: Apache-2.0
//! Adaptive Query Caching (#1821)
//!
//! Learn repeat query patterns and auto-cache with per-datasource TTL.
//! Tracks query frequency via fingerprints; promotes hot queries to cache
//! automatically. Builds on plan_cache + result_cache.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;

/// Per-datasource TTL configuration.
#[derive(Debug, Clone)]
pub struct DatasourceTtl {
    pub datasource: String,
    pub ttl: Duration,
}

/// Tracks query frequency and decides what to auto-cache.
pub struct AdaptiveCache {
    /// Query fingerprint → access count + last seen.
    frequency: Mutex<HashMap<String, QueryFreq>>,
    /// Per-datasource TTL overrides. Missing = default_ttl.
    datasource_ttls: HashMap<String, Duration>,
    /// Default TTL for datasources without explicit config.
    default_ttl: Duration,
    /// Minimum hits before a query is promoted to auto-cache.
    promotion_threshold: u32,
    /// Max tracked fingerprints (evict coldest when full).
    max_tracked: usize,
}

#[derive(Debug, Clone)]
struct QueryFreq {
    count: u32,
    last_seen: Instant,
    datasources: Vec<String>,
}

/// Stats about the adaptive cache state.
#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveCacheStats {
    pub tracked_queries: usize,
    pub hot_queries: usize,
    pub promotion_threshold: u32,
    pub datasource_ttls: HashMap<String, u64>,
}

impl AdaptiveCache {
    pub fn new(default_ttl_secs: u64, promotion_threshold: u32, max_tracked: usize) -> Self {
        Self {
            frequency: Mutex::new(HashMap::new()),
            datasource_ttls: HashMap::new(),
            default_ttl: Duration::from_secs(default_ttl_secs),
            promotion_threshold,
            max_tracked,
        }
    }

    /// Set per-datasource TTL.
    pub fn set_datasource_ttl(&mut self, datasource: &str, ttl_secs: u64) {
        self.datasource_ttls.insert(datasource.to_string(), Duration::from_secs(ttl_secs));
    }

    /// Record a query access. Returns true if the query should be cached (hot).
    pub fn record(&self, fingerprint: &str, datasources: &[String]) -> bool {
        let mut freq = self.frequency.lock().unwrap();

        if let Some(entry) = freq.get_mut(fingerprint) {
            entry.count += 1;
            entry.last_seen = Instant::now();
            return entry.count >= self.promotion_threshold;
        }

        // Evict coldest if at capacity
        if freq.len() >= self.max_tracked {
            if let Some(coldest) = freq.iter()
                .min_by_key(|(_, v)| v.count)
                .map(|(k, _)| k.clone())
            {
                freq.remove(&coldest);
            }
        }

        freq.insert(fingerprint.to_string(), QueryFreq {
            count: 1,
            last_seen: Instant::now(),
            datasources: datasources.to_vec(),
        });

        1 >= self.promotion_threshold
    }

    /// Get the effective TTL for a query based on its datasources.
    /// Uses the minimum TTL across all referenced datasources.
    pub fn effective_ttl(&self, datasources: &[String]) -> Duration {
        if datasources.is_empty() {
            return self.default_ttl;
        }
        datasources.iter()
            .map(|ds| self.datasource_ttls.get(ds).copied().unwrap_or(self.default_ttl))
            .min()
            .unwrap_or(self.default_ttl)
    }

    /// Check if a query fingerprint is hot (above promotion threshold).
    pub fn is_hot(&self, fingerprint: &str) -> bool {
        let freq = self.frequency.lock().unwrap();
        freq.get(fingerprint).map(|f| f.count >= self.promotion_threshold).unwrap_or(false)
    }

    /// Get access count for a fingerprint.
    pub fn access_count(&self, fingerprint: &str) -> u32 {
        let freq = self.frequency.lock().unwrap();
        freq.get(fingerprint).map(|f| f.count).unwrap_or(0)
    }

    /// Get stats for observability.
    pub fn stats(&self) -> AdaptiveCacheStats {
        let freq = self.frequency.lock().unwrap();
        let hot = freq.values().filter(|f| f.count >= self.promotion_threshold).count();
        AdaptiveCacheStats {
            tracked_queries: freq.len(),
            hot_queries: hot,
            promotion_threshold: self.promotion_threshold,
            datasource_ttls: self.datasource_ttls.iter()
                .map(|(k, v)| (k.clone(), v.as_secs()))
                .collect(),
        }
    }

    /// Reset all frequency tracking.
    pub fn reset(&self) {
        self.frequency.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_first_access_not_hot() {
        let ac = AdaptiveCache::new(60, 3, 100);
        assert!(!ac.record("fp1", &["ds1".into()]));
        assert!(!ac.is_hot("fp1"));
    }

    #[test]
    fn test_promotion_at_threshold() {
        let ac = AdaptiveCache::new(60, 3, 100);
        let ds = vec!["ds1".into()];
        ac.record("fp1", &ds);
        ac.record("fp1", &ds);
        assert!(ac.record("fp1", &ds)); // 3rd hit = promoted
        assert!(ac.is_hot("fp1"));
    }

    #[test]
    fn test_access_count() {
        let ac = AdaptiveCache::new(60, 5, 100);
        let ds = vec!["ds1".into()];
        ac.record("fp1", &ds);
        ac.record("fp1", &ds);
        assert_eq!(ac.access_count("fp1"), 2);
        assert_eq!(ac.access_count("unknown"), 0);
    }

    #[test]
    fn test_eviction_at_capacity() {
        let ac = AdaptiveCache::new(60, 3, 2);
        ac.record("fp1", &["ds1".into()]);
        ac.record("fp2", &["ds1".into()]);
        ac.record("fp3", &["ds1".into()]); // evicts coldest
        let stats = ac.stats();
        assert!(stats.tracked_queries <= 2);
    }

    #[test]
    fn test_default_ttl() {
        let ac = AdaptiveCache::new(30, 3, 100);
        assert_eq!(ac.effective_ttl(&["ds1".into()]), Duration::from_secs(30));
    }

    #[test]
    fn test_per_datasource_ttl() {
        let mut ac = AdaptiveCache::new(60, 3, 100);
        ac.set_datasource_ttl("fast_ds", 10);
        ac.set_datasource_ttl("slow_ds", 300);
        assert_eq!(ac.effective_ttl(&["fast_ds".into()]), Duration::from_secs(10));
        assert_eq!(ac.effective_ttl(&["slow_ds".into()]), Duration::from_secs(300));
    }

    #[test]
    fn test_cross_source_uses_min_ttl() {
        let mut ac = AdaptiveCache::new(60, 3, 100);
        ac.set_datasource_ttl("fast", 10);
        ac.set_datasource_ttl("slow", 300);
        // Cross-source query uses the minimum TTL
        assert_eq!(ac.effective_ttl(&["fast".into(), "slow".into()]), Duration::from_secs(10));
    }

    #[test]
    fn test_empty_datasources_uses_default() {
        let ac = AdaptiveCache::new(45, 3, 100);
        assert_eq!(ac.effective_ttl(&[]), Duration::from_secs(45));
    }

    #[test]
    fn test_stats() {
        let ac = AdaptiveCache::new(60, 2, 100);
        let ds = vec!["ds1".into()];
        ac.record("fp1", &ds);
        ac.record("fp1", &ds); // promoted
        ac.record("fp2", &ds); // not yet
        let stats = ac.stats();
        assert_eq!(stats.tracked_queries, 2);
        assert_eq!(stats.hot_queries, 1);
        assert_eq!(stats.promotion_threshold, 2);
    }

    #[test]
    fn test_reset() {
        let ac = AdaptiveCache::new(60, 3, 100);
        ac.record("fp1", &["ds1".into()]);
        ac.reset();
        assert_eq!(ac.access_count("fp1"), 0);
        assert_eq!(ac.stats().tracked_queries, 0);
    }

    #[test]
    fn test_threshold_one_always_hot() {
        let ac = AdaptiveCache::new(60, 1, 100);
        assert!(ac.record("fp1", &["ds1".into()])); // first access = hot
    }
}
