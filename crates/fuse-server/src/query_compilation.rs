// SPDX-License-Identifier: Apache-2.0

//! Query compilation cache (#1851).
//!
//! Caches parsed SQL/PPL → resolved datasource references and query metadata,
//! keyed by fingerprint so structurally identical queries (differing only in
//! literal values) share a compiled entry and skip re-parsing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;

/// Compiled representation of a query — everything needed to route execution
/// without re-parsing the SQL/PPL text.
#[derive(Clone, Debug, Serialize)]
pub struct CompiledQuery {
    pub sources: Vec<(String, String)>,
    pub is_union: bool,
    pub is_join: bool,
    pub is_distinct: bool,
    pub limit: Option<usize>,
    pub offset: usize,
    pub order_by: Vec<(String, bool)>,
    pub group_by: Vec<String>,
    pub has_having: bool,
    pub has_subquery: bool,
    pub fingerprint: String,
    #[serde(skip)]
    compiled_at: Instant,
}

impl CompiledQuery {
    pub fn new(
        sources: Vec<(String, String)>,
        is_union: bool,
        is_join: bool,
        is_distinct: bool,
        limit: Option<usize>,
        offset: usize,
        order_by: Vec<(String, bool)>,
        group_by: Vec<String>,
        has_having: bool,
        has_subquery: bool,
        fingerprint: String,
    ) -> Self {
        Self {
            sources, is_union, is_join, is_distinct, limit, offset,
            order_by, group_by, has_having, has_subquery, fingerprint,
            compiled_at: Instant::now(),
        }
    }
}

/// Thread-safe compilation cache with TTL and LRU-style eviction.
pub struct CompilationCache {
    entries: Mutex<HashMap<String, CompiledQuery>>,
    ttl: Duration,
    max_size: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    compilations: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompilationStats {
    pub cached: usize,
    pub hits: u64,
    pub misses: u64,
    pub compilations: u64,
}

impl CompilationCache {
    pub fn new(ttl_secs: u64, max_size: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
            max_size,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            compilations: AtomicU64::new(0),
        }
    }

    pub fn get(&self, fingerprint: &str) -> Option<CompiledQuery> {
        let entries = self.entries.lock().unwrap();
        match entries.get(fingerprint) {
            Some(entry) if entry.compiled_at.elapsed() < self.ttl => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.clone())
            }
            _ => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn insert(&self, fingerprint: String, compiled: CompiledQuery) {
        self.compilations.fetch_add(1, Ordering::Relaxed);
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= self.max_size {
            let ttl = self.ttl;
            entries.retain(|_, v| v.compiled_at.elapsed() < ttl);
            if entries.len() >= self.max_size {
                if let Some(oldest) = entries
                    .iter()
                    .max_by_key(|(_, v)| v.compiled_at.elapsed())
                    .map(|(k, _)| k.clone())
                {
                    entries.remove(&oldest);
                }
            }
        }
        entries.insert(fingerprint, compiled);
    }

    pub fn invalidate_datasource(&self, datasource: &str) {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, v| !v.sources.iter().any(|(ds, _)| ds == datasource));
    }

    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().unwrap().is_empty()
    }

    pub fn stats(&self) -> CompilationStats {
        CompilationStats {
            cached: self.len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            compilations: self.compilations.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(fp: &str) -> CompiledQuery {
        CompiledQuery::new(
            vec![("ds".into(), "t".into())], false, false, false,
            Some(100), 0, vec![], vec![], false, false, fp.into(),
        )
    }

    #[test]
    fn test_cache_hit() {
        let c = CompilationCache::new(60, 100);
        c.insert("fp1".into(), sample("fp1"));
        assert!(c.get("fp1").is_some());
        assert_eq!(c.stats().hits, 1);
    }

    #[test]
    fn test_cache_miss() {
        let c = CompilationCache::new(60, 100);
        assert!(c.get("missing").is_none());
        assert_eq!(c.stats().misses, 1);
    }

    #[test]
    fn test_ttl_expiry() {
        let c = CompilationCache::new(0, 100);
        c.insert("fp".into(), sample("fp"));
        std::thread::sleep(Duration::from_millis(10));
        assert!(c.get("fp").is_none());
    }

    #[test]
    fn test_eviction_at_capacity() {
        let c = CompilationCache::new(60, 2);
        c.insert("a".into(), sample("a"));
        c.insert("b".into(), sample("b"));
        c.insert("c".into(), sample("c"));
        assert!(c.len() <= 2);
    }

    #[test]
    fn test_invalidate_datasource() {
        let c = CompilationCache::new(60, 100);
        c.insert("q1".into(), CompiledQuery::new(
            vec![("cluster_a".into(), "logs".into())],
            false, false, false, None, 0, vec![], vec![], false, false, "q1".into(),
        ));
        c.insert("q2".into(), CompiledQuery::new(
            vec![("dynamodb".into(), "users".into())],
            false, false, false, None, 0, vec![], vec![], false, false, "q2".into(),
        ));
        assert_eq!(c.len(), 2);
        c.invalidate_datasource("cluster_a");
        assert_eq!(c.len(), 1);
        assert!(c.get("q2").is_some());
    }

    #[test]
    fn test_clear() {
        let c = CompilationCache::new(60, 100);
        c.insert("a".into(), sample("a"));
        c.insert("b".into(), sample("b"));
        c.clear();
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn test_stats_tracking() {
        let c = CompilationCache::new(60, 100);
        c.insert("fp".into(), sample("fp"));
        c.get("fp"); // hit
        c.get("fp"); // hit
        c.get("nope"); // miss
        let s = c.stats();
        assert_eq!(s.compilations, 1);
        assert_eq!(s.hits, 2);
        assert_eq!(s.misses, 1);
    }

    #[test]
    fn test_compiled_query_fields() {
        let cq = CompiledQuery::new(
            vec![("a".into(), "t1".into()), ("b".into(), "t2".into())],
            true, false, true, Some(50), 10,
            vec![("col".into(), true)], vec!["g".into()],
            true, true, "fp".into(),
        );
        assert!(cq.is_union);
        assert!(cq.is_distinct);
        assert!(!cq.is_join);
        assert_eq!(cq.limit, Some(50));
        assert_eq!(cq.offset, 10);
        assert_eq!(cq.group_by, vec!["g"]);
        assert!(cq.has_having);
        assert!(cq.has_subquery);
        assert_eq!(cq.sources.len(), 2);
    }

    #[test]
    fn test_same_fingerprint_overwrites() {
        let c = CompilationCache::new(60, 100);
        let mut cq1 = sample("fp");
        cq1.limit = Some(10);
        c.insert("fp".into(), cq1);
        let mut cq2 = sample("fp");
        cq2.limit = Some(20);
        c.insert("fp".into(), cq2);
        assert_eq!(c.len(), 1);
        assert_eq!(c.get("fp").unwrap().limit, Some(20));
    }
}
