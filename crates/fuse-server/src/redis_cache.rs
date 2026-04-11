// SPDX-License-Identifier: Apache-2.0
//! Redis-backed query result cache for horizontal scaling.
//!
//! When `FUSE_REDIS_URL` is set, results are cached in Redis with TTL.
//! Falls back to in-memory cache otherwise.

use std::sync::Arc;
use tracing::{debug, warn};

use crate::plan_cache::ResultCache as InMemoryResultCache;

/// Unified result cache — Redis or in-memory.
#[derive(Clone)]
pub enum RedisResultCache {
    Redis {
        client: redis::Client,
        ttl_secs: u64,
        prefix: String,
    },
    InMemory(Arc<InMemoryResultCache>),
}

impl RedisResultCache {
    /// Create from environment. Uses Redis if `FUSE_REDIS_URL` is set.
    pub fn from_env(ttl_secs: u64, max_in_memory: usize) -> Self {
        match std::env::var("FUSE_REDIS_URL") {
            Ok(url) => match redis::Client::open(url.as_str()) {
                Ok(client) => {
                    debug!("Result cache: Redis at {}", url);
                    Self::Redis { client, ttl_secs, prefix: "fuse:cache:".into() }
                }
                Err(e) => {
                    warn!("Redis connect failed ({}), falling back to in-memory", e);
                    Self::InMemory(Arc::new(InMemoryResultCache::new(ttl_secs, max_in_memory)))
                }
            },
            Err(_) => {
                debug!("Result cache: in-memory (set FUSE_REDIS_URL for Redis)");
                Self::InMemory(Arc::new(InMemoryResultCache::new(ttl_secs, max_in_memory)))
            }
        }
    }

    /// Get cached result by key.
    pub async fn get(&self, key: &str) -> Option<serde_json::Value> {
        match self {
            Self::Redis { client, prefix, .. } => {
                let mut conn = client.get_multiplexed_async_connection().await.ok()?;
                let data: Option<String> = redis::cmd("GET")
                    .arg(format!("{prefix}{key}"))
                    .query_async(&mut conn)
                    .await
                    .ok()?;
                data.and_then(|s| serde_json::from_str(&s).ok())
            }
            Self::InMemory(cache) => cache.get(key).map(|c| c.response_json),
        }
    }

    /// Insert result with TTL.
    pub async fn insert(&self, key: String, value: serde_json::Value) {
        match self {
            Self::Redis { client, ttl_secs, prefix } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return };
                let Ok(json) = serde_json::to_string(&value) else { return };
                let _: Result<(), _> = redis::cmd("SET")
                    .arg(format!("{prefix}{key}"))
                    .arg(json)
                    .arg("EX")
                    .arg(*ttl_secs)
                    .query_async(&mut conn)
                    .await;
            }
            Self::InMemory(cache) => cache.insert(key, value),
        }
    }

    /// Invalidate a specific key.
    pub async fn invalidate(&self, key: &str) {
        match self {
            Self::Redis { client, prefix, .. } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return };
                let _: Result<(), _> = redis::cmd("DEL")
                    .arg(format!("{prefix}{key}"))
                    .query_async(&mut conn)
                    .await;
            }
            Self::InMemory(_) => {} // in-memory relies on TTL expiry
        }
    }

    /// Check if using Redis backend.
    pub fn is_redis(&self) -> bool {
        matches!(self, Self::Redis { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_env_no_redis_url() {
        std::env::remove_var("FUSE_REDIS_URL");
        let cache = RedisResultCache::from_env(60, 100);
        assert!(!cache.is_redis());
    }

    #[test]
    fn test_from_env_invalid_redis_url() {
        std::env::set_var("FUSE_REDIS_URL", "not-a-url");
        let cache = RedisResultCache::from_env(60, 100);
        // Invalid URL → falls back to in-memory
        assert!(!cache.is_redis());
        std::env::remove_var("FUSE_REDIS_URL");
    }

    #[test]
    fn test_from_env_valid_redis_url() {
        std::env::set_var("FUSE_REDIS_URL", "redis://127.0.0.1:6379");
        let cache = RedisResultCache::from_env(60, 100);
        assert!(cache.is_redis());
        std::env::remove_var("FUSE_REDIS_URL");
    }

    #[tokio::test]
    async fn test_in_memory_get_miss() {
        std::env::remove_var("FUSE_REDIS_URL");
        let cache = RedisResultCache::from_env(60, 100);
        assert!(cache.get("missing").await.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_insert_and_get() {
        std::env::remove_var("FUSE_REDIS_URL");
        let cache = RedisResultCache::from_env(60, 100);
        cache.insert("k1".into(), serde_json::json!({"rows": [1,2,3]})).await;
        let val = cache.get("k1").await;
        assert!(val.is_some());
        assert_eq!(val.unwrap()["rows"], serde_json::json!([1,2,3]));
    }

    #[test]
    fn test_is_redis() {
        std::env::remove_var("FUSE_REDIS_URL");
        let mem = RedisResultCache::from_env(60, 100);
        assert!(!mem.is_redis());
    }
}
