// SPDX-License-Identifier: Apache-2.0
//! Datasource metadata cache — cache schema discovery results.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct CachedSchema {
    pub tables: Vec<String>,
    pub fields: HashMap<String, Vec<(String, String)>>, // table -> [(name, type)]
    created: Instant,
}

pub struct MetadataCache {
    entries: Mutex<HashMap<String, CachedSchema>>,
    ttl: Duration,
}

impl MetadataCache {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    pub fn get_tables(&self, datasource: &str) -> Option<Vec<String>> {
        let entries = self.entries.lock().unwrap();
        let cached = entries.get(datasource)?;
        if cached.created.elapsed() < self.ttl {
            Some(cached.tables.clone())
        } else {
            None
        }
    }

    pub fn get_fields(&self, datasource: &str, table: &str) -> Option<Vec<(String, String)>> {
        let entries = self.entries.lock().unwrap();
        let cached = entries.get(datasource)?;
        if cached.created.elapsed() < self.ttl {
            cached.fields.get(table).cloned()
        } else {
            None
        }
    }

    pub fn set_tables(&self, datasource: &str, tables: Vec<String>) {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries
            .entry(datasource.to_string())
            .or_insert_with(|| CachedSchema {
                tables: vec![],
                fields: HashMap::new(),
                created: Instant::now(),
            });
        entry.tables = tables;
        entry.created = Instant::now();
    }

    pub fn set_fields(&self, datasource: &str, table: &str, fields: Vec<(String, String)>) {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries
            .entry(datasource.to_string())
            .or_insert_with(|| CachedSchema {
                tables: vec![],
                fields: HashMap::new(),
                created: Instant::now(),
            });
        entry.fields.insert(table.to_string(), fields);
    }

    pub fn invalidate(&self, datasource: &str) {
        self.entries.lock().unwrap().remove(datasource);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_tables() {
        let c = MetadataCache::new(60);
        c.set_tables("pg", vec!["users".into(), "orders".into()]);
        let tables = c.get_tables("pg").unwrap();
        assert_eq!(tables, vec!["users", "orders"]);
    }

    #[test]
    fn test_cache_fields() {
        let c = MetadataCache::new(60);
        c.set_fields(
            "pg",
            "users",
            vec![("id".into(), "int".into()), ("name".into(), "text".into())],
        );
        let fields = c.get_fields("pg", "users").unwrap();
        assert_eq!(fields.len(), 2);
    }

    #[test]
    fn test_cache_miss() {
        let c = MetadataCache::new(60);
        assert!(c.get_tables("unknown").is_none());
    }

    #[test]
    fn test_invalidate() {
        let c = MetadataCache::new(60);
        c.set_tables("pg", vec!["t".into()]);
        c.invalidate("pg");
        assert!(c.get_tables("pg").is_none());
    }

    #[test]
    fn test_ttl_expiry() {
        let c = MetadataCache::new(0);
        c.set_tables("pg", vec!["t".into()]);
        assert!(c.get_tables("pg").is_none());
    }
}
