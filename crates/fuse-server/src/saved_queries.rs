// SPDX-License-Identifier: Apache-2.0
//! In-memory saved query registry — store and retrieve named queries.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQuery {
    pub name: String,
    pub query: String,
    pub format: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Default)]
pub struct SavedQueryRegistry {
    queries: Mutex<HashMap<String, SavedQuery>>,
}

impl SavedQueryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn save(&self, sq: SavedQuery) {
        self.queries.lock().unwrap().insert(sq.name.clone(), sq);
    }

    pub fn get(&self, name: &str) -> Option<SavedQuery> {
        self.queries.lock().unwrap().get(name).cloned()
    }

    pub fn delete(&self, name: &str) -> bool {
        self.queries.lock().unwrap().remove(name).is_some()
    }

    pub fn list(&self) -> Vec<SavedQuery> {
        self.queries.lock().unwrap().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sq(name: &str, query: &str) -> SavedQuery {
        SavedQuery {
            name: name.into(),
            query: query.into(),
            format: "sql".into(),
            description: String::new(),
        }
    }

    #[test]
    fn test_save_and_get() {
        let reg = SavedQueryRegistry::new();
        reg.save(sq("errors", "SELECT * FROM logs WHERE status >= 400"));
        let got = reg.get("errors").unwrap();
        assert_eq!(got.query, "SELECT * FROM logs WHERE status >= 400");
    }

    #[test]
    fn test_get_nonexistent() {
        let reg = SavedQueryRegistry::new();
        assert!(reg.get("nope").is_none());
    }

    #[test]
    fn test_delete() {
        let reg = SavedQueryRegistry::new();
        reg.save(sq("tmp", "SELECT 1"));
        assert!(reg.delete("tmp"));
        assert!(reg.get("tmp").is_none());
        assert!(!reg.delete("tmp"));
    }

    #[test]
    fn test_list() {
        let reg = SavedQueryRegistry::new();
        reg.save(sq("a", "SELECT 1"));
        reg.save(sq("b", "SELECT 2"));
        assert_eq!(reg.list().len(), 2);
    }

    #[test]
    fn test_overwrite() {
        let reg = SavedQueryRegistry::new();
        reg.save(sq("q", "SELECT 1"));
        reg.save(sq("q", "SELECT 2"));
        assert_eq!(reg.get("q").unwrap().query, "SELECT 2");
        assert_eq!(reg.list().len(), 1);
    }
}
