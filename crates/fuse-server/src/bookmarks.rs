// SPDX-License-Identifier: Apache-2.0
//! Query bookmarks — save frequently used queries with metadata.

use std::collections::HashMap;
use std::sync::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bookmark {
    pub id: String,
    pub name: String,
    pub query: String,
    pub format: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub created_at: u64,
}

pub struct BookmarkStore {
    bookmarks: Mutex<HashMap<String, Bookmark>>,
}

impl Default for BookmarkStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BookmarkStore {
    pub fn new() -> Self {
        Self { bookmarks: Mutex::new(HashMap::new()) }
    }

    pub fn save(&self, bookmark: Bookmark) {
        self.bookmarks.lock().unwrap().insert(bookmark.id.clone(), bookmark);
    }

    pub fn get(&self, id: &str) -> Option<Bookmark> {
        self.bookmarks.lock().unwrap().get(id).cloned()
    }

    pub fn delete(&self, id: &str) -> bool {
        self.bookmarks.lock().unwrap().remove(id).is_some()
    }

    pub fn list(&self) -> Vec<Bookmark> {
        self.bookmarks.lock().unwrap().values().cloned().collect()
    }

    pub fn search(&self, term: &str) -> Vec<Bookmark> {
        let lower = term.to_lowercase();
        self.bookmarks.lock().unwrap().values()
            .filter(|b| b.name.to_lowercase().contains(&lower)
                || b.query.to_lowercase().contains(&lower)
                || b.tags.iter().any(|t| t.to_lowercase().contains(&lower)))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(id: &str, name: &str) -> Bookmark {
        Bookmark { id: id.into(), name: name.into(), query: "SELECT 1".into(),
            format: "sql".into(), description: None, tags: vec![], created_at: 0 }
    }

    #[test]
    fn test_save_and_get() {
        let s = BookmarkStore::new();
        s.save(sample("b1", "My Query"));
        assert!(s.get("b1").is_some());
    }

    #[test]
    fn test_delete() {
        let s = BookmarkStore::new();
        s.save(sample("b1", "x"));
        assert!(s.delete("b1"));
        assert!(s.get("b1").is_none());
    }

    #[test]
    fn test_search() {
        let s = BookmarkStore::new();
        s.save(Bookmark { tags: vec!["prod".into()], ..sample("b1", "Error Logs") });
        s.save(sample("b2", "User Stats"));
        assert_eq!(s.search("error").len(), 1);
        assert_eq!(s.search("prod").len(), 1);
    }

    #[test]
    fn test_list() {
        let s = BookmarkStore::new();
        s.save(sample("a", "x"));
        s.save(sample("b", "y"));
        assert_eq!(s.list().len(), 2);
    }
}
