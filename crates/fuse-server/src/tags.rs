// SPDX-License-Identifier: Apache-2.0
//! Query tags — user-defined labels for query organization.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

pub struct TagRegistry {
    /// query_id -> set of tags
    tags: Mutex<HashMap<String, HashSet<String>>>,
}

impl Default for TagRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TagRegistry {
    pub fn new() -> Self {
        Self {
            tags: Mutex::new(HashMap::new()),
        }
    }

    pub fn tag(&self, query_id: &str, tag: &str) {
        self.tags
            .lock()
            .unwrap()
            .entry(query_id.to_string())
            .or_default()
            .insert(tag.to_string());
    }

    pub fn untag(&self, query_id: &str, tag: &str) {
        if let Some(tags) = self.tags.lock().unwrap().get_mut(query_id) {
            tags.remove(tag);
        }
    }

    pub fn get_tags(&self, query_id: &str) -> Vec<String> {
        self.tags
            .lock()
            .unwrap()
            .get(query_id)
            .map(|t| t.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn find_by_tag(&self, tag: &str) -> Vec<String> {
        self.tags
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, tags)| tags.contains(tag))
            .map(|(id, _)| id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_and_get() {
        let r = TagRegistry::new();
        r.tag("q-1", "production");
        r.tag("q-1", "slow");
        let tags = r.get_tags("q-1");
        assert_eq!(tags.len(), 2);
    }

    #[test]
    fn test_untag() {
        let r = TagRegistry::new();
        r.tag("q-1", "x");
        r.untag("q-1", "x");
        assert!(r.get_tags("q-1").is_empty());
    }

    #[test]
    fn test_find_by_tag() {
        let r = TagRegistry::new();
        r.tag("q-1", "slow");
        r.tag("q-2", "slow");
        r.tag("q-3", "fast");
        assert_eq!(r.find_by_tag("slow").len(), 2);
    }

    #[test]
    fn test_unknown_query() {
        let r = TagRegistry::new();
        assert!(r.get_tags("missing").is_empty());
    }
}
