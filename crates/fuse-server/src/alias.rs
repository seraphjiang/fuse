// SPDX-License-Identifier: Apache-2.0
//! Datasource aliases — short names for datasources.

use std::collections::HashMap;
use std::sync::Mutex;

pub struct AliasRegistry {
    aliases: Mutex<HashMap<String, String>>, // alias -> datasource_id
}

impl Default for AliasRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AliasRegistry {
    pub fn new() -> Self {
        Self {
            aliases: Mutex::new(HashMap::new()),
        }
    }

    pub fn set(&self, alias: &str, datasource_id: &str) {
        self.aliases
            .lock()
            .unwrap()
            .insert(alias.to_string(), datasource_id.to_string());
    }

    pub fn resolve(&self, name: &str) -> String {
        self.aliases
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    pub fn remove(&self, alias: &str) -> bool {
        self.aliases.lock().unwrap().remove(alias).is_some()
    }

    pub fn list(&self) -> Vec<(String, String)> {
        self.aliases
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_resolve() {
        let r = AliasRegistry::new();
        r.set("logs", "cluster_a");
        assert_eq!(r.resolve("logs"), "cluster_a");
    }

    #[test]
    fn test_resolve_passthrough() {
        let r = AliasRegistry::new();
        assert_eq!(r.resolve("unknown"), "unknown");
    }

    #[test]
    fn test_remove() {
        let r = AliasRegistry::new();
        r.set("x", "y");
        assert!(r.remove("x"));
        assert_eq!(r.resolve("x"), "x");
    }

    #[test]
    fn test_list() {
        let r = AliasRegistry::new();
        r.set("a", "ds1");
        r.set("b", "ds2");
        assert_eq!(r.list().len(), 2);
    }
}
