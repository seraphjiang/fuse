// SPDX-License-Identifier: Apache-2.0
//! Connector factory registry — register factories by type name.

use std::collections::HashMap;
use std::sync::Mutex;

/// A factory function that creates a connector from config.
pub type FactoryFn = Box<dyn Fn(&toml::Value) -> Result<String, String> + Send + Sync>;

pub struct FactoryRegistry {
    factories: Mutex<HashMap<String, FactoryFn>>,
}

impl Default for FactoryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FactoryRegistry {
    pub fn new() -> Self {
        Self { factories: Mutex::new(HashMap::new()) }
    }

    pub fn register(&self, connector_type: &str, factory: FactoryFn) {
        self.factories.lock().unwrap().insert(connector_type.to_string(), factory);
    }

    pub fn has(&self, connector_type: &str) -> bool {
        self.factories.lock().unwrap().contains_key(connector_type)
    }

    pub fn types(&self) -> Vec<String> {
        self.factories.lock().unwrap().keys().cloned().collect()
    }

    pub fn count(&self) -> usize {
        self.factories.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_has() {
        let reg = FactoryRegistry::new();
        reg.register("postgres", Box::new(|_| Ok("pg".into())));
        assert!(reg.has("postgres"));
        assert!(!reg.has("mysql"));
    }

    #[test]
    fn test_types() {
        let reg = FactoryRegistry::new();
        reg.register("a", Box::new(|_| Ok("a".into())));
        reg.register("b", Box::new(|_| Ok("b".into())));
        assert_eq!(reg.count(), 2);
        assert_eq!(reg.types().len(), 2);
    }

    #[test]
    fn test_empty() {
        let reg = FactoryRegistry::new();
        assert_eq!(reg.count(), 0);
        assert!(reg.types().is_empty());
    }
}
