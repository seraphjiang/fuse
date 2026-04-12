// SPDX-License-Identifier: Apache-2.0
//! Query templates — parameterized queries for reuse.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTemplate {
    pub name: String,
    pub template: String,
    pub params: Vec<String>,
    #[serde(default)]
    pub description: Option<String>,
}

impl QueryTemplate {
    /// Render the template with provided parameter values.
    ///
    /// # Security Warning
    /// Values are substituted as raw strings — NO escaping is applied.
    /// For user-facing inputs, use prepared statements ($1 params) instead
    /// of templates. Templates are intended for admin/power-user use where
    /// parameter values come from trusted sources (config, internal systems).
    pub fn render(&self, values: &HashMap<String, String>) -> Result<String, String> {
        let mut result = self.template.clone();
        for param in &self.params {
            let placeholder = format!("{{{{{}}}}}", param);
            match values.get(param) {
                Some(val) => result = result.replace(&placeholder, val),
                None => return Err(format!("missing parameter: {}", param)),
            }
        }
        Ok(result)
    }
}

pub struct TemplateStore {
    templates: Mutex<HashMap<String, QueryTemplate>>,
}

impl Default for TemplateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl TemplateStore {
    pub fn new() -> Self {
        Self {
            templates: Mutex::new(HashMap::new()),
        }
    }

    pub fn save(&self, template: QueryTemplate) {
        self.templates
            .lock()
            .unwrap()
            .insert(template.name.clone(), template);
    }

    pub fn get(&self, name: &str) -> Option<QueryTemplate> {
        self.templates.lock().unwrap().get(name).cloned()
    }

    pub fn list(&self) -> Vec<QueryTemplate> {
        self.templates.lock().unwrap().values().cloned().collect()
    }

    pub fn delete(&self, name: &str) -> bool {
        self.templates.lock().unwrap().remove(name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render() {
        let t = QueryTemplate {
            name: "errors".into(),
            template: "SELECT * FROM {{ds}}.logs WHERE status >= {{min_status}} LIMIT {{limit}}"
                .into(),
            params: vec!["ds".into(), "min_status".into(), "limit".into()],
            description: None,
        };
        let mut vals = HashMap::new();
        vals.insert("ds".into(), "cluster_a".into());
        vals.insert("min_status".into(), "500".into());
        vals.insert("limit".into(), "100".into());
        let sql = t.render(&vals).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM cluster_a.logs WHERE status >= 500 LIMIT 100"
        );
    }

    #[test]
    fn test_missing_param() {
        let t = QueryTemplate {
            name: "t".into(),
            template: "SELECT {{x}}".into(),
            params: vec!["x".into()],
            description: None,
        };
        assert!(t.render(&HashMap::new()).is_err());
    }

    #[test]
    fn test_store() {
        let store = TemplateStore::new();
        store.save(QueryTemplate {
            name: "t1".into(),
            template: "SELECT 1".into(),
            params: vec![],
            description: None,
        });
        assert!(store.get("t1").is_some());
        assert_eq!(store.list().len(), 1);
        assert!(store.delete("t1"));
        assert!(store.get("t1").is_none());
    }
}
