// SPDX-License-Identifier: Apache-2.0

//! Natural language to SQL — LLM-powered query generation (#1500).
//!
//! Accepts a natural language question, builds a schema-aware prompt,
//! sends it to an LLM backend (Bedrock or OpenAI-compatible), and
//! returns the generated SQL along with optional execution results.
//!
//! # Endpoint
//!
//! `POST /api/fuse/query/nl`
//!
//! ```json
//! { "question": "show me the top 10 error logs from yesterday",
//!   "datasources": ["cluster_a"],
//!   "execute": true }
//! ```
//!
//! # Configuration (fuse.toml)
//!
//! ```toml
//! [engine.nl]
//! enabled = true
//! backend = "bedrock"  # or "openai"
//! model = "anthropic.claude-3-haiku-20240307-v1:0"
//! # For OpenAI-compatible:
//! # api_url = "https://api.openai.com/v1/chat/completions"
//! # api_key = "secret://fuse/openai-key"
//! ```

use serde::{Deserialize, Serialize};

/// NL query configuration.
#[derive(Debug, Clone)]
pub struct NlConfig {
    pub enabled: bool,
    pub backend: NlBackend,
    pub model: String,
    pub api_url: Option<String>,
    pub api_key: Option<String>,
    pub max_tokens: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NlBackend {
    Bedrock,
    OpenAi,
}

impl Default for NlConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: NlBackend::Bedrock,
            model: "anthropic.claude-3-haiku-20240307-v1:0".into(),
            api_url: None,
            api_key: None,
            max_tokens: 1024,
        }
    }
}

impl NlConfig {
    pub fn from_toml(table: Option<&toml::Value>) -> Self {
        let Some(t) = table.and_then(|v| v.as_table()) else {
            return Self::default();
        };
        let backend = match t.get("backend").and_then(|v| v.as_str()) {
            Some("openai") => NlBackend::OpenAi,
            _ => NlBackend::Bedrock,
        };
        Self {
            enabled: t.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false),
            backend,
            model: t.get("model").and_then(|v| v.as_str())
                .unwrap_or("anthropic.claude-3-haiku-20240307-v1:0").into(),
            api_url: t.get("api_url").and_then(|v| v.as_str()).map(|s| s.into()),
            api_key: t.get("api_key").and_then(|v| v.as_str()).map(|s| s.into()),
            max_tokens: t.get("max_tokens").and_then(|v| v.as_integer()).unwrap_or(1024) as u32,
        }
    }
}

/// Request to the NL query endpoint.
#[derive(Debug, Deserialize)]
pub struct NlQueryRequest {
    pub question: String,
    /// Optional: limit schema context to these datasources.
    #[serde(default)]
    pub datasources: Vec<String>,
    /// If true, execute the generated SQL and return results.
    #[serde(default)]
    pub execute: bool,
}

/// Response from the NL query endpoint.
#[derive(Debug, Serialize)]
pub struct NlQueryResponse {
    pub question: String,
    pub generated_sql: String,
    pub explanation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Schema context for prompt construction.
#[derive(Debug, Clone)]
pub struct SchemaContext {
    pub datasource: String,
    pub table: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// Build a schema-aware system prompt for SQL generation.
pub fn build_prompt(question: &str, schemas: &[SchemaContext]) -> String {
    let mut prompt = String::from(
        "You are a SQL query generator for the Fuse federated query engine. \
         Generate a single SQL query that answers the user's question. \
         Return ONLY the SQL query, no explanation or markdown.\n\n"
    );

    if !schemas.is_empty() {
        prompt.push_str("Available tables and columns:\n\n");
        for schema in schemas {
            prompt.push_str(&format!("-- {}.{}\n", schema.datasource, schema.table));
            for col in &schema.columns {
                prompt.push_str(&format!("--   {} ({})\n", col.name, col.data_type));
            }
            prompt.push('\n');
        }
    }

    prompt.push_str(&format!("Question: {}\nSQL:", question));
    prompt
}

/// Extract SQL from LLM response (strip markdown fences, trim).
pub fn extract_sql(response: &str) -> String {
    let trimmed = response.trim();
    // Strip ```sql ... ``` fences
    if let Some(inner) = trimmed.strip_prefix("```sql") {
        if let Some(sql) = inner.strip_suffix("```") {
            return sql.trim().to_string();
        }
    }
    if let Some(inner) = trimmed.strip_prefix("```") {
        if let Some(sql) = inner.strip_suffix("```") {
            return sql.trim().to_string();
        }
    }
    trimmed.to_string()
}

/// Build the OpenAI-compatible chat completion request body.
pub fn build_openai_request(prompt: &str, model: &str, max_tokens: u32) -> serde_json::Value {
    serde_json::json!({
        "model": model,
        "messages": [
            { "role": "system", "content": "You are a SQL query generator. Return ONLY SQL, no explanation." },
            { "role": "user", "content": prompt }
        ],
        "max_tokens": max_tokens,
        "temperature": 0.0
    })
}

/// Parse the SQL from an OpenAI-compatible chat completion response.
pub fn parse_openai_response(json: &serde_json::Value) -> Option<String> {
    json["choices"][0]["message"]["content"]
        .as_str()
        .map(extract_sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = NlConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.backend, NlBackend::Bedrock);
        assert_eq!(cfg.max_tokens, 1024);
    }

    #[test]
    fn test_config_from_toml() {
        let val: toml::Value = toml::from_str(r#"
            enabled = true
            backend = "openai"
            model = "gpt-4"
            api_url = "https://api.example.com/v1/chat/completions"
            api_key = "sk-test"
            max_tokens = 2048
        "#).unwrap();
        let cfg = NlConfig::from_toml(Some(&val));
        assert!(cfg.enabled);
        assert_eq!(cfg.backend, NlBackend::OpenAi);
        assert_eq!(cfg.model, "gpt-4");
        assert_eq!(cfg.api_url.as_deref(), Some("https://api.example.com/v1/chat/completions"));
        assert_eq!(cfg.max_tokens, 2048);
    }

    #[test]
    fn test_config_from_toml_none() {
        let cfg = NlConfig::from_toml(None);
        assert!(!cfg.enabled);
    }

    #[test]
    fn test_build_prompt_with_schema() {
        let schemas = vec![SchemaContext {
            datasource: "cluster_a".into(),
            table: "logs".into(),
            columns: vec![
                ColumnInfo { name: "timestamp".into(), data_type: "Utf8".into() },
                ColumnInfo { name: "level".into(), data_type: "Utf8".into() },
            ],
        }];
        let prompt = build_prompt("show errors", &schemas);
        assert!(prompt.contains("cluster_a.logs"));
        assert!(prompt.contains("timestamp (Utf8)"));
        assert!(prompt.contains("show errors"));
    }

    #[test]
    fn test_build_prompt_no_schema() {
        let prompt = build_prompt("count all rows", &[]);
        assert!(prompt.contains("count all rows"));
        assert!(!prompt.contains("Available tables"));
    }

    #[test]
    fn test_extract_sql_plain() {
        assert_eq!(extract_sql("SELECT * FROM t"), "SELECT * FROM t");
    }

    #[test]
    fn test_extract_sql_fenced() {
        assert_eq!(extract_sql("```sql\nSELECT 1\n```"), "SELECT 1");
    }

    #[test]
    fn test_extract_sql_generic_fence() {
        assert_eq!(extract_sql("```\nSELECT 1\n```"), "SELECT 1");
    }

    #[test]
    fn test_build_openai_request() {
        let req = build_openai_request("test prompt", "gpt-4", 512);
        assert_eq!(req["model"], "gpt-4");
        assert_eq!(req["max_tokens"], 512);
        assert_eq!(req["temperature"], 0.0);
        assert_eq!(req["messages"][1]["content"], "test prompt");
    }

    #[test]
    fn test_parse_openai_response() {
        let resp = serde_json::json!({
            "choices": [{ "message": { "content": "```sql\nSELECT count(*) FROM logs\n```" } }]
        });
        assert_eq!(parse_openai_response(&resp), Some("SELECT count(*) FROM logs".into()));
    }

    #[test]
    fn test_parse_openai_response_empty() {
        let resp = serde_json::json!({ "choices": [] });
        assert_eq!(parse_openai_response(&resp), None);
    }
}
