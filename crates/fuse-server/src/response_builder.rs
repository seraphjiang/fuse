// SPDX-License-Identifier: Apache-2.0
//! Query response builder — standardize response construction.

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub metadata: ResponseMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct ResponseMetadata {
    pub total_rows: u64,
    pub format: String,
    pub trace_id: String,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasources_queried: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached: Option<bool>,
}

/// Builder for constructing query responses.
pub struct ResponseBuilder {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    trace_id: String,
    format: String,
    duration_ms: u64,
    datasources: Vec<String>,
    cached: bool,
    warnings: Vec<String>,
}

impl ResponseBuilder {
    pub fn new(trace_id: &str, format: &str) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            trace_id: trace_id.to_string(),
            format: format.to_string(),
            duration_ms: 0,
            datasources: Vec::new(),
            cached: false,
            warnings: Vec::new(),
        }
    }

    pub fn columns(mut self, cols: Vec<String>) -> Self {
        self.columns = cols;
        self
    }
    pub fn rows(mut self, rows: Vec<Vec<Value>>) -> Self {
        self.rows = rows;
        self
    }
    pub fn duration_ms(mut self, ms: u64) -> Self {
        self.duration_ms = ms;
        self
    }
    pub fn datasources(mut self, ds: Vec<String>) -> Self {
        self.datasources = ds;
        self
    }
    pub fn cached(mut self, c: bool) -> Self {
        self.cached = c;
        self
    }
    pub fn warning(mut self, w: &str) -> Self {
        self.warnings.push(w.to_string());
        self
    }

    pub fn build(self) -> QueryResponse {
        QueryResponse {
            metadata: ResponseMetadata {
                total_rows: self.rows.len() as u64,
                format: self.format,
                trace_id: self.trace_id,
                duration_ms: self.duration_ms,
                datasources_queried: if self.datasources.is_empty() {
                    None
                } else {
                    Some(self.datasources)
                },
                cached: if self.cached { Some(true) } else { None },
            },
            columns: self.columns,
            rows: self.rows,
            warnings: if self.warnings.is_empty() {
                None
            } else {
                Some(self.warnings)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_response() {
        let resp = ResponseBuilder::new("q-1", "sql")
            .columns(vec!["id".into()])
            .rows(vec![vec![json!(1)]])
            .duration_ms(50)
            .build();
        assert_eq!(resp.metadata.total_rows, 1);
        assert_eq!(resp.metadata.trace_id, "q-1");
        assert!(resp.warnings.is_none());
    }

    #[test]
    fn test_with_warnings() {
        let resp = ResponseBuilder::new("q-2", "sql")
            .warning("result truncated")
            .build();
        assert_eq!(resp.warnings.unwrap().len(), 1);
    }

    #[test]
    fn test_cached_response() {
        let resp = ResponseBuilder::new("q-3", "sql").cached(true).build();
        assert_eq!(resp.metadata.cached, Some(true));
    }

    #[test]
    fn test_serialization() {
        let resp = ResponseBuilder::new("q-4", "ppl")
            .columns(vec!["x".into()])
            .rows(vec![vec![json!("a")]])
            .datasources(vec!["pg".into()])
            .build();
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"trace_id\":\"q-4\""));
        assert!(json.contains("\"datasources_queried\""));
    }
}
