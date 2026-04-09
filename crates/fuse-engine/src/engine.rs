// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use fuse_core::connector::ResultSet;
use fuse_core::error::FuseError;
use fuse_core::registry::ConnectorRegistry;

/// The federated query engine. Parses queries, plans execution across
/// connectors, and merges results.
pub struct FuseEngine {
    registry: Arc<ConnectorRegistry>,
}

/// Format of the incoming query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryFormat {
    Sql,
    Ppl,
}

/// Result of explaining a query — the execution plan as a string.
#[derive(Debug, Clone)]
pub struct ExplainResult {
    pub plan: String,
}

impl FuseEngine {
    pub fn new(registry: Arc<ConnectorRegistry>) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &ConnectorRegistry {
        &self.registry
    }

    /// Execute a federated query and return merged results.
    pub async fn execute_query(
        &self,
        query: &str,
        format: QueryFormat,
    ) -> Result<ResultSet, FuseError> {
        // Phase 1: parse the query to extract datasource + table references
        let (datasource, table) = parse_source_ref(query, format)?;

        let connector = self
            .registry
            .get(&datasource)
            .ok_or_else(|| FuseError::Plan(format!("datasource '{}' not found", datasource)))?;

        let sub_query = fuse_core::connector::SubQuery {
            schema: table,
            filter: None,
            limit: Some(100),
            passthrough: Some(query.to_string()),
        };

        let result = connector
            .execute(&sub_query)
            .await
            .map_err(|e| FuseError::Execution(e.to_string()))?;

        Ok(result)
    }

    /// Parse and validate a query without executing it.
    pub fn validate_query(&self, query: &str, format: QueryFormat) -> Result<(), FuseError> {
        let (datasource, _table) = parse_source_ref(query, format)?;
        if self.registry.get(&datasource).is_none() {
            return Err(FuseError::Plan(format!(
                "datasource '{}' not found in registry",
                datasource
            )));
        }
        Ok(())
    }

    /// Return the execution plan for a query.
    pub fn explain_query(
        &self,
        query: &str,
        format: QueryFormat,
    ) -> Result<ExplainResult, FuseError> {
        let (datasource, table) = parse_source_ref(query, format)?;
        let plan = format!(
            "FederatedPlan {{\n  datasource: \"{}\",\n  table: \"{}\",\n  format: {:?},\n  strategy: FanOut\n}}",
            datasource, table, format
        );
        Ok(ExplainResult { plan })
    }
}

/// Minimal source reference parser. Extracts `datasource.table` from a query string.
/// Full parser will replace this in a later phase.
fn parse_source_ref(query: &str, format: QueryFormat) -> Result<(String, String), FuseError> {
    let query = query.trim();
    match format {
        QueryFormat::Ppl => {
            // PPL: `source = datasource.table | ...`
            let rest = query
                .strip_prefix("source")
                .and_then(|s| s.trim_start().strip_prefix('='))
                .map(|s| s.trim_start())
                .ok_or_else(|| FuseError::Parse("PPL query must start with 'source = '".into()))?;

            let source_part = rest.split('|').next().unwrap_or(rest).trim();
            // Take first source for now (multi-source comes later)
            let first_source = source_part.split(',').next().unwrap_or(source_part).trim();
            parse_qualified_name(first_source)
        }
        QueryFormat::Sql => {
            // SQL: look for FROM datasource.table
            let lower = query.to_lowercase();
            let from_pos = lower
                .find("from ")
                .ok_or_else(|| FuseError::Parse("SQL query must contain FROM clause".into()))?;
            let after_from = &query[from_pos + 5..].trim_start();
            let table_part = after_from
                .split_whitespace()
                .next()
                .ok_or_else(|| FuseError::Parse("expected table reference after FROM".into()))?;
            parse_qualified_name(table_part)
        }
    }
}

fn parse_qualified_name(name: &str) -> Result<(String, String), FuseError> {
    if let Some((ds, tbl)) = name.split_once('.') {
        Ok((ds.to_string(), tbl.to_string()))
    } else {
        Err(FuseError::Parse(format!(
            "expected qualified name 'datasource.table', got '{}'",
            name
        )))
    }
}
