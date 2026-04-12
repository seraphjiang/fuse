// SPDX-License-Identifier: Apache-2.0
//! Schema Relationship Discovery (#1831).
//!
//! Auto-detect foreign key relationships across datasources by analyzing
//! column name patterns and optionally sampling value overlap.

use std::sync::Arc;

use serde::Serialize;

use fuse_core::registry::ConnectorRegistry;

/// A discovered relationship between two columns across datasources.
#[derive(Debug, Clone, Serialize)]
pub struct Relationship {
    pub left_datasource: String,
    pub left_table: String,
    pub left_column: String,
    pub right_datasource: String,
    pub right_table: String,
    pub right_column: String,
    /// How the relationship was detected.
    pub method: DiscoveryMethod,
    /// Confidence score 0.0–1.0.
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryMethod {
    /// Exact column name match across tables.
    NameMatch,
    /// Column name follows `<table>_id` or `<table>Id` pattern.
    NamingConvention,
}

/// Column metadata used for relationship detection.
#[derive(Debug, Clone)]
struct ColumnInfo {
    datasource: String,
    table: String,
    name: String,
    data_type: String,
}

/// Discover relationships across all datasources by column name analysis.
pub async fn discover_relationships(registry: &Arc<ConnectorRegistry>) -> Vec<Relationship> {
    // Collect all columns from all datasources
    let mut all_columns: Vec<ColumnInfo> = Vec::new();

    for (ds_id, connector) in registry.connectors() {
        let tables = match connector.discover_schemas().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        for table_info in tables {
            let schema = match connector.get_schema(&table_info.name).await {
                Ok(s) => s,
                Err(_) => continue,
            };
            for field in schema.fields() {
                all_columns.push(ColumnInfo {
                    datasource: ds_id.clone(),
                    table: table_info.name.clone(),
                    name: field.name().to_string(),
                    data_type: field.data_type().to_string(),
                });
            }
        }
    }

    let mut relationships = Vec::new();

    // Strategy 1: Exact name match across different tables
    for (i, left) in all_columns.iter().enumerate() {
        for right in all_columns.iter().skip(i + 1) {
            // Skip same table
            if left.datasource == right.datasource && left.table == right.table {
                continue;
            }
            // Skip type mismatches
            if !types_compatible(&left.data_type, &right.data_type) {
                continue;
            }
            if left.name == right.name && is_likely_key(&left.name) {
                relationships.push(Relationship {
                    left_datasource: left.datasource.clone(),
                    left_table: left.table.clone(),
                    left_column: left.name.clone(),
                    right_datasource: right.datasource.clone(),
                    right_table: right.table.clone(),
                    right_column: right.name.clone(),
                    method: DiscoveryMethod::NameMatch,
                    confidence: 0.8,
                });
            }
        }
    }

    // Strategy 2: Naming convention — `<table>_id` or `<table>Id` patterns
    let table_names: Vec<(String, String)> = all_columns
        .iter()
        .map(|c| (c.datasource.clone(), c.table.clone()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    for col in &all_columns {
        for (ds, tbl) in &table_names {
            if &col.datasource == ds && &col.table == tbl {
                continue;
            }
            let snake = format!("{}_id", tbl.to_lowercase());
            let camel = format!("{}Id", tbl);
            if col.name == snake || col.name == camel {
                // Check if the target table has an "id" column
                let has_id = all_columns
                    .iter()
                    .any(|c| c.datasource == *ds && c.table == *tbl && c.name == "id");
                if has_id {
                    // Avoid duplicates
                    let exists = relationships.iter().any(|r| {
                        (r.left_datasource == col.datasource
                            && r.left_table == col.table
                            && r.left_column == col.name
                            && r.right_datasource == *ds
                            && r.right_table == *tbl)
                            || (r.right_datasource == col.datasource
                                && r.right_table == col.table
                                && r.right_column == col.name
                                && r.left_datasource == *ds
                                && r.left_table == *tbl)
                    });
                    if !exists {
                        relationships.push(Relationship {
                            left_datasource: col.datasource.clone(),
                            left_table: col.table.clone(),
                            left_column: col.name.clone(),
                            right_datasource: ds.clone(),
                            right_table: tbl.clone(),
                            right_column: "id".to_string(),
                            method: DiscoveryMethod::NamingConvention,
                            confidence: 0.7,
                        });
                    }
                }
            }
        }
    }

    // Sort by confidence descending
    relationships.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    relationships
}

/// Check if two data types are compatible for a join relationship.
fn types_compatible(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let ca = type_category(a);
    let cb = type_category(b);
    ca == cb
}

fn type_category(t: &str) -> u8 {
    let l = t.to_lowercase();
    if l.contains("int") || l.contains("long") || l.contains("bigint") {
        return 1;
    }
    if l.contains("float") || l.contains("double") || l.contains("decimal") {
        return 2;
    }
    if l.contains("bool") {
        return 3;
    }
    0 // string-like
}

/// Heuristic: column name looks like a key/identifier.
fn is_likely_key(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower.ends_with("_id")
        || lower.ends_with("id")
        || lower == "id"
        || lower == "key"
        || lower.ends_with("_key")
        || lower == "uuid"
        || lower.ends_with("_uuid")
        || lower == "email"
        || lower == "username"
        || lower == "user_id"
        || lower == "trace_id"
        || lower == "request_id"
}

/// GET /api/fuse/relationships — discover cross-datasource relationships.
pub async fn relationships_handler(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let rels = discover_relationships(&state.registry).await;
    axum::Json(rels).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_likely_key() {
        assert!(is_likely_key("user_id"));
        assert!(is_likely_key("userId"));
        assert!(is_likely_key("id"));
        assert!(is_likely_key("trace_id"));
        assert!(is_likely_key("request_id"));
        assert!(is_likely_key("email"));
        assert!(!is_likely_key("name"));
        assert!(!is_likely_key("message"));
        assert!(!is_likely_key("status"));
    }

    #[test]
    fn test_discovery_method_serialization() {
        let rel = Relationship {
            left_datasource: "a".into(),
            left_table: "t1".into(),
            left_column: "user_id".into(),
            right_datasource: "b".into(),
            right_table: "t2".into(),
            right_column: "user_id".into(),
            method: DiscoveryMethod::NameMatch,
            confidence: 0.8,
        };
        let json = serde_json::to_string(&rel).unwrap();
        assert!(json.contains("name_match"));
        assert!(json.contains("0.8"));
    }

    #[test]
    fn test_naming_convention_serialization() {
        let rel = Relationship {
            left_datasource: "a".into(),
            left_table: "orders".into(),
            left_column: "user_id".into(),
            right_datasource: "b".into(),
            right_table: "users".into(),
            right_column: "id".into(),
            method: DiscoveryMethod::NamingConvention,
            confidence: 0.7,
        };
        let json = serde_json::to_string(&rel).unwrap();
        assert!(json.contains("naming_convention"));
    }

    #[tokio::test]
    async fn test_discover_empty_registry() {
        let registry = Arc::new(ConnectorRegistry::new());
        let rels = discover_relationships(&registry).await;
        assert!(rels.is_empty());
    }

    #[test]
    fn test_types_compatible() {
        assert!(types_compatible("Int64", "Int64"));
        assert!(types_compatible("Int32", "Int64"));
        assert!(types_compatible("Utf8", "Utf8"));
        assert!(!types_compatible("Int64", "Boolean"));
        assert!(types_compatible("Float32", "Float64"));
    }

    #[test]
    fn test_type_category() {
        assert_eq!(type_category("Int64"), type_category("BigInt"));
        assert_eq!(type_category("Float32"), type_category("Double"));
        assert_ne!(type_category("Int64"), type_category("Boolean"));
        assert_eq!(type_category("Utf8"), type_category("String"));
    }

    #[test]
    fn test_is_likely_key_negative() {
        assert!(!is_likely_key("timestamp"));
        assert!(!is_likely_key("level"));
        assert!(!is_likely_key("count"));
        assert!(!is_likely_key("value"));
    }
}
