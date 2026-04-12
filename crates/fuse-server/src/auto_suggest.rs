// SPDX-License-Identifier: Apache-2.0

//! Auto-suggest queries from schema (#1501).
//!
//! Given a datasource and table with column metadata, generates useful
//! starter queries: row counts, top-N, recent data, aggregations,
//! distinct values, and null checks.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SuggestedQuery {
    pub label: String,
    pub query: String,
    pub category: &'static str,
}

/// Column metadata for suggestion generation.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Generate query suggestions for a given datasource.table with columns.
pub fn suggest(datasource: &str, table: &str, columns: &[ColumnMeta]) -> Vec<SuggestedQuery> {
    let fqn = format!("{}.{}", datasource, table);
    let mut suggestions = Vec::new();

    // 1. Preview
    suggestions.push(SuggestedQuery {
        label: format!("Preview {}", table),
        query: format!("SELECT * FROM {} LIMIT 10", fqn),
        category: "explore",
    });

    // 2. Row count
    suggestions.push(SuggestedQuery {
        label: format!("Count rows in {}", table),
        query: format!("SELECT count(*) AS total FROM {}", fqn),
        category: "aggregate",
    });

    // 3. Time-based: recent data (if timestamp-like column exists)
    if let Some(ts) = find_timestamp_column(columns) {
        suggestions.push(SuggestedQuery {
            label: "Recent data (last 24h)".into(),
            query: format!("SELECT * FROM {} ORDER BY {} DESC LIMIT 50", fqn, ts),
            category: "explore",
        });
    }

    // 4. Group by for low-cardinality string columns (first one found)
    if let Some(col) = columns.iter().find(|c| is_string_type(&c.data_type)) {
        suggestions.push(SuggestedQuery {
            label: format!("Distribution by {}", col.name),
            query: format!(
                "SELECT {}, count(*) AS cnt FROM {} GROUP BY {} ORDER BY cnt DESC LIMIT 20",
                col.name, fqn, col.name
            ),
            category: "aggregate",
        });
    }

    // 5. Numeric stats
    if let Some(col) = columns.iter().find(|c| is_numeric_type(&c.data_type)) {
        suggestions.push(SuggestedQuery {
            label: format!("Stats for {}", col.name),
            query: format!(
                "SELECT min({0}), max({0}), avg({0}) FROM {1}",
                col.name, fqn
            ),
            category: "aggregate",
        });
    }

    // 6. Null check for nullable columns
    if let Some(col) = columns.iter().find(|c| c.nullable) {
        suggestions.push(SuggestedQuery {
            label: format!("Null check: {}", col.name),
            query: format!(
                "SELECT count(*) AS null_count FROM {} WHERE {} IS NULL",
                fqn, col.name
            ),
            category: "quality",
        });
    }

    // 7. Distinct values
    if let Some(col) = columns.iter().find(|c| is_string_type(&c.data_type)) {
        suggestions.push(SuggestedQuery {
            label: format!("Distinct {}", col.name),
            query: format!("SELECT DISTINCT {} FROM {} LIMIT 100", col.name, fqn),
            category: "explore",
        });
    }

    suggestions
}

fn find_timestamp_column(columns: &[ColumnMeta]) -> Option<&str> {
    let ts_names = [
        "timestamp",
        "created_at",
        "updated_at",
        "time",
        "ts",
        "date",
        "event_time",
    ];
    columns
        .iter()
        .find(|c| ts_names.contains(&c.name.to_lowercase().as_str()))
        .map(|c| c.name.as_str())
}

fn is_string_type(dt: &str) -> bool {
    let lower = dt.to_lowercase();
    lower.contains("utf8")
        || lower.contains("string")
        || lower.contains("text")
        || lower.contains("varchar")
}

fn is_numeric_type(dt: &str) -> bool {
    let lower = dt.to_lowercase();
    lower.contains("int")
        || lower.contains("float")
        || lower.contains("double")
        || lower.contains("decimal")
        || lower.contains("numeric")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_columns() -> Vec<ColumnMeta> {
        vec![
            ColumnMeta {
                name: "timestamp".into(),
                data_type: "Utf8".into(),
                nullable: false,
            },
            ColumnMeta {
                name: "level".into(),
                data_type: "Utf8".into(),
                nullable: false,
            },
            ColumnMeta {
                name: "message".into(),
                data_type: "Utf8".into(),
                nullable: true,
            },
            ColumnMeta {
                name: "status".into(),
                data_type: "Int64".into(),
                nullable: false,
            },
        ]
    }

    #[test]
    fn test_always_has_preview_and_count() {
        let s = suggest("ds", "logs", &sample_columns());
        assert!(s.iter().any(|q| q.label.contains("Preview")));
        assert!(s.iter().any(|q| q.label.contains("Count")));
    }

    #[test]
    fn test_timestamp_column_detected() {
        let s = suggest("ds", "logs", &sample_columns());
        assert!(s.iter().any(|q| q.label.contains("Recent")));
    }

    #[test]
    fn test_no_timestamp_no_recent() {
        let cols = vec![ColumnMeta {
            name: "id".into(),
            data_type: "Int64".into(),
            nullable: false,
        }];
        let s = suggest("ds", "t", &cols);
        assert!(!s.iter().any(|q| q.label.contains("Recent")));
    }

    #[test]
    fn test_string_column_distribution() {
        let s = suggest("ds", "logs", &sample_columns());
        assert!(s
            .iter()
            .any(|q| q.category == "aggregate" && q.query.contains("GROUP BY")));
    }

    #[test]
    fn test_numeric_stats() {
        let s = suggest("ds", "logs", &sample_columns());
        assert!(s.iter().any(|q| q.query.contains("avg(")));
    }

    #[test]
    fn test_null_check() {
        let s = suggest("ds", "logs", &sample_columns());
        assert!(s
            .iter()
            .any(|q| q.category == "quality" && q.query.contains("IS NULL")));
    }

    #[test]
    fn test_distinct_values() {
        let s = suggest("ds", "logs", &sample_columns());
        assert!(s.iter().any(|q| q.query.contains("DISTINCT")));
    }

    #[test]
    fn test_fqn_in_queries() {
        let s = suggest("cluster_a", "application_logs", &sample_columns());
        for q in &s {
            assert!(
                q.query.contains("cluster_a.application_logs"),
                "missing FQN in: {}",
                q.query
            );
        }
    }

    #[test]
    fn test_empty_columns() {
        let s = suggest("ds", "t", &[]);
        assert_eq!(s.len(), 2); // just preview + count
    }
}
