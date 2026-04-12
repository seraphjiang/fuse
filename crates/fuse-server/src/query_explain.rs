// SPDX-License-Identifier: Apache-2.0

//! Query explanation in plain English (#1830).
//!
//! Translates SQL queries into human-readable descriptions.
//! "This query joins error logs with user profiles to find premium users
//! hitting 500 errors yesterday."

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct QueryExplanation {
    pub summary: String,
    pub datasources: Vec<String>,
    pub operations: Vec<String>,
}

/// Generate a plain English explanation of a SQL query.
pub fn explain_query(sql: &str) -> QueryExplanation {
    let upper = sql.to_uppercase();
    let mut operations = Vec::new();
    let mut datasources = Vec::new();

    // Extract datasources from FROM/JOIN clauses
    for word in sql.split_whitespace() {
        if word.contains('.') && !word.starts_with('\'') && !word.contains('(') {
            let ds = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '.' && c != '_');
            if ds.contains('.') {
                let parts: Vec<&str> = ds.split('.').collect();
                if !datasources.contains(&parts[0].to_string()) {
                    datasources.push(parts[0].to_string());
                }
            }
        }
    }

    // Detect operations
    if upper.contains("JOIN") {
        operations.push("joins data across sources".into());
    }
    if upper.contains("WHERE") {
        operations.push("filters results".into());
    }
    if upper.contains("GROUP BY") {
        operations.push("groups and aggregates".into());
    }
    if upper.contains("ORDER BY") {
        operations.push("sorts results".into());
    }
    if upper.contains("LIMIT") {
        operations.push("limits output rows".into());
    }
    if upper.contains("UNION") {
        operations.push("combines results from multiple sources".into());
    }
    if upper.contains("DISTINCT") {
        operations.push("removes duplicates".into());
    }
    if upper.contains("HAVING") {
        operations.push("filters aggregated groups".into());
    }
    if upper.contains("COUNT(") || upper.contains("SUM(") || upper.contains("AVG(") {
        operations.push("computes aggregations".into());
    }

    // Build summary
    let ds_text = if datasources.is_empty() {
        "a datasource".to_string()
    } else if datasources.len() == 1 {
        format!("'{}'", datasources[0])
    } else {
        let last = datasources.last().unwrap().clone();
        let rest: Vec<String> = datasources[..datasources.len() - 1]
            .iter()
            .map(|d| format!("'{}'", d))
            .collect();
        format!("{} and '{}'", rest.join(", "), last)
    };

    let ops_text = if operations.is_empty() {
        "retrieves data".to_string()
    } else {
        operations.join(", ")
    };

    let summary = format!("This query reads from {} and {}", ds_text, ops_text);

    QueryExplanation {
        summary,
        datasources,
        operations,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let e = explain_query("SELECT * FROM cluster_a.logs LIMIT 10");
        assert!(e.summary.contains("cluster_a"));
        assert!(e.operations.contains(&"limits output rows".to_string()));
    }

    #[test]
    fn test_join_query() {
        let e = explain_query("SELECT l.id FROM cluster_a.logs l JOIN dynamodb.users u ON l.uid = u.uid WHERE l.status >= 500");
        assert!(e.datasources.contains(&"cluster_a".to_string()));
        assert!(e.datasources.contains(&"dynamodb".to_string()));
        assert!(e
            .operations
            .contains(&"joins data across sources".to_string()));
        assert!(e.operations.contains(&"filters results".to_string()));
    }

    #[test]
    fn test_aggregation() {
        let e = explain_query("SELECT host, count(*) FROM cluster_a.logs GROUP BY host ORDER BY count(*) DESC LIMIT 10");
        assert!(e.operations.contains(&"groups and aggregates".to_string()));
        assert!(e.operations.contains(&"computes aggregations".to_string()));
        assert!(e.operations.contains(&"sorts results".to_string()));
    }

    #[test]
    fn test_union() {
        let e = explain_query("SELECT * FROM a.logs UNION ALL SELECT * FROM b.logs LIMIT 100");
        assert!(e
            .operations
            .contains(&"combines results from multiple sources".to_string()));
    }

    #[test]
    fn test_no_datasource() {
        let e = explain_query("SELECT 1");
        assert!(e.summary.contains("a datasource"));
    }

    #[test]
    fn test_multiple_datasources_text() {
        let e = explain_query("SELECT * FROM a.t1 JOIN b.t2 ON a.t1.id = b.t2.id JOIN c.t3 ON b.t2.id = c.t3.id LIMIT 10");
        assert!(e.datasources.len() >= 2);
    }
}
