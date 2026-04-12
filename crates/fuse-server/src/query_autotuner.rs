// SPDX-License-Identifier: Apache-2.0
//! Query auto-tuning — analyze slow queries and suggest index/partition changes.
//!
//! Examines query history for slow patterns and recommends:
//! - Index creation on frequently filtered columns
//! - Partition pruning opportunities
//! - Sort key alignment for range queries
//! - Aggregation pushdown hints

use serde::Serialize;

/// A tuning recommendation based on query history analysis.
#[derive(Debug, Clone, Serialize)]
pub struct TuningRecommendation {
    pub datasource: String,
    pub table: String,
    pub recommendation_type: RecommendationType,
    pub description: String,
    pub impact: Impact,
    /// Queries that would benefit from this recommendation.
    pub affected_query_count: usize,
    pub avg_latency_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationType {
    CreateIndex,
    AddPartition,
    AlignSortKey,
    PushdownAggregation,
    AddFilter,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Impact {
    High,
    Medium,
    Low,
}

/// Entry from query history used for analysis.
pub struct QuerySample {
    pub query: String,
    pub datasource: String,
    pub table: String,
    pub latency_ms: u64,
}

/// Analyze query samples and produce tuning recommendations.
pub fn analyze(samples: &[QuerySample], slow_threshold_ms: u64) -> Vec<TuningRecommendation> {
    let slow: Vec<&QuerySample> = samples.iter().filter(|s| s.latency_ms >= slow_threshold_ms).collect();
    if slow.is_empty() {
        return vec![];
    }

    let mut recs = Vec::new();

    // Group by (datasource, table)
    let mut groups: std::collections::HashMap<(String, String), Vec<&QuerySample>> =
        std::collections::HashMap::new();
    for s in &slow {
        groups
            .entry((s.datasource.clone(), s.table.clone()))
            .or_default()
            .push(s);
    }

    for ((ds, table), queries) in &groups {
        let avg_latency = queries.iter().map(|q| q.latency_ms).sum::<u64>() / queries.len() as u64;

        // Detect frequently filtered columns (WHERE col = / col > patterns)
        let mut filter_cols: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for q in queries {
            for col in extract_where_columns(&q.query) {
                *filter_cols.entry(col).or_default() += 1;
            }
        }
        for (col, count) in &filter_cols {
            if *count >= 2 {
                recs.push(TuningRecommendation {
                    datasource: ds.clone(),
                    table: table.clone(),
                    recommendation_type: RecommendationType::CreateIndex,
                    description: format!("Create index on column '{}' — used in WHERE clause of {} slow queries", col, count),
                    impact: if *count >= 5 { Impact::High } else { Impact::Medium },
                    affected_query_count: *count,
                    avg_latency_ms: avg_latency,
                });
            }
        }

        // Detect missing LIMIT on large scans
        let no_limit: Vec<&&QuerySample> = queries.iter().filter(|q| {
            let u = q.query.to_uppercase();
            !u.contains("LIMIT") && !u.contains("COUNT(")
        }).collect();
        if no_limit.len() >= 2 {
            recs.push(TuningRecommendation {
                datasource: ds.clone(),
                table: table.clone(),
                recommendation_type: RecommendationType::AddFilter,
                description: format!("{} slow queries lack LIMIT — consider adding pagination", no_limit.len()),
                impact: Impact::High,
                affected_query_count: no_limit.len(),
                avg_latency_ms: avg_latency,
            });
        }

        // Detect ORDER BY without matching sort key
        let mut order_cols: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for q in queries {
            for col in extract_order_columns(&q.query) {
                *order_cols.entry(col).or_default() += 1;
            }
        }
        for (col, count) in &order_cols {
            if *count >= 2 {
                recs.push(TuningRecommendation {
                    datasource: ds.clone(),
                    table: table.clone(),
                    recommendation_type: RecommendationType::AlignSortKey,
                    description: format!("Align sort key with column '{}' — used in ORDER BY of {} slow queries", col, count),
                    impact: Impact::Medium,
                    affected_query_count: *count,
                    avg_latency_ms: avg_latency,
                });
            }
        }

        // Detect GROUP BY that could be pushed down
        let group_by_count = queries.iter().filter(|q| q.query.to_uppercase().contains("GROUP BY")).count();
        if group_by_count >= 2 {
            recs.push(TuningRecommendation {
                datasource: ds.clone(),
                table: table.clone(),
                recommendation_type: RecommendationType::PushdownAggregation,
                description: format!("{} slow queries use GROUP BY — verify aggregation pushdown is enabled", group_by_count),
                impact: Impact::Medium,
                affected_query_count: group_by_count,
                avg_latency_ms: avg_latency,
            });
        }
    }

    recs.sort_by(|a, b| b.affected_query_count.cmp(&a.affected_query_count));
    recs
}

/// Extract column names from WHERE clauses (simple heuristic).
fn extract_where_columns(sql: &str) -> Vec<String> {
    let upper = sql.to_uppercase();
    let Some(where_pos) = upper.find("WHERE") else { return vec![] };
    let clause = &sql[where_pos + 5..];
    // Stop at GROUP BY, ORDER BY, LIMIT, HAVING, or end
    let end = ["GROUP BY", "ORDER BY", "LIMIT", "HAVING"]
        .iter()
        .filter_map(|kw| clause.to_uppercase().find(kw))
        .min()
        .unwrap_or(clause.len());
    let clause = &clause[..end];
    extract_identifiers_before_operators(clause)
}

/// Extract column names from ORDER BY clauses.
fn extract_order_columns(sql: &str) -> Vec<String> {
    let upper = sql.to_uppercase();
    let Some(pos) = upper.find("ORDER BY") else { return vec![] };
    let clause = &sql[pos + 8..];
    let end = ["LIMIT", "OFFSET"]
        .iter()
        .filter_map(|kw| clause.to_uppercase().find(kw))
        .min()
        .unwrap_or(clause.len());
    clause[..end]
        .split(',')
        .filter_map(|part| {
            let token = part.trim().split_whitespace().next()?;
            let col = token.split('.').last()?;
            if col.chars().all(|c| c.is_alphanumeric() || c == '_') && !col.is_empty() {
                Some(col.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Extract identifiers that appear before =, >, <, >=, <=, LIKE, IN, BETWEEN.
fn extract_identifiers_before_operators(clause: &str) -> Vec<String> {
    let ops = [">=", "<=", "!=", "=", ">", "<"];
    let mut cols = Vec::new();
    for op in ops {
        for (i, _) in clause.match_indices(op) {
            let before = clause[..i].trim();
            if let Some(token) = before.split_whitespace().last() {
                let col = token.split('.').last().unwrap_or(token);
                let col = col.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                if !col.is_empty() && col.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    let upper = col.to_uppercase();
                    if !["AND", "OR", "NOT", "WHERE", "ON"].contains(&upper.as_str()) {
                        cols.push(col.to_string());
                    }
                }
            }
        }
    }
    cols.sort();
    cols.dedup();
    cols
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_where_columns() {
        let cols = extract_where_columns("SELECT * FROM t WHERE status >= 500 AND user_id = 'abc'");
        assert!(cols.contains(&"status".to_string()));
        assert!(cols.contains(&"user_id".to_string()));
    }

    #[test]
    fn test_extract_order_columns() {
        let cols = extract_order_columns("SELECT * FROM t ORDER BY timestamp DESC, name ASC LIMIT 10");
        assert_eq!(cols, vec!["timestamp".to_string(), "name".to_string()]);
    }

    #[test]
    fn test_analyze_produces_index_recommendation() {
        let samples = vec![
            QuerySample { query: "SELECT * FROM t WHERE status >= 500".into(), datasource: "ds".into(), table: "logs".into(), latency_ms: 5000 },
            QuerySample { query: "SELECT * FROM t WHERE status = 200".into(), datasource: "ds".into(), table: "logs".into(), latency_ms: 4000 },
        ];
        let recs = analyze(&samples, 1000);
        assert!(!recs.is_empty());
        assert!(recs.iter().any(|r| matches!(r.recommendation_type, RecommendationType::CreateIndex)));
    }

    #[test]
    fn test_analyze_no_slow_queries() {
        let samples = vec![
            QuerySample { query: "SELECT 1".into(), datasource: "ds".into(), table: "t".into(), latency_ms: 10 },
        ];
        let recs = analyze(&samples, 1000);
        assert!(recs.is_empty());
    }

    #[test]
    fn test_analyze_missing_limit() {
        let samples = vec![
            QuerySample { query: "SELECT * FROM t WHERE x = 1".into(), datasource: "ds".into(), table: "t".into(), latency_ms: 3000 },
            QuerySample { query: "SELECT * FROM t WHERE y = 2".into(), datasource: "ds".into(), table: "t".into(), latency_ms: 4000 },
        ];
        let recs = analyze(&samples, 1000);
        assert!(recs.iter().any(|r| matches!(r.recommendation_type, RecommendationType::AddFilter)));
    }
}
