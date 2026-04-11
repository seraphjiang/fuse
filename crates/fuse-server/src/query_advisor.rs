// SPDX-License-Identifier: Apache-2.0

//! Query optimization advisor (#1502).
//!
//! Analyzes queries and suggests improvements: pushdown opportunities,
//! join reordering, missing filters, index hints, and limit additions.
//!
//! `POST /api/fuse/query/advise` — returns optimization suggestions.

use serde::Serialize;

use fuse_core::connector::ConnectorCapabilities;

/// A single optimization suggestion.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Suggestion {
    pub severity: Severity,
    pub category: &'static str,
    pub message: String,
    pub suggested_rewrite: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// Analyze a SQL query and return optimization suggestions.
pub fn advise(sql: &str, capabilities: &[(&str, ConnectorCapabilities)]) -> Vec<Suggestion> {
    let upper = sql.to_uppercase();
    let mut suggestions = Vec::new();

    // 1. Missing LIMIT on unbounded queries
    if !upper.contains("LIMIT") && !upper.contains("COUNT(") && !upper.contains("GROUP BY") {
        suggestions.push(Suggestion {
            severity: Severity::Warning,
            category: "performance",
            message: "Query has no LIMIT — may return unbounded rows. Consider adding LIMIT.".into(),
            suggested_rewrite: Some(format!("{} LIMIT 1000", sql.trim_end_matches(';').trim())),
        });
    }

    // 2. SELECT * without projection
    if upper.contains("SELECT *") {
        suggestions.push(Suggestion {
            severity: Severity::Info,
            category: "pushdown",
            message: "SELECT * fetches all columns. Specify columns to enable projection pushdown.".into(),
            suggested_rewrite: None,
        });
    }

    // 3. Cross-datasource JOIN without filter
    if upper.contains("JOIN") && !upper.contains("WHERE") {
        suggestions.push(Suggestion {
            severity: Severity::Critical,
            category: "performance",
            message: "JOIN without WHERE clause — this is a cross-join that may be very expensive.".into(),
            suggested_rewrite: None,
        });
    }

    // 4. LIKE with leading wildcard
    if upper.contains("LIKE '%") || upper.contains("LIKE \"%") {
        suggestions.push(Suggestion {
            severity: Severity::Warning,
            category: "pushdown",
            message: "LIKE with leading wildcard prevents index usage and filter pushdown.".into(),
            suggested_rewrite: None,
        });
    }

    // 5. ORDER BY without LIMIT
    if upper.contains("ORDER BY") && !upper.contains("LIMIT") {
        suggestions.push(Suggestion {
            severity: Severity::Warning,
            category: "performance",
            message: "ORDER BY without LIMIT sorts all rows. Add LIMIT for top-N queries.".into(),
            suggested_rewrite: None,
        });
    }

    // 6. Check connector capabilities for pushdown opportunities
    for (ds_name, caps) in capabilities {
        if !caps.supports_filtering && upper.contains("WHERE") {
            suggestions.push(Suggestion {
                severity: Severity::Info,
                category: "pushdown",
                message: format!("Datasource '{}' doesn't support filter pushdown — WHERE will be applied post-fetch.", ds_name),
                suggested_rewrite: None,
            });
        }
        if !caps.supports_aggregation && upper.contains("GROUP BY") {
            suggestions.push(Suggestion {
                severity: Severity::Info,
                category: "pushdown",
                message: format!("Datasource '{}' doesn't support aggregation pushdown — GROUP BY will be computed by Fuse engine.", ds_name),
                suggested_rewrite: None,
            });
        }
    }

    // 7. UNION without ALL (dedup is expensive)
    if upper.contains(" UNION ") && !upper.contains("UNION ALL") {
        suggestions.push(Suggestion {
            severity: Severity::Info,
            category: "performance",
            message: "UNION (without ALL) deduplicates results which is expensive. Use UNION ALL if duplicates are acceptable.".into(),
            suggested_rewrite: None,
        });
    }

    suggestions
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_core::connector::ConnectorCapabilities;

    #[test]
    fn test_missing_limit() {
        let s = advise("SELECT * FROM logs", &[]);
        assert!(s.iter().any(|s| s.category == "performance" && s.message.contains("LIMIT")));
        assert!(s.iter().any(|s| s.suggested_rewrite.as_deref() == Some("SELECT * FROM logs LIMIT 1000")));
    }

    #[test]
    fn test_has_limit_no_warning() {
        let s = advise("SELECT * FROM logs LIMIT 10", &[]);
        assert!(!s.iter().any(|s| s.message.contains("no LIMIT")));
    }

    #[test]
    fn test_select_star() {
        let s = advise("SELECT * FROM logs LIMIT 10", &[]);
        assert!(s.iter().any(|s| s.category == "pushdown" && s.message.contains("SELECT *")));
    }

    #[test]
    fn test_join_without_where() {
        let s = advise("SELECT a.id FROM a JOIN b ON a.id = b.id LIMIT 10", &[]);
        assert!(s.iter().any(|s| s.severity == Severity::Critical && s.message.contains("cross-join")));
    }

    #[test]
    fn test_join_with_where_ok() {
        let s = advise("SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.x = 1 LIMIT 10", &[]);
        assert!(!s.iter().any(|s| s.message.contains("cross-join")));
    }

    #[test]
    fn test_leading_wildcard() {
        let s = advise("SELECT id FROM t WHERE name LIKE '%test' LIMIT 10", &[]);
        assert!(s.iter().any(|s| s.message.contains("leading wildcard")));
    }

    #[test]
    fn test_order_by_without_limit() {
        let s = advise("SELECT id FROM t ORDER BY id", &[]);
        assert!(s.iter().any(|s| s.message.contains("ORDER BY without LIMIT")));
    }

    #[test]
    fn test_no_filter_pushdown_warning() {
        let mut caps = ConnectorCapabilities::full();
        caps.supports_filtering = false;
        let s = advise("SELECT id FROM t WHERE x = 1 LIMIT 10", &[("redis", caps)]);
        assert!(s.iter().any(|s| s.message.contains("redis") && s.message.contains("filter pushdown")));
    }

    #[test]
    fn test_no_agg_pushdown_warning() {
        let mut caps = ConnectorCapabilities::full();
        caps.supports_aggregation = false;
        let s = advise("SELECT count(*) FROM t GROUP BY x", &[("ddb", caps)]);
        assert!(s.iter().any(|s| s.message.contains("ddb") && s.message.contains("aggregation")));
    }

    #[test]
    fn test_union_vs_union_all() {
        let s = advise("SELECT id FROM a UNION SELECT id FROM b LIMIT 10", &[]);
        assert!(s.iter().any(|s| s.message.contains("UNION ALL")));
    }

    #[test]
    fn test_union_all_no_warning() {
        let s = advise("SELECT id FROM a UNION ALL SELECT id FROM b LIMIT 10", &[]);
        assert!(!s.iter().any(|s| s.message.contains("UNION (without ALL)")));
    }

    #[test]
    fn test_clean_query_minimal_suggestions() {
        let s = advise("SELECT id, name FROM users WHERE active = true LIMIT 50", &[]);
        // Should only have no critical suggestions
        assert!(!s.iter().any(|s| s.severity == Severity::Critical));
    }
}
