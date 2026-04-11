// SPDX-License-Identifier: Apache-2.0
//! Query complexity scoring — estimate resource needs before execution.

/// Complexity score for a query.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ComplexityScore {
    pub score: u32,
    pub datasource_count: usize,
    pub has_join: bool,
    pub has_union: bool,
    pub has_subquery: bool,
    pub has_aggregation: bool,
    pub level: &'static str,
}

/// Score a SQL query's complexity based on structural analysis.
pub fn score_query(query: &str) -> ComplexityScore {
    let upper = query.to_uppercase();
    let datasource_count = count_datasources(&upper);
    let has_join = upper.contains(" JOIN ");
    let has_union = upper.contains(" UNION ");
    let has_subquery = upper.contains("(SELECT") || upper.contains("( SELECT");
    let has_aggregation = ["GROUP BY", "COUNT(", "SUM(", "AVG(", "MAX(", "MIN("]
        .iter().any(|k| upper.contains(k));

    let mut score: u32 = 1;
    score += datasource_count.saturating_sub(1) as u32 * 3; // cross-source penalty
    if has_join { score += 5; }
    if has_union { score += 3; }
    if has_subquery { score += 4; }
    if has_aggregation { score += 2; }
    if upper.contains("ORDER BY") { score += 1; }
    if upper.contains("WINDOW") || upper.contains("OVER(") || upper.contains("OVER (") { score += 3; }

    let level = match score {
        0..=3 => "simple",
        4..=8 => "moderate",
        9..=15 => "complex",
        _ => "very_complex",
    };

    ComplexityScore { score, datasource_count, has_join, has_union, has_subquery, has_aggregation, level }
}

fn count_datasources(upper: &str) -> usize {
    let mut count = 0;
    for keyword in &["FROM ", "JOIN "] {
        let mut pos = 0;
        while let Some(idx) = upper[pos..].find(keyword) {
            let after = &upper[pos + idx + keyword.len()..];
            if let Some(token) = after.split_whitespace().next() {
                if token.contains('.') { count += 1; }
            }
            pos += idx + keyword.len();
        }
    }
    count.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let s = score_query("SELECT * FROM ds.table LIMIT 10");
        assert_eq!(s.level, "simple");
        assert!(!s.has_join);
    }

    #[test]
    fn test_join_query() {
        let s = score_query("SELECT a.x, b.y FROM ds1.t1 a JOIN ds2.t2 b ON a.id = b.id");
        assert!(s.has_join);
        assert!(s.score >= 5);
    }

    #[test]
    fn test_complex_query() {
        let s = score_query(
            "SELECT a.service, COUNT(*) FROM ds1.logs a JOIN ds2.users b ON a.uid = b.uid \
             WHERE a.status >= 500 GROUP BY a.service ORDER BY COUNT(*) DESC"
        );
        assert!(s.has_join);
        assert!(s.has_aggregation);
        assert!(s.score >= 8);
    }

    #[test]
    fn test_subquery() {
        let s = score_query("SELECT * FROM ds.t WHERE id IN (SELECT id FROM ds2.t2)");
        assert!(s.has_subquery);
    }

    #[test]
    fn test_union() {
        let s = score_query("SELECT * FROM ds1.t1 UNION ALL SELECT * FROM ds2.t2");
        assert!(s.has_union);
    }
}
