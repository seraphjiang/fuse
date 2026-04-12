// SPDX-License-Identifier: Apache-2.0
//! Query similarity detection — find duplicate/similar queries across tenants.
//!
//! Normalizes queries by stripping literals and whitespace, then groups by
//! fingerprint to identify duplicate patterns. Useful for caching optimization
//! and identifying common access patterns.

use std::collections::HashMap;

use serde::Serialize;

/// A group of similar queries sharing the same fingerprint.
#[derive(Debug, Clone, Serialize)]
pub struct SimilarityGroup {
    pub fingerprint: String,
    pub normalized_query: String,
    pub count: usize,
    pub example_queries: Vec<String>,
    pub avg_latency_ms: u64,
    pub tenants: Vec<String>,
}

/// Normalize a SQL query: lowercase, collapse whitespace, replace literals with ?.
pub fn normalize_query(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    let mut quote_char = '\'';

    while let Some(c) = chars.next() {
        if in_string {
            if c == quote_char && chars.peek() != Some(&quote_char) {
                in_string = false;
                result.push('?');
            }
            continue;
        }
        match c {
            '\'' | '"' => {
                in_string = true;
                quote_char = c;
            }
            c if c.is_ascii_digit() => {
                // Skip entire number
                while chars
                    .peek()
                    .is_some_and(|p| p.is_ascii_digit() || *p == '.')
                {
                    chars.next();
                }
                result.push('?');
            }
            c if c.is_whitespace() => {
                if !result.ends_with(' ') && !result.is_empty() {
                    result.push(' ');
                }
            }
            _ => result.push(c.to_ascii_lowercase()),
        }
    }
    result.trim().to_string()
}

/// Compute a fingerprint (hash) for a normalized query.
pub fn fingerprint(normalized: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    normalized.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Input for similarity analysis.
pub struct QueryEntry {
    pub query: String,
    pub latency_ms: u64,
    pub tenant: Option<String>,
}

/// Analyze queries and group by similarity.
pub fn find_similar(entries: &[QueryEntry], min_group_size: usize) -> Vec<SimilarityGroup> {
    let mut groups: HashMap<String, (String, Vec<&QueryEntry>)> = HashMap::new();

    for entry in entries {
        let norm = normalize_query(&entry.query);
        let fp = fingerprint(&norm);
        groups
            .entry(fp)
            .or_insert_with(|| (norm, Vec::new()))
            .1
            .push(entry);
    }

    let mut result: Vec<SimilarityGroup> = groups
        .into_iter()
        .filter(|(_, (_, entries))| entries.len() >= min_group_size)
        .map(|(fp, (norm, entries))| {
            let avg_latency =
                entries.iter().map(|e| e.latency_ms).sum::<u64>() / entries.len().max(1) as u64;
            let mut tenants: Vec<String> =
                entries.iter().filter_map(|e| e.tenant.clone()).collect();
            tenants.sort();
            tenants.dedup();
            let examples: Vec<String> = entries.iter().take(3).map(|e| e.query.clone()).collect();
            SimilarityGroup {
                fingerprint: fp,
                normalized_query: norm,
                count: entries.len(),
                example_queries: examples,
                avg_latency_ms: avg_latency,
                tenants,
            }
        })
        .collect();

    result.sort_by(|a, b| b.count.cmp(&a.count));
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_strips_literals() {
        let norm = normalize_query("SELECT * FROM t WHERE name = 'alice' AND age > 25");
        assert_eq!(norm, "select * from t where name = ? and age > ?");
    }

    #[test]
    fn test_normalize_collapses_whitespace() {
        let norm = normalize_query("SELECT   *   FROM   t   LIMIT   10");
        assert_eq!(norm, "select * from t limit ?");
    }

    #[test]
    fn test_same_fingerprint_for_similar_queries() {
        let fp1 = fingerprint(&normalize_query("SELECT * FROM t WHERE id = 1"));
        let fp2 = fingerprint(&normalize_query("SELECT * FROM t WHERE id = 999"));
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_different_fingerprint_for_different_queries() {
        let fp1 = fingerprint(&normalize_query("SELECT * FROM t WHERE id = 1"));
        let fp2 = fingerprint(&normalize_query("SELECT * FROM t WHERE name = 'x'"));
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_find_similar_groups() {
        let entries = vec![
            QueryEntry {
                query: "SELECT * FROM t WHERE id = 1".into(),
                latency_ms: 100,
                tenant: Some("a".into()),
            },
            QueryEntry {
                query: "SELECT * FROM t WHERE id = 2".into(),
                latency_ms: 200,
                tenant: Some("b".into()),
            },
            QueryEntry {
                query: "SELECT * FROM t WHERE id = 3".into(),
                latency_ms: 150,
                tenant: Some("a".into()),
            },
            QueryEntry {
                query: "SELECT name FROM t2".into(),
                latency_ms: 50,
                tenant: Some("a".into()),
            },
        ];
        let groups = find_similar(&entries, 2);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].count, 3);
        assert_eq!(groups[0].avg_latency_ms, 150);
    }
}
