// SPDX-License-Identifier: Apache-2.0
//! Query Lineage & Data Catalog (#1840)
//!
//! Track data flow across connectors. Per-query lineage graph:
//! source → transform → sink. POST /api/fuse/lineage for playground UI.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize)]
pub struct QueryLineage {
    pub query_id: String,
    pub sources: Vec<LineageSource>,
    pub join_type: Option<String>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LineageSource {
    pub datasource: String,
    pub table: String,
    pub rows_scanned: Option<u64>,
    pub bytes_read: Option<u64>,
    pub push_down_applied: bool,
}

impl QueryLineage {
    pub fn new(query_id: &str, sources: Vec<(&str, &str)>) -> Self {
        Self {
            query_id: query_id.to_string(),
            sources: sources.into_iter().map(|(ds, tbl)| LineageSource {
                datasource: ds.to_string(), table: tbl.to_string(),
                rows_scanned: None, bytes_read: None, push_down_applied: false,
            }).collect(),
            join_type: None,
            timestamp: crate::audit::now_secs(),
        }
    }

    pub fn with_join(mut self, join_type: &str) -> Self {
        self.join_type = Some(join_type.to_string());
        self
    }

    pub fn datasource_ids(&self) -> Vec<&str> {
        self.sources.iter().map(|s| s.datasource.as_str()).collect()
    }

    pub fn is_cross_source(&self) -> bool {
        let mut ds: Vec<&str> = self.sources.iter().map(|s| s.datasource.as_str()).collect();
        ds.sort();
        ds.dedup();
        ds.len() > 1
    }
}

// ── Lineage Graph ──

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum NodeType { Source, Transform, Sink }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    pub id: String,
    pub label: String,
    pub node_type: NodeType,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    pub from: String,
    pub to: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageGraph {
    pub query: String,
    pub nodes: Vec<LineageNode>,
    pub edges: Vec<LineageEdge>,
}

pub fn extract_lineage(query: &str, format: &str) -> LineageGraph {
    let upper = query.to_uppercase();
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut nid = 0usize;

    let tables = extract_table_refs(query, format);
    let source_ids: Vec<String> = tables.iter().map(|t| {
        let id = format!("n{nid}"); nid += 1;
        let mut meta = HashMap::new();
        if let Some((ds, tbl)) = t.split_once('.') {
            meta.insert("datasource".into(), ds.into());
            meta.insert("table".into(), tbl.into());
        }
        nodes.push(LineageNode { id: id.clone(), label: t.clone(), node_type: NodeType::Source, metadata: meta });
        id
    }).collect();

    let transforms = detect_transforms(&upper);
    let mut prev = source_ids;
    for tf in &transforms {
        let id = format!("n{nid}"); nid += 1;
        nodes.push(LineageNode { id: id.clone(), label: tf.clone(), node_type: NodeType::Transform, metadata: HashMap::new() });
        for p in &prev { edges.push(LineageEdge { from: p.clone(), to: id.clone(), label: None }); }
        prev = vec![id];
    }

    let sink_id = format!("n{nid}");
    nodes.push(LineageNode { id: sink_id.clone(), label: "Result".into(), node_type: NodeType::Sink, metadata: HashMap::new() });
    for p in &prev { edges.push(LineageEdge { from: p.clone(), to: sink_id.clone(), label: None }); }

    LineageGraph { query: query.to_string(), nodes, edges }
}

fn extract_table_refs(query: &str, format: &str) -> Vec<String> {
    let mut tables = Vec::new();
    if format == "ppl" {
        for part in query.split('|') {
            let trimmed = part.trim();
            if let Some(rest) = trimmed.strip_prefix("source") {
                if let Some(tbl) = rest.trim().strip_prefix('=') {
                    if let Some(t) = tbl.trim().split_whitespace().next() {
                        if !t.is_empty() { tables.push(t.to_string()); }
                    }
                }
            }
            if trimmed.starts_with("lookup") {
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                if parts.len() >= 2 { tables.push(parts[1].to_string()); }
            }
        }
    } else {
        let tokens: Vec<&str> = query.split_whitespace().collect();
        let upper_tokens: Vec<String> = tokens.iter().map(|t| t.to_uppercase()).collect();
        for (i, ut) in upper_tokens.iter().enumerate() {
            if (ut == "FROM" || ut == "JOIN") && i + 1 < tokens.len() {
                let tbl = tokens[i + 1].trim_end_matches(|c| c == ',' || c == ')');
                if !tbl.is_empty() && !tbl.starts_with('(') && tbl.to_uppercase() != "SELECT" {
                    tables.push(tbl.to_string());
                }
            }
        }
    }
    tables.sort();
    tables.dedup();
    tables
}

fn detect_transforms(upper: &str) -> Vec<String> {
    let mut t = Vec::new();
    if upper.contains(" JOIN ") { t.push("JOIN".into()); }
    if upper.contains(" WHERE ") || upper.contains("| WHERE ") { t.push("FILTER".into()); }
    if upper.contains(" GROUP BY ") || upper.contains("| STATS ") { t.push("AGGREGATE".into()); }
    if upper.contains(" ORDER BY ") || upper.contains("| SORT ") { t.push("SORT".into()); }
    if upper.contains(" UNION ") { t.push("UNION".into()); }
    if upper.contains("ROW_NUMBER") || upper.contains("RANK(") || upper.contains(" OVER ") { t.push("WINDOW".into()); }
    t
}

// ── Lineage Store ──

pub struct LineageStore {
    entries: Mutex<Vec<LineageGraph>>,
    max_entries: usize,
}

impl LineageStore {
    pub fn new(max_entries: usize) -> Self {
        Self { entries: Mutex::new(Vec::new()), max_entries }
    }

    pub fn record(&self, graph: LineageGraph) {
        let mut e = self.entries.lock().unwrap();
        if e.len() >= self.max_entries { e.remove(0); }
        e.push(graph);
    }

    pub fn entries(&self) -> Vec<LineageGraph> { self.entries.lock().unwrap().clone() }
    pub fn count(&self) -> usize { self.entries.lock().unwrap().len() }

    pub fn catalog(&self) -> Vec<CatalogEntry> {
        let entries = self.entries.lock().unwrap();
        let mut seen: HashMap<String, usize> = HashMap::new();
        for g in entries.iter() {
            for n in &g.nodes {
                if n.node_type == NodeType::Source { *seen.entry(n.label.clone()).or_insert(0) += 1; }
            }
        }
        let mut cat: Vec<CatalogEntry> = seen.into_iter().map(|(name, qc)| {
            let (ds, tbl) = name.split_once('.')
                .map(|(d, t)| (d.into(), t.into()))
                .unwrap_or((name.clone(), String::new()));
            CatalogEntry { name, datasource: ds, table: tbl, query_count: qc }
        }).collect();
        cat.sort_by(|a, b| b.query_count.cmp(&a.query_count));
        cat
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogEntry {
    pub name: String,
    pub datasource: String,
    pub table: String,
    pub query_count: usize,
}

#[derive(Deserialize)]
pub struct LineageRequest {
    pub query: String,
    #[serde(default = "default_sql")]
    pub format: String,
}
fn default_sql() -> String { "sql".into() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_source() {
        let l = QueryLineage::new("q-1", vec![("cluster_a", "logs")]);
        assert_eq!(l.sources.len(), 1);
        assert!(!l.is_cross_source());
    }

    #[test]
    fn test_cross_source() {
        let l = QueryLineage::new("q-2", vec![("cluster_a", "logs"), ("dynamodb", "users")]);
        assert!(l.is_cross_source());
        assert_eq!(l.datasource_ids(), vec!["cluster_a", "dynamodb"]);
    }

    #[test]
    fn test_with_join() {
        let l = QueryLineage::new("q-3", vec![("a", "t1"), ("b", "t2")]).with_join("hash_join");
        assert_eq!(l.join_type.as_deref(), Some("hash_join"));
    }

    #[test]
    fn test_same_source_not_cross() {
        let l = QueryLineage::new("q-4", vec![("ds", "t1"), ("ds", "t2")]);
        assert!(!l.is_cross_source());
    }

    #[test]
    fn test_extract_sql_single() {
        let g = extract_lineage("SELECT * FROM cluster_a.logs", "sql");
        assert_eq!(g.nodes.iter().filter(|n| n.node_type == NodeType::Source).count(), 1);
        assert!(g.nodes.iter().any(|n| n.label == "cluster_a.logs"));
    }

    #[test]
    fn test_extract_sql_join() {
        let g = extract_lineage("SELECT l.id FROM cluster_a.logs l JOIN dynamodb.users u ON l.uid = u.uid", "sql");
        assert_eq!(g.nodes.iter().filter(|n| n.node_type == NodeType::Source).count(), 2);
        assert!(g.nodes.iter().any(|n| n.label == "JOIN" && n.node_type == NodeType::Transform));
    }

    #[test]
    fn test_extract_filter_agg() {
        let g = extract_lineage("SELECT svc, count(*) FROM a.logs WHERE status >= 500 GROUP BY svc", "sql");
        assert!(g.nodes.iter().any(|n| n.label == "FILTER"));
        assert!(g.nodes.iter().any(|n| n.label == "AGGREGATE"));
    }

    #[test]
    fn test_extract_ppl() {
        let g = extract_lineage("source = cluster_a.logs | where status >= 500 | lookup dynamodb.users user_id", "ppl");
        assert_eq!(g.nodes.iter().filter(|n| n.node_type == NodeType::Source).count(), 2);
    }

    #[test]
    fn test_extract_union() {
        let g = extract_lineage("SELECT * FROM a.t1 UNION ALL SELECT * FROM b.t2", "sql");
        assert!(g.nodes.iter().any(|n| n.label == "UNION"));
    }

    #[test]
    fn test_extract_window() {
        let g = extract_lineage("SELECT ROW_NUMBER() OVER (ORDER BY ts) FROM a.logs", "sql");
        assert!(g.nodes.iter().any(|n| n.label == "WINDOW"));
    }

    #[test]
    fn test_node_metadata() {
        let g = extract_lineage("SELECT * FROM myds.mytable", "sql");
        let src = g.nodes.iter().find(|n| n.node_type == NodeType::Source).unwrap();
        assert_eq!(src.metadata.get("datasource").unwrap(), "myds");
        assert_eq!(src.metadata.get("table").unwrap(), "mytable");
    }

    #[test]
    fn test_store_record() {
        let s = LineageStore::new(10);
        s.record(extract_lineage("SELECT * FROM t", "sql"));
        assert_eq!(s.count(), 1);
    }

    #[test]
    fn test_store_eviction() {
        let s = LineageStore::new(2);
        s.record(extract_lineage("SELECT * FROM t1", "sql"));
        s.record(extract_lineage("SELECT * FROM t2", "sql"));
        s.record(extract_lineage("SELECT * FROM t3", "sql"));
        assert_eq!(s.count(), 2);
        assert!(s.entries()[0].query.contains("t2"));
    }

    #[test]
    fn test_catalog() {
        let s = LineageStore::new(100);
        s.record(extract_lineage("SELECT * FROM ds.t1", "sql"));
        s.record(extract_lineage("SELECT * FROM ds.t1 JOIN ds.t2 ON 1=1", "sql"));
        let cat = s.catalog();
        assert!(cat.iter().any(|c| c.name == "ds.t1" && c.query_count == 2));
        assert!(cat.iter().any(|c| c.name == "ds.t2" && c.query_count == 1));
    }

    #[test]
    fn test_catalog_sorted() {
        let s = LineageStore::new(100);
        s.record(extract_lineage("SELECT * FROM a.rare", "sql"));
        s.record(extract_lineage("SELECT * FROM b.popular", "sql"));
        s.record(extract_lineage("SELECT * FROM b.popular", "sql"));
        assert_eq!(s.catalog()[0].name, "b.popular");
    }

    #[test]
    fn test_edges_reach_sink() {
        let g = extract_lineage("SELECT * FROM a.t1 JOIN b.t2 ON 1=1 WHERE x > 1", "sql");
        let sink = g.nodes.iter().find(|n| n.node_type == NodeType::Sink).unwrap();
        assert!(g.edges.iter().any(|e| e.to == sink.id));
    }
}
