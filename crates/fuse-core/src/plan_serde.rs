// SPDX-License-Identifier: Apache-2.0
//! Query plan serialization — serialize plans for caching and debugging.

use serde::{Deserialize, Serialize};

/// Serializable query plan node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanNode {
    pub op: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<PlanNode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_cost: Option<f64>,
}

impl PlanNode {
    pub fn leaf(op: &str, detail: &str) -> Self {
        Self { op: op.into(), detail: detail.into(), children: vec![], estimated_rows: None, estimated_cost: None }
    }

    pub fn with_child(mut self, child: PlanNode) -> Self {
        self.children.push(child);
        self
    }

    pub fn with_cost(mut self, rows: u64, cost: f64) -> Self {
        self.estimated_rows = Some(rows);
        self.estimated_cost = Some(cost);
        self
    }

    /// Serialize to JSON string.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    /// Deserialize from JSON string.
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|e| e.to_string())
    }

    /// Count total nodes in the plan tree.
    pub fn node_count(&self) -> usize {
        1 + self.children.iter().map(|c| c.node_count()).sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf() {
        let n = PlanNode::leaf("Scan", "cluster_a.logs");
        assert_eq!(n.op, "Scan");
        assert_eq!(n.node_count(), 1);
    }

    #[test]
    fn test_tree() {
        let plan = PlanNode::leaf("HashJoin", "ON id = id")
            .with_child(PlanNode::leaf("Scan", "ds1.t1").with_cost(1000, 10.0))
            .with_child(PlanNode::leaf("Scan", "ds2.t2").with_cost(500, 5.0));
        assert_eq!(plan.node_count(), 3);
    }

    #[test]
    fn test_roundtrip() {
        let plan = PlanNode::leaf("Scan", "t").with_cost(100, 1.5);
        let json = plan.to_json();
        let restored = PlanNode::from_json(&json).unwrap();
        assert_eq!(plan, restored);
    }

    #[test]
    fn test_invalid_json() {
        assert!(PlanNode::from_json("not json").is_err());
    }
}
