// SPDX-License-Identifier: Apache-2.0
//! Datasource capability summary — aggregate connector features.

use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize)]
pub struct CapabilitySummary {
    pub total: usize,
    pub supports_filtering: usize,
    pub supports_projection: usize,
    pub supports_aggregation: usize,
    pub supports_sorting: usize,
    pub supports_limit: usize,
    pub supports_streaming: usize,
}

impl CapabilitySummary {
    pub fn from_capabilities(
        caps: &HashMap<String, fuse_core::connector::ConnectorCapabilities>,
    ) -> Self {
        let mut s = Self {
            total: caps.len(),
            ..Default::default()
        };
        for c in caps.values() {
            if c.supports_filtering {
                s.supports_filtering += 1;
            }
            if c.supports_projection {
                s.supports_projection += 1;
            }
            if c.supports_aggregation {
                s.supports_aggregation += 1;
            }
            if c.supports_sorting {
                s.supports_sorting += 1;
            }
            if c.supports_limit {
                s.supports_limit += 1;
            }
            if c.supports_streaming {
                s.supports_streaming += 1;
            }
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_core::connector::ConnectorCapabilities;

    #[test]
    fn test_summary() {
        let mut caps = HashMap::new();
        caps.insert("pg".into(), ConnectorCapabilities::full());
        let mut limited = ConnectorCapabilities::full();
        limited.supports_filtering = false;
        caps.insert("s3".into(), limited);
        let s = CapabilitySummary::from_capabilities(&caps);
        assert_eq!(s.total, 2);
        assert_eq!(s.supports_filtering, 1);
    }

    #[test]
    fn test_empty() {
        let s = CapabilitySummary::from_capabilities(&HashMap::new());
        assert_eq!(s.total, 0);
    }
}
