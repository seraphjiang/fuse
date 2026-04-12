// SPDX-License-Identifier: Apache-2.0
//! Registry snapshot — serializable state of all datasources.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct RegistrySnapshot {
    pub datasources: Vec<DatasourceInfo>,
    pub total: usize,
    pub healthy: usize,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatasourceInfo {
    pub id: String,
    pub connector_type: String,
    pub healthy: bool,
    pub table_count: Option<usize>,
}

impl RegistrySnapshot {
    pub fn new(datasources: Vec<DatasourceInfo>) -> Self {
        let total = datasources.len();
        let healthy = datasources.iter().filter(|d| d.healthy).count();
        Self {
            datasources,
            total,
            healthy,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    pub fn unhealthy(&self) -> Vec<&DatasourceInfo> {
        self.datasources.iter().filter(|d| !d.healthy).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot() {
        let snap = RegistrySnapshot::new(vec![
            DatasourceInfo {
                id: "pg".into(),
                connector_type: "postgres".into(),
                healthy: true,
                table_count: Some(5),
            },
            DatasourceInfo {
                id: "es".into(),
                connector_type: "elasticsearch".into(),
                healthy: false,
                table_count: None,
            },
        ]);
        assert_eq!(snap.total, 2);
        assert_eq!(snap.healthy, 1);
        assert_eq!(snap.unhealthy().len(), 1);
    }

    #[test]
    fn test_empty_snapshot() {
        let snap = RegistrySnapshot::new(vec![]);
        assert_eq!(snap.total, 0);
        assert_eq!(snap.healthy, 0);
    }
}
