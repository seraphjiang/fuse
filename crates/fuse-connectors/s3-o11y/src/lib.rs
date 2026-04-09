// SPDX-License-Identifier: Apache-2.0
//! S3 O11y connector — reads gzipped NDJSON logs from S3.

pub mod ndjson;

use std::sync::Arc;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::FederatedConnector;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

/// Factory for creating S3 O11y connectors.
pub struct S3O11yConnectorFactory;

impl ConnectorFactory for S3O11yConnectorFactory {
    fn connector_type(&self) -> &str {
        "s3-o11y"
    }

    fn create(
        &self,
        _config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Err(ConnectorError::Unsupported(
            "S3 O11y connector not yet fully implemented".into(),
        ))
    }
}
