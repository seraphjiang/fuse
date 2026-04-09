// SPDX-License-Identifier: Apache-2.0

//! Fuse Connector SDK — everything you need to build a Fuse connector.
//!
//! This crate re-exports the core traits and types from `fuse-core` and
//! provides test utilities so connector authors don't need to depend on
//! Fuse internals.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use fuse_connector_sdk::prelude::*;
//!
//! #[derive(Debug)]
//! struct MyConnector { /* ... */ }
//!
//! #[async_trait]
//! impl FederatedConnector for MyConnector {
//!     fn id(&self) -> &str { "my_ds" }
//!     fn connector_type(&self) -> &str { "custom" }
//!     fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
//!     // ... implement remaining methods
//! }
//! ```
//!
//! # Testing
//!
//! ```rust,ignore
//! use fuse_connector_sdk::testing::{MockConnector, smoke_test, assert_batch_columns};
//!
//! #[tokio::test]
//! async fn test_my_connector() {
//!     let mock = MockConnector::new("test")
//!         .with_table("events", vec!["id", "name"])
//!         .with_rows("events", vec![vec!["1", "click"], vec!["2", "view"]]);
//!     smoke_test(&mock).await.unwrap();
//! }
//! ```

pub mod testing;

/// Prelude — import everything needed to implement a connector.
pub mod prelude {
    pub use async_trait::async_trait;

    // Core trait
    pub use fuse_core::connector::FederatedConnector;

    // Types needed for trait implementation
    pub use fuse_core::connector::{
        ConnectorCapabilities, ConnectorHealth, ConnectorType, HealthStatus, LatencyClass,
        SchemaInfo, SchemaType, SubQuery,
    };

    // Expression types for push-down
    pub use fuse_core::connector::{
        AggFunction, AggregationExpr, ComparisonOp, FilterExpr, ScalarValue, SortExpr,
    };

    // Error type
    pub use fuse_core::error::ConnectorError;

    // Factory trait
    pub use fuse_core::registry::ConnectorFactory;

    // Config
    pub use fuse_core::config::ConnectorConfig;

    // Arrow re-exports
    pub use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    pub use arrow::record_batch::RecordBatch;

    // Async
    pub use tokio::sync::mpsc;
}
