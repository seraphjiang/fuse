// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

/// Top-level error type for the Fuse engine.
#[derive(Error, Debug)]
pub enum FuseError {
    #[error("connector error: {0}")]
    Connector(String),

    #[error("registry error: {0}")]
    Registry(#[from] RegistryError),

    #[error("config error: {0}")]
    Config(String),

    #[error("query parse error: {0}")]
    Parse(String),

    #[error("query planning error: {0}")]
    Plan(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Errors specific to connector registration.
#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("connector with id '{0}' already registered")]
    DuplicateId(String),

    #[error("connector '{0}' not found")]
    NotFound(String),
}

/// Error type returned by individual connectors.
#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("query execution failed: {0}")]
    QueryFailed(String),

    #[error("schema discovery failed: {0}")]
    SchemaDiscovery(String),

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("connection failed: {0}")]
    Connection(String),

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("{0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type FuseResult<T> = Result<T, FuseError>;
