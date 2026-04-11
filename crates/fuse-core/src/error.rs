// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use thiserror::Error;

/// Structured error code for machine-readable error identification.
///
/// Format: `FUSE-XYYY` where X = category, YYY = specific error.
/// - 1xxx: Configuration errors
/// - 2xxx: Connector / registry errors
/// - 3xxx: Query parse errors
/// - 4xxx: Query planning errors
/// - 5xxx: Execution errors
/// - 6xxx: Authentication / authorization errors
/// - 7xxx: I/O / transport errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorCode(u16);

impl ErrorCode {
    // Configuration
    pub const CONFIG_INVALID: Self = Self(1000);
    pub const CONFIG_MISSING_FIELD: Self = Self(1001);
    pub const CONFIG_SECRET_RESOLVE: Self = Self(1002);

    // Connector / registry
    pub const CONNECTOR_ERROR: Self = Self(2000);
    pub const REGISTRY_DUPLICATE: Self = Self(2001);
    pub const REGISTRY_NOT_FOUND: Self = Self(2002);
    pub const CONNECTOR_QUERY_FAILED: Self = Self(2010);
    pub const CONNECTOR_SCHEMA_FAILED: Self = Self(2011);
    pub const CONNECTOR_UNSUPPORTED: Self = Self(2012);
    pub const CONNECTOR_CHANNEL_CLOSED: Self = Self(2013);

    // Parse
    pub const PARSE_ERROR: Self = Self(3000);

    // Planning
    pub const PLAN_ERROR: Self = Self(4000);

    // Execution
    pub const EXECUTION_ERROR: Self = Self(5000);

    // Auth
    pub const AUTH_FAILED: Self = Self(6000);
    pub const AUTH_CONNECTION_FAILED: Self = Self(6001);

    // I/O
    pub const IO_ERROR: Self = Self(7000);
    pub const ARROW_ERROR: Self = Self(7001);

    pub fn as_u16(self) -> u16 {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FUSE-{:04}", self.0)
    }
}

/// Top-level error type for the Fuse engine.
#[derive(Error, Debug)]
pub enum FuseError {
    #[error("[{code}] connector error: {msg}")]
    Connector { code: ErrorCode, msg: String },

    #[error("registry error: {0}")]
    Registry(#[from] RegistryError),

    #[error("[{code}] config error: {msg}")]
    Config { code: ErrorCode, msg: String },

    #[error("[FUSE-3000] query parse error: {0}")]
    Parse(String),

    #[error("[FUSE-4000] query planning error: {0}")]
    Plan(String),

    #[error("[FUSE-5000] execution error: {0}")]
    Execution(String),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl FuseError {
    /// Convenience constructor matching the old `FuseError::Connector(String)` call-sites.
    pub fn connector(msg: impl fmt::Display) -> Self {
        Self::Connector {
            code: ErrorCode::CONNECTOR_ERROR,
            msg: msg.to_string(),
        }
    }

    /// Convenience constructor matching the old `FuseError::Config(String)` call-sites.
    pub fn config(msg: impl fmt::Display) -> Self {
        Self::Config {
            code: ErrorCode::CONFIG_INVALID,
            msg: msg.to_string(),
        }
    }

    /// Return the structured error code for this error.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::Connector { code, .. } => *code,
            Self::Registry(RegistryError::DuplicateId(_)) => ErrorCode::REGISTRY_DUPLICATE,
            Self::Registry(RegistryError::NotFound(_)) => ErrorCode::REGISTRY_NOT_FOUND,
            Self::Config { code, .. } => *code,
            Self::Parse(_) => ErrorCode::PARSE_ERROR,
            Self::Plan(_) => ErrorCode::PLAN_ERROR,
            Self::Execution(_) => ErrorCode::EXECUTION_ERROR,
            Self::Arrow(_) => ErrorCode::ARROW_ERROR,
            Self::Io(_) => ErrorCode::IO_ERROR,
        }
    }
}

/// Errors specific to connector registration.
#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("[FUSE-2001] connector with id '{0}' already registered")]
    DuplicateId(String),

    #[error("[FUSE-2002] connector '{0}' not found")]
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

    #[error("streaming channel closed")]
    ChannelClosed,

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("{0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl ConnectorError {
    pub fn query<S: fmt::Display>(msg: S) -> Self {
        Self::QueryFailed(msg.to_string())
    }

    pub fn schema<S: fmt::Display>(msg: S) -> Self {
        Self::SchemaDiscovery(msg.to_string())
    }

    /// Return the structured error code for this connector error.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::QueryFailed(_) => ErrorCode::CONNECTOR_QUERY_FAILED,
            Self::SchemaDiscovery(_) => ErrorCode::CONNECTOR_SCHEMA_FAILED,
            Self::Auth(_) => ErrorCode::AUTH_FAILED,
            Self::Connection(_) => ErrorCode::AUTH_CONNECTION_FAILED,
            Self::ChannelClosed => ErrorCode::CONNECTOR_CHANNEL_CLOSED,
            Self::Unsupported(_) => ErrorCode::CONNECTOR_UNSUPPORTED,
            Self::Arrow(_) => ErrorCode::ARROW_ERROR,
            Self::Other(_) => ErrorCode::CONNECTOR_ERROR,
        }
    }
}

pub type FuseResult<T> = Result<T, FuseError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_display() {
        assert_eq!(ErrorCode::CONFIG_INVALID.to_string(), "FUSE-1000");
        assert_eq!(ErrorCode::CONNECTOR_QUERY_FAILED.to_string(), "FUSE-2010");
        assert_eq!(ErrorCode::PARSE_ERROR.to_string(), "FUSE-3000");
    }

    #[test]
    fn test_fuse_error_code_method() {
        assert_eq!(FuseError::config("bad").error_code(), ErrorCode::CONFIG_INVALID);
        assert_eq!(FuseError::connector("fail").error_code(), ErrorCode::CONNECTOR_ERROR);
        assert_eq!(FuseError::Parse("x".into()).error_code(), ErrorCode::PARSE_ERROR);
        assert_eq!(FuseError::Plan("x".into()).error_code(), ErrorCode::PLAN_ERROR);
        assert_eq!(FuseError::Execution("x".into()).error_code(), ErrorCode::EXECUTION_ERROR);
    }

    #[test]
    fn test_connector_error_codes() {
        assert_eq!(ConnectorError::query("t").error_code(), ErrorCode::CONNECTOR_QUERY_FAILED);
        assert_eq!(ConnectorError::schema("t").error_code(), ErrorCode::CONNECTOR_SCHEMA_FAILED);
        assert_eq!(ConnectorError::Auth("t".into()).error_code(), ErrorCode::AUTH_FAILED);
        assert_eq!(ConnectorError::Connection("t".into()).error_code(), ErrorCode::AUTH_CONNECTION_FAILED);
        assert_eq!(ConnectorError::ChannelClosed.error_code(), ErrorCode::CONNECTOR_CHANNEL_CLOSED);
        assert_eq!(ConnectorError::Unsupported("t".into()).error_code(), ErrorCode::CONNECTOR_UNSUPPORTED);
    }

    #[test]
    fn test_connector_error_display() {
        let e = ConnectorError::query("timeout after 30s");
        assert_eq!(e.to_string(), "query execution failed: timeout after 30s");
    }

    #[test]
    fn test_connector_error_schema_helper() {
        let e = ConnectorError::schema("no properties found");
        assert_eq!(e.to_string(), "schema discovery failed: no properties found");
    }

    #[test]
    fn test_registry_error_duplicate() {
        let e = RegistryError::DuplicateId("cluster_a".into());
        assert!(e.to_string().contains("cluster_a"));
        assert!(e.to_string().contains("FUSE-2001"));
    }

    #[test]
    fn test_fuse_error_from_registry() {
        let re = RegistryError::NotFound("missing".into());
        let fe: FuseError = re.into();
        assert!(fe.to_string().contains("missing"));
        assert_eq!(fe.error_code(), ErrorCode::REGISTRY_NOT_FOUND);
    }

    #[test]
    fn test_fuse_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file gone");
        let fe: FuseError = io_err.into();
        assert!(fe.to_string().contains("file gone"));
        assert_eq!(fe.error_code(), ErrorCode::IO_ERROR);
    }

    #[test]
    fn test_connector_error_channel_closed() {
        let e = ConnectorError::ChannelClosed;
        assert_eq!(e.to_string(), "streaming channel closed");
    }

    #[test]
    fn test_connector_error_variants_display() {
        assert!(ConnectorError::Auth("bad token".into()).to_string().contains("bad token"));
        assert!(ConnectorError::Connection("refused".into()).to_string().contains("refused"));
        assert!(ConnectorError::Unsupported("no scroll".into()).to_string().contains("no scroll"));
    }

    #[test]
    fn test_registry_error_not_found_display() {
        let e = RegistryError::NotFound("ds_x".into());
        assert!(e.to_string().contains("ds_x"));
        assert!(e.to_string().contains("FUSE-2002"));
    }

    #[test]
    fn test_fuse_error_display_includes_code() {
        let e = FuseError::config("missing field");
        assert!(e.to_string().contains("FUSE-1000"));
        assert!(e.to_string().contains("missing field"));
    }

    #[test]
    fn test_error_code_as_u16() {
        assert_eq!(ErrorCode::CONFIG_INVALID.as_u16(), 1000);
        assert_eq!(ErrorCode::EXECUTION_ERROR.as_u16(), 5000);
    }
}
