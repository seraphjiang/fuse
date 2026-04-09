// SPDX-License-Identifier: Apache-2.0

//! Connector protocol versioning for forward/backward compatibility.
//!
//! Connectors declare their protocol version. The engine negotiates
//! compatibility and can warn or reject connectors that are too old.

use serde::{Deserialize, Serialize};

/// Semantic version for the connector protocol.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConnectorVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl ConnectorVersion {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }

    /// Current engine protocol version.
    pub const fn current() -> Self {
        Self::new(1, 0, 0)
    }

    /// Minimum supported connector protocol version.
    pub const fn minimum_supported() -> Self {
        Self::new(1, 0, 0)
    }
}

impl std::fmt::Display for ConnectorVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Compatibility result between engine and connector versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Compatibility {
    /// Fully compatible.
    Compatible,
    /// Connector is newer minor version — engine may not support all features.
    ConnectorAhead { connector: ConnectorVersion, engine: ConnectorVersion },
    /// Connector major version mismatch — incompatible.
    Incompatible { reason: String },
}

impl Compatibility {
    pub fn is_usable(&self) -> bool {
        matches!(self, Self::Compatible | Self::ConnectorAhead { .. })
    }
}

/// Negotiates version compatibility between engine and connector.
pub struct VersionNegotiator {
    engine_version: ConnectorVersion,
    minimum_version: ConnectorVersion,
}

impl VersionNegotiator {
    pub fn new() -> Self {
        Self {
            engine_version: ConnectorVersion::current(),
            minimum_version: ConnectorVersion::minimum_supported(),
        }
    }

    pub fn check(&self, connector_version: &ConnectorVersion) -> Compatibility {
        // Major version must match
        if connector_version.major != self.engine_version.major {
            return Compatibility::Incompatible {
                reason: format!(
                    "major version mismatch: engine={}, connector={}",
                    self.engine_version, connector_version
                ),
            };
        }

        // Connector must meet minimum
        if connector_version < &self.minimum_version {
            return Compatibility::Incompatible {
                reason: format!(
                    "connector version {} is below minimum {}",
                    connector_version, self.minimum_version
                ),
            };
        }

        // Connector ahead on minor — usable but may have features engine doesn't know about
        if connector_version.minor > self.engine_version.minor {
            return Compatibility::ConnectorAhead {
                connector: connector_version.clone(),
                engine: self.engine_version.clone(),
            };
        }

        Compatibility::Compatible
    }
}

impl Default for VersionNegotiator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compatible() {
        let n = VersionNegotiator::new();
        assert_eq!(n.check(&ConnectorVersion::new(1, 0, 0)), Compatibility::Compatible);
    }

    #[test]
    fn test_connector_ahead_minor() {
        let n = VersionNegotiator::new();
        let result = n.check(&ConnectorVersion::new(1, 2, 0));
        assert!(result.is_usable());
        assert!(matches!(result, Compatibility::ConnectorAhead { .. }));
    }

    #[test]
    fn test_major_mismatch() {
        let n = VersionNegotiator::new();
        let result = n.check(&ConnectorVersion::new(2, 0, 0));
        assert!(!result.is_usable());
        assert!(matches!(result, Compatibility::Incompatible { .. }));
    }
}
