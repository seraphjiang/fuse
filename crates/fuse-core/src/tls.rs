// SPDX-License-Identifier: Apache-2.0

//! Per-connector TLS/mTLS configuration.
//!
//! Each `[[connector]]` in fuse.toml can optionally include:
//!
//! ```toml
//! [connector.tls]
//! ca_cert   = "/path/to/ca.pem"
//! client_cert = "/path/to/client.pem"
//! client_key  = "/path/to/client-key.pem"
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::FuseError;

/// TLS settings parsed from a connector's `[connector.tls]` table.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to a PEM-encoded CA certificate bundle for server verification.
    pub ca_cert: Option<PathBuf>,
    /// Path to a PEM-encoded client certificate (for mTLS).
    pub client_cert: Option<PathBuf>,
    /// Path to a PEM-encoded client private key (for mTLS).
    pub client_key: Option<PathBuf>,
}

impl TlsConfig {
    /// Extract TLS config from the connector properties map.
    /// Looks for a `tls` table with optional `ca_cert`, `client_cert`, `client_key` string fields.
    pub fn from_properties(properties: &HashMap<String, toml::Value>) -> Option<Self> {
        let tls_table = properties.get("tls")?.as_table()?;
        let ca_cert = tls_table
            .get("ca_cert")
            .and_then(|v| v.as_str())
            .map(PathBuf::from);
        let client_cert = tls_table
            .get("client_cert")
            .and_then(|v| v.as_str())
            .map(PathBuf::from);
        let client_key = tls_table
            .get("client_key")
            .and_then(|v| v.as_str())
            .map(PathBuf::from);
        Some(Self {
            ca_cert,
            client_cert,
            client_key,
        })
    }

    /// Returns true if any TLS field is configured.
    pub fn is_configured(&self) -> bool {
        self.ca_cert.is_some() || self.client_cert.is_some() || self.client_key.is_some()
    }

    /// Validate that referenced files exist and mTLS fields are paired.
    pub fn validate(&self) -> Result<(), FuseError> {
        if let Some(ref p) = self.ca_cert {
            check_file_exists(p, "ca_cert")?;
        }
        match (&self.client_cert, &self.client_key) {
            (Some(cert), Some(key)) => {
                check_file_exists(cert, "client_cert")?;
                check_file_exists(key, "client_key")?;
            }
            (Some(_), None) => {
                return Err(FuseError::config(
                    "tls.client_cert requires tls.client_key",
                ));
            }
            (None, Some(_)) => {
                return Err(FuseError::config(
                    "tls.client_key requires tls.client_cert",
                ));
            }
            (None, None) => {}
        }
        Ok(())
    }

    /// Read the CA certificate bytes (PEM).
    pub fn read_ca_cert(&self) -> Result<Option<Vec<u8>>, FuseError> {
        match &self.ca_cert {
            Some(p) => Ok(Some(std::fs::read(p).map_err(|e| {
                FuseError::config(format!("failed to read ca_cert '{}': {e}", p.display()))
            })?)),
            None => Ok(None),
        }
    }

    #[allow(clippy::type_complexity)]
    /// Read the client identity (cert + key) as PEM bytes.
    pub fn read_identity(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, FuseError> {
        match (&self.client_cert, &self.client_key) {
            (Some(cert_path), Some(key_path)) => {
                let cert = std::fs::read(cert_path).map_err(|e| {
                    FuseError::config(format!(
                        "failed to read client_cert '{}': {e}",
                        cert_path.display()
                    ))
                })?;
                let key = std::fs::read(key_path).map_err(|e| {
                    FuseError::config(format!(
                        "failed to read client_key '{}': {e}",
                        key_path.display()
                    ))
                })?;
                Ok(Some((cert, key)))
            }
            _ => Ok(None),
        }
    }

    /// Apply this TLS config to a `reqwest::ClientBuilder`.
    ///
    /// - Adds the CA cert as a root certificate (disabling system roots is left to the caller).
    /// - Adds the client identity for mTLS.
    pub fn apply_to_reqwest(
        &self,
        mut builder: reqwest::ClientBuilder,
    ) -> Result<reqwest::ClientBuilder, FuseError> {
        if let Some(ca_bytes) = self.read_ca_cert()? {
            let cert = reqwest::Certificate::from_pem(&ca_bytes).map_err(|e| {
                FuseError::config(format!("invalid ca_cert PEM: {e}"))
            })?;
            builder = builder.add_root_certificate(cert);
        }
        if let Some((cert_bytes, key_bytes)) = self.read_identity()? {
            // reqwest Identity expects a combined PEM (cert + key).
            let mut combined = cert_bytes;
            combined.push(b'\n');
            combined.extend_from_slice(&key_bytes);
            let identity = reqwest::Identity::from_pem(&combined).map_err(|e| {
                FuseError::config(format!("invalid client identity PEM: {e}"))
            })?;
            builder = builder.identity(identity);
        }
        Ok(builder)
    }

    /// Build a `rustls::ClientConfig` from this TLS config.
    ///
    /// Requires the `rustls-tls` feature on fuse-core.
    #[cfg(feature = "rustls-tls")]
    pub fn build_rustls_config(&self) -> Result<rustls::ClientConfig, FuseError> {
        use std::io::BufReader;

        let mut root_store = rustls::RootCertStore::empty();

        if let Some(ca_bytes) = self.read_ca_cert()? {
            let certs = rustls_pemfile::certs(&mut BufReader::new(ca_bytes.as_slice()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| FuseError::config(format!("invalid ca_cert PEM: {e}")))?;
            for cert in certs {
                root_store.add(cert).map_err(|e| {
                    FuseError::config(format!("failed to add CA cert: {e}"))
                })?;
            }
        } else {
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        let builder = rustls::ClientConfig::builder()
            .with_root_certificates(root_store);

        let config = if let Some((cert_bytes, key_bytes)) = self.read_identity()? {
            let certs = rustls_pemfile::certs(&mut BufReader::new(cert_bytes.as_slice()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| FuseError::config(format!("invalid client_cert PEM: {e}")))?;
            let key = rustls_pemfile::private_key(&mut BufReader::new(key_bytes.as_slice()))
                .map_err(|e| FuseError::config(format!("invalid client_key PEM: {e}")))?
                .ok_or_else(|| FuseError::config("no private key found in client_key PEM"))?;
            builder.with_client_auth_cert(certs, key)
                .map_err(|e| FuseError::config(format!("invalid client identity: {e}")))?
        } else {
            builder.with_no_client_auth()
        };

        Ok(config)
    }
}

fn check_file_exists(path: &Path, field: &str) -> Result<(), FuseError> {
    if !path.exists() {
        return Err(FuseError::config(format!(
            "tls.{field} file not found: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_from_properties_none_when_no_tls() {
        let props = HashMap::new();
        assert!(TlsConfig::from_properties(&props).is_none());
    }

    #[test]
    fn test_from_properties_parses_all_fields() {
        let toml_str = r#"
[tls]
ca_cert = "/tmp/ca.pem"
client_cert = "/tmp/client.pem"
client_key = "/tmp/client-key.pem"
"#;
        let val: toml::Value = toml::from_str(toml_str).unwrap();
        let props: HashMap<String, toml::Value> = val
            .as_table()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let tls = TlsConfig::from_properties(&props).unwrap();
        assert_eq!(tls.ca_cert.unwrap(), PathBuf::from("/tmp/ca.pem"));
        assert_eq!(tls.client_cert.unwrap(), PathBuf::from("/tmp/client.pem"));
        assert_eq!(tls.client_key.unwrap(), PathBuf::from("/tmp/client-key.pem"));
    }

    #[test]
    fn test_from_properties_ca_only() {
        let toml_str = r#"
[tls]
ca_cert = "/tmp/ca.pem"
"#;
        let val: toml::Value = toml::from_str(toml_str).unwrap();
        let props: HashMap<String, toml::Value> = val
            .as_table()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let tls = TlsConfig::from_properties(&props).unwrap();
        assert!(tls.ca_cert.is_some());
        assert!(tls.client_cert.is_none());
        assert!(tls.client_key.is_none());
    }

    #[test]
    fn test_is_configured() {
        let empty = TlsConfig::default();
        assert!(!empty.is_configured());

        let with_ca = TlsConfig {
            ca_cert: Some(PathBuf::from("/tmp/ca.pem")),
            ..Default::default()
        };
        assert!(with_ca.is_configured());
    }

    #[test]
    fn test_validate_missing_ca_cert_file() {
        let tls = TlsConfig {
            ca_cert: Some(PathBuf::from("/nonexistent/ca.pem")),
            ..Default::default()
        };
        let err = tls.validate().unwrap_err();
        assert!(err.to_string().contains("ca_cert"));
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_validate_client_cert_without_key() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "cert").unwrap();
        let tls = TlsConfig {
            client_cert: Some(f.path().to_path_buf()),
            client_key: None,
            ..Default::default()
        };
        let err = tls.validate().unwrap_err();
        assert!(err.to_string().contains("client_key"));
    }

    #[test]
    fn test_validate_client_key_without_cert() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "key").unwrap();
        let tls = TlsConfig {
            client_cert: None,
            client_key: Some(f.path().to_path_buf()),
            ..Default::default()
        };
        let err = tls.validate().unwrap_err();
        assert!(err.to_string().contains("client_cert"));
    }

    #[test]
    fn test_validate_valid_files() {
        let mut ca = NamedTempFile::new().unwrap();
        writeln!(ca, "ca").unwrap();
        let mut cert = NamedTempFile::new().unwrap();
        writeln!(cert, "cert").unwrap();
        let mut key = NamedTempFile::new().unwrap();
        writeln!(key, "key").unwrap();

        let tls = TlsConfig {
            ca_cert: Some(ca.path().to_path_buf()),
            client_cert: Some(cert.path().to_path_buf()),
            client_key: Some(key.path().to_path_buf()),
        };
        assert!(tls.validate().is_ok());
    }

    #[test]
    fn test_read_ca_cert_none() {
        let tls = TlsConfig::default();
        assert!(tls.read_ca_cert().unwrap().is_none());
    }

    #[test]
    fn test_read_ca_cert_reads_file() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"PEM DATA").unwrap();
        let tls = TlsConfig {
            ca_cert: Some(f.path().to_path_buf()),
            ..Default::default()
        };
        let data = tls.read_ca_cert().unwrap().unwrap();
        assert_eq!(data, b"PEM DATA");
    }

    #[test]
    fn test_read_identity_none() {
        let tls = TlsConfig::default();
        assert!(tls.read_identity().unwrap().is_none());
    }

    #[test]
    fn test_read_identity_reads_files() {
        let mut cert = NamedTempFile::new().unwrap();
        cert.write_all(b"CERT").unwrap();
        let mut key = NamedTempFile::new().unwrap();
        key.write_all(b"KEY").unwrap();
        let tls = TlsConfig {
            client_cert: Some(cert.path().to_path_buf()),
            client_key: Some(key.path().to_path_buf()),
            ..Default::default()
        };
        let (c, k) = tls.read_identity().unwrap().unwrap();
        assert_eq!(c, b"CERT");
        assert_eq!(k, b"KEY");
    }

    #[test]
    fn test_read_ca_cert_missing_file_errors() {
        let tls = TlsConfig {
            ca_cert: Some(PathBuf::from("/nonexistent/ca.pem")),
            ..Default::default()
        };
        assert!(tls.read_ca_cert().is_err());
    }

    #[test]
    fn test_validate_empty_is_ok() {
        let tls = TlsConfig::default();
        assert!(tls.validate().is_ok());
    }
}
