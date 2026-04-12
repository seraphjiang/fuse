// SPDX-License-Identifier: Apache-2.0

//! HMAC-based request signing and verification.
//!
//! Clients sign requests by computing HMAC-SHA256 over the request body
//! using a shared secret. Signature sent in `X-Fuse-Signature: sha256=<hex>`.
//! Optional `X-Fuse-Timestamp: <unix secs>` for replay protection.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct SigningConfig {
    secrets: Vec<SigningKey>,
    pub max_age_secs: u64,
}

#[derive(Debug, Clone)]
pub struct SigningKey {
    pub key_id: String,
    pub secret: Vec<u8>,
}

impl SigningConfig {
    pub fn new(secrets: Vec<SigningKey>, max_age_secs: u64) -> Self {
        Self { secrets, max_age_secs }
    }

    pub fn disabled() -> Self {
        Self { secrets: vec![], max_age_secs: 300 }
    }

    pub fn is_enabled(&self) -> bool {
        !self.secrets.is_empty()
    }
}

pub fn verify_signature(
    config: &SigningConfig,
    signature_header: &str,
    timestamp: Option<u64>,
    body: &[u8],
) -> Result<(), SigningError> {
    if !config.is_enabled() {
        return Ok(());
    }

    if let Some(ts) = timestamp {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now.abs_diff(ts) > config.max_age_secs {
            return Err(SigningError::Expired);
        }
    }

    let hex_sig = signature_header
        .strip_prefix("sha256=")
        .ok_or(SigningError::InvalidFormat)?;
    let expected = hex_decode(hex_sig).map_err(|_| SigningError::InvalidFormat)?;

    let payload = build_payload(timestamp, body);

    for key in &config.secrets {
        let mut mac = HmacSha256::new_from_slice(&key.secret)
            .map_err(|_| SigningError::InvalidKey)?;
        mac.update(&payload);
        if mac.verify_slice(&expected).is_ok() {
            return Ok(());
        }
    }

    Err(SigningError::InvalidSignature)
}

pub fn sign_request(secret: &[u8], timestamp: Option<u64>, body: &[u8]) -> String {
    let payload = build_payload(timestamp, body);
    let mut mac = HmacSha256::new_from_slice(secret).expect("valid key length");
    mac.update(&payload);
    let result = mac.finalize().into_bytes();
    format!("sha256={}", hex_encode(&result))
}

fn build_payload(timestamp: Option<u64>, body: &[u8]) -> Vec<u8> {
    match timestamp {
        Some(ts) => {
            let mut p = ts.to_string().into_bytes();
            p.push(b'.');
            p.extend_from_slice(body);
            p
        }
        None => body.to_vec(),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(hex: &str) -> Result<Vec<u8>, ()> {
    if !hex.len().is_multiple_of(2) { return Err(()); }
    (0..hex.len()).step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SigningError {
    InvalidFormat,
    InvalidSignature,
    InvalidKey,
    Expired,
}

impl std::fmt::Display for SigningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFormat => write!(f, "invalid signature format (expected sha256=<hex>)"),
            Self::InvalidSignature => write!(f, "invalid request signature"),
            Self::InvalidKey => write!(f, "invalid signing key"),
            Self::Expired => write!(f, "request timestamp expired"),
        }
    }
}

/// Axum middleware for request signature verification.
pub async fn signing_middleware(
    signing: Option<axum::Extension<Arc<SigningConfig>>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let config = match signing {
        Some(ref ext) => ext.0.clone(),
        None => return next.run(req).await,
    };

    if !config.is_enabled() {
        return next.run(req).await;
    }

    let path = req.uri().path().to_string();
    if matches!(path.as_str(), "/api/fuse/health" | "/metrics" | "/" | "/playground") {
        return next.run(req).await;
    }

    // Only verify POST requests with bodies
    if req.method() != axum::http::Method::POST {
        return next.run(req).await;
    }

    let sig_header = req.headers().get("x-fuse-signature")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let timestamp = req.headers().get("x-fuse-timestamp")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let (parts, body) = req.into_parts();
    let bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return error_response(axum::http::StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
        }
    };

    if let Some(sig) = &sig_header {
        if let Err(e) = verify_signature(&config, sig, timestamp, &bytes) {
            return error_response(axum::http::StatusCode::UNAUTHORIZED, &e.to_string());
        }
    } else {
        return error_response(axum::http::StatusCode::UNAUTHORIZED, "missing X-Fuse-Signature header");
    }

    let req = axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes));
    next.run(req).await
}

fn error_response(status: axum::http::StatusCode, msg: &str) -> axum::response::Response {
    axum::response::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            serde_json::json!({"error": msg}).to_string(),
        ))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SigningConfig {
        SigningConfig::new(
            vec![SigningKey { key_id: "k1".into(), secret: b"my-secret-key".to_vec() }],
            300,
        )
    }

    #[test]
    fn test_sign_and_verify() {
        let config = test_config();
        let sig = sign_request(b"my-secret-key", None, b"hello world");
        assert!(verify_signature(&config, &sig, None, b"hello world").is_ok());
    }

    #[test]
    fn test_sign_with_timestamp() {
        let config = test_config();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let sig = sign_request(b"my-secret-key", Some(now), b"hello");
        assert!(verify_signature(&config, &sig, Some(now), b"hello").is_ok());
    }

    #[test]
    fn test_wrong_key_rejected() {
        let config = test_config();
        let sig = sign_request(b"wrong-key", None, b"hello");
        assert_eq!(verify_signature(&config, &sig, None, b"hello"), Err(SigningError::InvalidSignature));
    }

    #[test]
    fn test_tampered_body_rejected() {
        let config = test_config();
        let sig = sign_request(b"my-secret-key", None, b"original");
        assert_eq!(verify_signature(&config, &sig, None, b"tampered"), Err(SigningError::InvalidSignature));
    }

    #[test]
    fn test_expired_timestamp() {
        let config = test_config();
        let sig = sign_request(b"my-secret-key", Some(1000), b"hello");
        assert_eq!(verify_signature(&config, &sig, Some(1000), b"hello"), Err(SigningError::Expired));
    }

    #[test]
    fn test_invalid_format() {
        let config = test_config();
        assert_eq!(verify_signature(&config, "bad", None, b"x"), Err(SigningError::InvalidFormat));
    }

    #[test]
    fn test_disabled_passes() {
        let config = SigningConfig::disabled();
        assert!(verify_signature(&config, "anything", None, b"x").is_ok());
    }

    #[test]
    fn test_multiple_keys() {
        let config = SigningConfig::new(vec![
            SigningKey { key_id: "old".into(), secret: b"old-secret".to_vec() },
            SigningKey { key_id: "new".into(), secret: b"new-secret".to_vec() },
        ], 300);
        let sig_old = sign_request(b"old-secret", None, b"test");
        let sig_new = sign_request(b"new-secret", None, b"test");
        assert!(verify_signature(&config, &sig_old, None, b"test").is_ok());
        assert!(verify_signature(&config, &sig_new, None, b"test").is_ok());
    }

    #[test]
    fn test_signature_format() {
        let sig = sign_request(b"key", None, b"body");
        assert!(sig.starts_with("sha256="));
        assert_eq!(sig.len(), 7 + 64); // "sha256=" + 32 bytes hex
    }

    #[test]
    fn test_hex_roundtrip() {
        let data = b"\x00\xff\x42\xab";
        let decoded = hex_decode(&hex_encode(data)).unwrap();
        assert_eq!(decoded, data);
    }
}
