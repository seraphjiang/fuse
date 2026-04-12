// SPDX-License-Identifier: Apache-2.0
//! #1400 CORS configuration.

use axum::http::{HeaderValue, Method};
use tower_http::cors::{Any, CorsLayer};

/// Build a CORS layer from configured origins.
/// Empty list = no CORS headers (same-origin only).
/// ["*"] = allow any origin.
/// Otherwise, allow only the listed origins.
pub fn build_cors_layer(origins: &[String]) -> Option<CorsLayer> {
    if origins.is_empty() {
        return None;
    }

    let layer = if origins.len() == 1 && origins[0] == "*" {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
            .allow_headers(Any)
    } else {
        let allowed: Vec<HeaderValue> = origins.iter().filter_map(|o| o.parse().ok()).collect();
        CorsLayer::new()
            .allow_origin(allowed)
            .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
            .allow_headers(Any)
    };

    Some(layer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_origins_returns_none() {
        assert!(build_cors_layer(&[]).is_none());
    }

    #[test]
    fn test_wildcard_returns_layer() {
        assert!(build_cors_layer(&["*".into()]).is_some());
    }

    #[test]
    fn test_specific_origins_returns_layer() {
        let origins = vec![
            "http://localhost:3000".into(),
            "https://app.example.com".into(),
        ];
        assert!(build_cors_layer(&origins).is_some());
    }
}
