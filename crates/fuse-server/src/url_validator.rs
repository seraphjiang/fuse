// SPDX-License-Identifier: Apache-2.0

//! URL validation for SSRF protection.
//!
//! Blocks webhook callback URLs targeting internal/private networks,
//! cloud metadata endpoints, and localhost.

use std::net::IpAddr;

/// Validate a callback URL is safe for outbound requests.
/// Rejects private IPs, loopback, link-local, metadata endpoints.
pub fn validate_callback_url(url: &str) -> Result<(), String> {
    let parsed = url::Url::parse(url)
        .map_err(|e| format!("invalid URL: {e}"))?;

    // Only allow http/https schemes
    match parsed.scheme() {
        "http" | "https" => {}
        s => return Err(format!("scheme '{}' not allowed (use http or https)", s)),
    }

    let host = parsed.host_str()
        .ok_or("URL must have a host")?;

    // Block known dangerous hostnames
    let lower = host.to_lowercase();
    if lower == "localhost" || lower == "metadata.google.internal" {
        return Err(format!("host '{}' is blocked", host));
    }

    // If host is an IP, check for private/reserved ranges
    // Strip brackets for IPv6 addresses (url crate returns "[::1]")
    let bare_host = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
    if let Ok(ip) = bare_host.parse::<IpAddr>() {
        if is_private_ip(&ip) {
            return Err(format!("private/reserved IP '{}' is blocked", ip));
        }
    }

    // Block AWS metadata endpoint by IP
    if host == "169.254.169.254" || host == "fd00:ec2::254" {
        return Err("cloud metadata endpoint is blocked".into());
    }

    Ok(())
}

fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()              // 127.0.0.0/8
            || v4.is_private()            // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
            || v4.is_link_local()         // 169.254.0.0/16
            || v4.is_unspecified()        // 0.0.0.0
            || v4.is_broadcast()          // 255.255.255.255
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()              // ::1
            || v6.is_unspecified()        // ::
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_public_url() {
        assert!(validate_callback_url("https://example.com/webhook").is_ok());
        assert!(validate_callback_url("http://hooks.slack.com/services/T/B/x").is_ok());
    }

    #[test]
    fn test_blocks_localhost() {
        assert!(validate_callback_url("http://localhost:8080/hook").is_err());
        assert!(validate_callback_url("http://127.0.0.1/hook").is_err());
        assert!(validate_callback_url("http://[::1]/hook").is_err());
    }

    #[test]
    fn test_blocks_private_ips() {
        assert!(validate_callback_url("http://10.0.0.1/hook").is_err());
        assert!(validate_callback_url("http://172.16.0.1/hook").is_err());
        assert!(validate_callback_url("http://192.168.1.1/hook").is_err());
    }

    #[test]
    fn test_blocks_metadata() {
        assert!(validate_callback_url("http://169.254.169.254/latest/meta-data/").is_err());
        assert!(validate_callback_url("http://metadata.google.internal/").is_err());
    }

    #[test]
    fn test_blocks_link_local() {
        assert!(validate_callback_url("http://169.254.1.1/hook").is_err());
    }

    #[test]
    fn test_blocks_non_http_schemes() {
        assert!(validate_callback_url("ftp://example.com/hook").is_err());
        assert!(validate_callback_url("file:///etc/passwd").is_err());
        assert!(validate_callback_url("gopher://evil.com").is_err());
    }

    #[test]
    fn test_blocks_unspecified() {
        assert!(validate_callback_url("http://0.0.0.0/hook").is_err());
    }

    #[test]
    fn test_invalid_url() {
        assert!(validate_callback_url("not a url").is_err());
    }
}
