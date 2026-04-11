// SPDX-License-Identifier: Apache-2.0
//! Connection URL utilities — parse and validate connector URLs.

/// Parsed connection URL components.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectorUrl {
    pub scheme: String,
    pub host: String,
    pub port: Option<u16>,
    pub path: String,
    pub is_tls: bool,
}

impl ConnectorUrl {
    /// Parse a URL string into components.
    pub fn parse(url: &str) -> Result<Self, String> {
        let (scheme, rest) = url.split_once("://")
            .ok_or_else(|| format!("missing scheme in URL: {}", url))?;

        let is_tls = matches!(scheme, "https" | "grpcs" | "rediss" | "amqps");

        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };

        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => match p.parse::<u16>() {
                Ok(port) => (h.to_string(), Some(port)),
                Err(_) => (authority.to_string(), None),
            },
            None => (authority.to_string(), None),
        };

        Ok(Self {
            scheme: scheme.to_string(),
            host,
            port,
            path: path.to_string(),
            is_tls,
        })
    }

    /// Reconstruct the URL string.
    pub fn to_url(&self) -> String {
        match self.port {
            Some(p) => format!("{}://{}:{}{}", self.scheme, self.host, p, self.path),
            None => format!("{}://{}{}", self.scheme, self.host, self.path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_https() {
        let u = ConnectorUrl::parse("https://example.com:9200/path").unwrap();
        assert_eq!(u.scheme, "https");
        assert_eq!(u.host, "example.com");
        assert_eq!(u.port, Some(9200));
        assert_eq!(u.path, "/path");
        assert!(u.is_tls);
    }

    #[test]
    fn test_parse_http_no_port() {
        let u = ConnectorUrl::parse("http://localhost/api").unwrap();
        assert_eq!(u.host, "localhost");
        assert!(u.port.is_none());
        assert!(!u.is_tls);
    }

    #[test]
    fn test_parse_no_path() {
        let u = ConnectorUrl::parse("http://host:8080").unwrap();
        assert_eq!(u.port, Some(8080));
        assert_eq!(u.path, "/");
    }

    #[test]
    fn test_parse_grpcs() {
        let u = ConnectorUrl::parse("grpcs://flight.example.com:443").unwrap();
        assert!(u.is_tls);
        assert_eq!(u.scheme, "grpcs");
    }

    #[test]
    fn test_parse_missing_scheme() {
        assert!(ConnectorUrl::parse("no-scheme.com").is_err());
    }

    #[test]
    fn test_roundtrip() {
        let url = "https://example.com:9200/path";
        let u = ConnectorUrl::parse(url).unwrap();
        assert_eq!(u.to_url(), url);
    }
}
