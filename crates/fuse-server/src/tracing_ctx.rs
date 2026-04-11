// SPDX-License-Identifier: Apache-2.0
//! #1430 Distributed tracing — W3C Trace Context propagation.
//!
//! Extracts `traceparent` header from incoming requests and generates
//! trace/span IDs for internal use. Enables correlation across
//! federated Fuse instances without requiring a full OTel SDK.

use std::fmt;

/// Parsed W3C traceparent: version-trace_id-parent_id-flags
#[derive(Clone, Debug, PartialEq)]
pub struct TraceContext {
    pub trace_id: String,
    pub parent_span_id: String,
    pub span_id: String,
    pub sampled: bool,
}

impl TraceContext {
    /// Parse a W3C traceparent header value.
    /// Format: `00-<32 hex trace_id>-<16 hex parent_id>-<2 hex flags>`
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return None;
        }
        if parts[1].len() != 32 || parts[2].len() != 16 || parts[3].len() != 2 {
            return None;
        }
        // Validate hex
        if !parts[1].chars().all(|c| c.is_ascii_hexdigit())
            || !parts[2].chars().all(|c| c.is_ascii_hexdigit())
            || !parts[3].chars().all(|c| c.is_ascii_hexdigit())
        {
            return None;
        }
        let flags = u8::from_str_radix(parts[3], 16).ok()?;
        Some(Self {
            trace_id: parts[1].to_string(),
            parent_span_id: parts[2].to_string(),
            span_id: generate_span_id(),
            sampled: flags & 0x01 != 0,
        })
    }

    /// Create a new root trace context.
    pub fn new_root() -> Self {
        Self {
            trace_id: generate_trace_id(),
            parent_span_id: "0000000000000000".to_string(),
            span_id: generate_span_id(),
            sampled: true,
        }
    }

    /// Create a child span under this context.
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            parent_span_id: self.span_id.clone(),
            span_id: generate_span_id(),
            sampled: self.sampled,
        }
    }

    /// Format as W3C traceparent header value.
    pub fn to_traceparent(&self) -> String {
        let flags = if self.sampled { "01" } else { "00" };
        format!("00-{}-{}-{}", self.trace_id, self.span_id, flags)
    }
}

impl fmt::Display for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "trace={} span={}", &self.trace_id[..8], &self.span_id[..8])
    }
}

fn generate_trace_id() -> String {
    use std::time::SystemTime;
    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:032x}", t)
}

fn generate_span_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let val = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    format!("{:016x}", val ^ t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_traceparent() {
        let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let ctx = TraceContext::from_traceparent(tp).unwrap();
        assert_eq!(ctx.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.parent_span_id, "b7ad6b7169203331");
        assert!(ctx.sampled);
    }

    #[test]
    fn test_parse_unsampled() {
        let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00";
        let ctx = TraceContext::from_traceparent(tp).unwrap();
        assert!(!ctx.sampled);
    }

    #[test]
    fn test_parse_invalid_version() {
        assert!(TraceContext::from_traceparent("01-abc-def-00").is_none());
    }

    #[test]
    fn test_parse_invalid_length() {
        assert!(TraceContext::from_traceparent("00-short-short-00").is_none());
    }

    #[test]
    fn test_parse_invalid_hex() {
        let tp = "00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-b7ad6b7169203331-01";
        assert!(TraceContext::from_traceparent(tp).is_none());
    }

    #[test]
    fn test_new_root() {
        let ctx = TraceContext::new_root();
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.span_id.len(), 16);
        assert!(ctx.sampled);
    }

    #[test]
    fn test_child_inherits_trace_id() {
        let parent = TraceContext::new_root();
        let child = parent.child();
        assert_eq!(child.trace_id, parent.trace_id);
        assert_eq!(child.parent_span_id, parent.span_id);
        assert_ne!(child.span_id, parent.span_id);
    }

    #[test]
    fn test_roundtrip_traceparent() {
        let ctx = TraceContext::from_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        ).unwrap();
        let tp = ctx.to_traceparent();
        assert!(tp.starts_with("00-0af7651916cd43dd8448eb211c80319c-"));
        assert!(tp.ends_with("-01"));
    }

    #[test]
    fn test_display() {
        let ctx = TraceContext::new_root();
        let s = format!("{}", ctx);
        assert!(s.starts_with("trace="));
        assert!(s.contains("span="));
    }
}
