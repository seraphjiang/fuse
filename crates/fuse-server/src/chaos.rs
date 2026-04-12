// SPDX-License-Identifier: Apache-2.0
//! Chaos testing — inject random failures into connector execution.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

static CHAOS_ENABLED: AtomicBool = AtomicBool::new(false);
static CHAOS_RATE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(10);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    pub enabled: bool,
    /// Failure rate as percentage (0-100).
    pub failure_rate_pct: u32,
}

/// Enable chaos mode with a given failure rate.
pub fn enable(rate_pct: u32) {
    CHAOS_RATE.store(rate_pct.min(100), Ordering::Relaxed);
    CHAOS_ENABLED.store(true, Ordering::Relaxed);
}

/// Disable chaos mode.
pub fn disable() {
    CHAOS_ENABLED.store(false, Ordering::Relaxed);
}

/// Returns true if chaos is enabled.
pub fn is_enabled() -> bool {
    CHAOS_ENABLED.load(Ordering::Relaxed)
}

/// Check if this request should fail. Returns an error message if so.
pub fn maybe_fail(connector_id: &str) -> Option<String> {
    if !CHAOS_ENABLED.load(Ordering::Relaxed) {
        return None;
    }
    let rate = CHAOS_RATE.load(Ordering::Relaxed);
    // Simple pseudo-random using time-based hash (no rand dependency)
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let roll: u32 = nanos % 100;
    if roll < rate {
        let failures = [
            "chaos: simulated connection timeout",
            "chaos: simulated connection refused",
            "chaos: simulated internal server error",
            "chaos: simulated partial result (truncated)",
        ];
        let msg = failures[roll as usize % failures.len()];
        tracing::warn!(connector_id, msg, "Chaos injection triggered");
        Some(format!("[{}] {}", connector_id, msg))
    } else {
        None
    }
}

/// Get current chaos config.
pub fn config() -> ChaosConfig {
    ChaosConfig {
        enabled: CHAOS_ENABLED.load(Ordering::Relaxed),
        failure_rate_pct: CHAOS_RATE.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_by_default() {
        disable(); // reset
        assert!(!is_enabled());
        assert!(maybe_fail("test").is_none());
    }

    #[test]
    fn test_enable_disable() {
        enable(50);
        assert!(is_enabled());
        assert_eq!(config().failure_rate_pct, 50);
        disable();
        assert!(!is_enabled());
    }

    #[test]
    fn test_100_pct_always_fails() {
        enable(100);
        let result = maybe_fail("test_connector");
        assert!(result.is_some());
        assert!(result.unwrap().contains("chaos:"));
        disable();
    }

    #[test]
    fn test_0_pct_never_fails() {
        enable(0);
        for _ in 0..100 {
            assert!(maybe_fail("test").is_none());
        }
        disable();
    }

    #[test]
    fn test_rate_capped_at_100() {
        enable(200);
        assert_eq!(config().failure_rate_pct, 100);
        disable();
    }
}
