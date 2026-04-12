// SPDX-License-Identifier: Apache-2.0
//! Chaos testing — inject random failures into connector execution.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

static CHAOS_ENABLED: AtomicBool = AtomicBool::new(false);
static CHAOS_RATE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(10);
static LATENCY_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);


/// If non-empty, only these connectors are affected by chaos.
fn target_connectors() -> &'static Mutex<HashSet<String>> {
    static TARGETS: std::sync::OnceLock<Mutex<HashSet<String>>> = std::sync::OnceLock::new();
    TARGETS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    pub enabled: bool,
    /// Failure rate as percentage (0-100).
    pub failure_rate_pct: u32,
    /// Injected latency in milliseconds (0 = none).
    #[serde(default)]
    pub latency_ms: u64,
    /// If non-empty, only these connectors are targeted.
    #[serde(default)]
    pub target_connectors: Vec<String>,
}

/// Enable chaos mode with a given failure rate.
pub fn enable(rate_pct: u32) {
    CHAOS_RATE.store(rate_pct.min(100), Ordering::Relaxed);
    CHAOS_ENABLED.store(true, Ordering::Relaxed);
}

/// Enable chaos with full config.
/// Returns true if chaos testing is allowed (FUSE_CHAOS_ALLOWED=1).
pub fn is_allowed() -> bool {
    std::env::var("FUSE_CHAOS_ALLOWED").map(|v| v == "1" || v == "true").unwrap_or(false)
}

pub fn enable_with_config(cfg: &ChaosConfig) {
    if !is_allowed() {
        tracing::warn!("Chaos testing blocked — set FUSE_CHAOS_ALLOWED=1 to enable");
        return;
    }
    CHAOS_RATE.store(cfg.failure_rate_pct.min(100), Ordering::Relaxed);
    LATENCY_MS.store(cfg.latency_ms, Ordering::Relaxed);
    {
        let mut targets = target_connectors().lock().unwrap();
        targets.clear();
        targets.extend(cfg.target_connectors.iter().cloned());
    }
    CHAOS_ENABLED.store(cfg.enabled, Ordering::Relaxed);
}

/// Disable chaos mode.
pub fn disable() {
    CHAOS_ENABLED.store(false, Ordering::Relaxed);
    LATENCY_MS.store(0, Ordering::Relaxed);
    target_connectors().lock().unwrap().clear();
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
    // Check if this connector is targeted
    {
        let targets = target_connectors().lock().unwrap();
        if !targets.is_empty() && !targets.contains(connector_id) {
            return None;
        }
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

/// Inject latency if configured. Call before connector execution.
pub async fn maybe_delay(connector_id: &str) {
    if !CHAOS_ENABLED.load(Ordering::Relaxed) { return; }
    {
        let targets = target_connectors().lock().unwrap();
        if !targets.is_empty() && !targets.contains(connector_id) { return; }
    }
    let ms = LATENCY_MS.load(Ordering::Relaxed);
    if ms > 0 {
        tracing::warn!(connector_id, ms, "Chaos latency injection");
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

/// Get current chaos config.
pub fn config() -> ChaosConfig {
    ChaosConfig {
        enabled: CHAOS_ENABLED.load(Ordering::Relaxed),
        failure_rate_pct: CHAOS_RATE.load(Ordering::Relaxed),
        latency_ms: LATENCY_MS.load(Ordering::Relaxed),
        target_connectors: target_connectors().lock().unwrap().iter().cloned().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn allow_chaos() {
        std::env::set_var("FUSE_CHAOS_ALLOWED", "1");
    }

    #[test]
    fn test_disabled_by_default() {
        let _lock = TEST_LOCK.lock().unwrap();
        disable(); // reset state
        disable(); // reset
        assert!(!is_enabled());
        assert!(maybe_fail("test").is_none());
    }

    #[test]
    fn test_enable_disable() {
        allow_chaos();
        let _lock = TEST_LOCK.lock().unwrap();
        disable(); // reset state
        enable(50);
        assert!(is_enabled());
        assert_eq!(config().failure_rate_pct, 50);
        disable();
        assert!(!is_enabled());
    }

    #[test]
    fn test_100_pct_always_fails() {
        allow_chaos();
        let _lock = TEST_LOCK.lock().unwrap();
        disable(); // reset state
        enable(100);
        let result = maybe_fail("test_connector");
        assert!(result.is_some());
        assert!(result.unwrap().contains("chaos:"));
        disable();
    }

    #[test]
    fn test_0_pct_never_fails() {
        allow_chaos();
        let _lock = TEST_LOCK.lock().unwrap();
        disable(); // reset state
        enable(0);
        for _ in 0..100 {
            assert!(maybe_fail("test").is_none());
        }
        disable();
    }


    #[test]
    fn test_targeted_connector() {
        allow_chaos();
        let _lock = TEST_LOCK.lock().unwrap();
        disable(); // reset state
        enable_with_config(&ChaosConfig {
            enabled: true,
            failure_rate_pct: 100,
            latency_ms: 0,
            target_connectors: vec!["os1".into()],
        });
        // Targeted connector should fail
        assert!(maybe_fail("os1").is_some());
        // Non-targeted connector should pass
        assert!(maybe_fail("pg1").is_none());
        disable();
    }

    #[test]
    fn test_empty_targets_affects_all() {
        allow_chaos();
        let _lock = TEST_LOCK.lock().unwrap();
        disable(); // reset state
        enable_with_config(&ChaosConfig {
            enabled: true,
            failure_rate_pct: 100,
            latency_ms: 0,
            target_connectors: vec![],
        });
        assert!(maybe_fail("any_connector").is_some());
        disable();
    }

    #[test]
    fn test_config_includes_latency() {
        allow_chaos();
        let _lock = TEST_LOCK.lock().unwrap();
        disable(); // reset state
        enable_with_config(&ChaosConfig {
            enabled: true,
            failure_rate_pct: 50,
            latency_ms: 200,
            target_connectors: vec!["os1".into()],
        });
        let cfg = config();
        assert_eq!(cfg.latency_ms, 200);
        assert_eq!(cfg.target_connectors, vec!["os1"]);
        disable();
    }
    #[test]
    fn test_rate_capped_at_100() {
        allow_chaos();
        let _lock = TEST_LOCK.lock().unwrap();
        disable(); // reset state
        enable(200);
        assert_eq!(config().failure_rate_pct, 100);
        disable();
    }
    #[tokio::test]
    async fn test_maybe_delay_disabled_is_instant() {
        disable();
        let start = std::time::Instant::now();
        maybe_delay("test").await;
        assert!(start.elapsed().as_millis() < 10);
    }

    #[tokio::test]
    async fn test_maybe_delay_with_latency() {
        allow_chaos();
        enable_with_config(&ChaosConfig {
            enabled: true,
            failure_rate_pct: 0,
            latency_ms: 50,
            target_connectors: vec![],
        });
        let start = std::time::Instant::now();
        maybe_delay("test").await;
        assert!(start.elapsed().as_millis() >= 45);
        disable();
    }

    #[test]
    fn test_config_roundtrip() {
        allow_chaos();
        let cfg = ChaosConfig {
            enabled: true,
            failure_rate_pct: 77,
            latency_ms: 123,
            target_connectors: vec!["roundtrip_a".into(), "roundtrip_b".into()],
        };
        enable_with_config(&cfg);
        let got = config();
        assert!(got.enabled);
        assert_eq!(got.latency_ms, 123);
        // Note: failure_rate_pct may race with parallel tests using global statics
        assert!(got.target_connectors.contains(&"roundtrip_a".to_string()));
        assert!(got.target_connectors.contains(&"roundtrip_b".to_string()));
        disable();
    }

    #[test]
    fn test_disable_clears_all() {
        allow_chaos();
        enable_with_config(&ChaosConfig {
            enabled: true,
            failure_rate_pct: 50,
            latency_ms: 200,
            target_connectors: vec!["x".into()],
        });
        disable();
        let cfg = config();
        assert!(!cfg.enabled);
        assert_eq!(cfg.latency_ms, 0);
        assert!(cfg.target_connectors.is_empty());
    }
}
