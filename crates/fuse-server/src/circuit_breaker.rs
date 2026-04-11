// SPDX-License-Identifier: Apache-2.0
//! Circuit breaker — stop querying failing datasources temporarily.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    Closed,   // Normal — requests flow through
    Open,     // Tripped — requests rejected
    HalfOpen, // Testing — allow one request to check recovery
}

struct BreakerState {
    state: CircuitState,
    failure_count: u32,
    last_failure: Option<Instant>,
    last_success: Option<Instant>,
}

pub struct CircuitBreaker {
    breakers: Mutex<HashMap<String, BreakerState>>,
    failure_threshold: u32,
    recovery_timeout: Duration,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_secs: u64) -> Self {
        Self {
            breakers: Mutex::new(HashMap::new()),
            failure_threshold,
            recovery_timeout: Duration::from_secs(recovery_secs),
        }
    }

    /// Check if a request should be allowed.
    pub fn allow(&self, datasource: &str) -> bool {
        let mut map = self.breakers.lock().unwrap();
        let b = map.entry(datasource.to_string()).or_insert(BreakerState {
            state: CircuitState::Closed, failure_count: 0,
            last_failure: None, last_success: None,
        });
        match b.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if b.last_failure.map(|t| t.elapsed() > self.recovery_timeout).unwrap_or(true) {
                    b.state = CircuitState::HalfOpen;
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful request.
    pub fn record_success(&self, datasource: &str) {
        let mut map = self.breakers.lock().unwrap();
        if let Some(b) = map.get_mut(datasource) {
            b.failure_count = 0;
            b.state = CircuitState::Closed;
            b.last_success = Some(Instant::now());
        }
    }

    /// Record a failed request.
    pub fn record_failure(&self, datasource: &str) {
        let mut map = self.breakers.lock().unwrap();
        let b = map.entry(datasource.to_string()).or_insert(BreakerState {
            state: CircuitState::Closed, failure_count: 0,
            last_failure: None, last_success: None,
        });
        b.failure_count += 1;
        b.last_failure = Some(Instant::now());
        if b.failure_count >= self.failure_threshold {
            b.state = CircuitState::Open;
        }
    }

    pub fn state(&self, datasource: &str) -> CircuitState {
        self.breakers.lock().unwrap()
            .get(datasource)
            .map(|b| b.state.clone())
            .unwrap_or(CircuitState::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initially_closed() {
        let cb = CircuitBreaker::new(3, 30);
        assert!(cb.allow("pg"));
        assert_eq!(cb.state("pg"), CircuitState::Closed);
    }

    #[test]
    fn test_opens_after_threshold() {
        let cb = CircuitBreaker::new(3, 30);
        cb.record_failure("pg");
        cb.record_failure("pg");
        assert!(cb.allow("pg")); // still closed
        cb.record_failure("pg");
        assert_eq!(cb.state("pg"), CircuitState::Open);
        assert!(!cb.allow("pg"));
    }

    #[test]
    fn test_success_resets() {
        let cb = CircuitBreaker::new(3, 30);
        cb.record_failure("pg");
        cb.record_failure("pg");
        cb.record_success("pg");
        assert_eq!(cb.state("pg"), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_after_timeout() {
        let cb = CircuitBreaker::new(2, 0); // 0s recovery
        cb.record_failure("pg");
        cb.record_failure("pg");
        assert_eq!(cb.state("pg"), CircuitState::Open);
        // 0s timeout means immediate half-open
        assert!(cb.allow("pg"));
        assert_eq!(cb.state("pg"), CircuitState::HalfOpen);
    }
}
