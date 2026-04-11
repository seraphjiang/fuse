// SPDX-License-Identifier: Apache-2.0
//! #1401 Retry wrapper for transient connector failures.

use std::time::Duration;

/// Retry configuration.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

/// Execute an async operation with exponential backoff retry.
pub async fn with_retry<F, Fut, T, E>(config: &RetryConfig, mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                attempt += 1;
                if attempt > config.max_retries {
                    return Err(e);
                }
                let backoff = std::cmp::min(
                    config.initial_backoff * 2u32.saturating_pow(attempt - 1),
                    config.max_backoff,
                );
                tracing::warn!(
                    attempt,
                    max = config.max_retries,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "Retrying after transient failure"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn test_succeeds_first_try() {
        let config = RetryConfig::default();
        let result: Result<i32, String> = with_retry(&config, || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retries_then_succeeds() {
        let config = RetryConfig { max_retries: 3, initial_backoff: Duration::from_millis(1), max_backoff: Duration::from_millis(10) };
        let count = AtomicU32::new(0);
        let result: Result<&str, String> = with_retry(&config, || {
            let n = count.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 { Err("transient".into()) } else { Ok("ok") }
            }
        }).await;
        assert_eq!(result.unwrap(), "ok");
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_exhausts_retries() {
        let config = RetryConfig { max_retries: 2, initial_backoff: Duration::from_millis(1), max_backoff: Duration::from_millis(5) };
        let result: Result<(), String> = with_retry(&config, || async { Err("permanent".into()) }).await;
        assert_eq!(result.unwrap_err(), "permanent");
    }

    #[tokio::test]
    async fn test_zero_retries() {
        let config = RetryConfig { max_retries: 0, ..Default::default() };
        let result: Result<(), String> = with_retry(&config, || async { Err("fail".into()) }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_backoff_capped() {
        let config = RetryConfig { max_retries: 10, initial_backoff: Duration::from_secs(1), max_backoff: Duration::from_secs(2) };
        // Backoff at attempt 10 would be 1*2^9 = 512s, but capped at 2s
        let backoff = std::cmp::min(
            config.initial_backoff * 2u32.saturating_pow(9),
            config.max_backoff,
        );
        assert_eq!(backoff, Duration::from_secs(2));
    }
}
