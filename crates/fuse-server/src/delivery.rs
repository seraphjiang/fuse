// SPDX-License-Identifier: Apache-2.0
//! Streaming threshold — decide when to stream vs buffer results.

/// Recommendation for how to deliver query results.
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryMode {
    /// Buffer all results in memory, return as single JSON response.
    Buffered,
    /// Stream results as NDJSON chunks.
    Streaming,
}

/// Decide delivery mode based on estimated result size.
pub fn recommend_delivery(
    estimated_rows: Option<u64>,
    estimated_bytes: Option<u64>,
    row_threshold: u64,
    byte_threshold: u64,
) -> DeliveryMode {
    if let Some(rows) = estimated_rows {
        if rows > row_threshold {
            return DeliveryMode::Streaming;
        }
    }
    if let Some(bytes) = estimated_bytes {
        if bytes > byte_threshold {
            return DeliveryMode::Streaming;
        }
    }
    DeliveryMode::Buffered
}

/// Default thresholds.
pub const DEFAULT_ROW_THRESHOLD: u64 = 10_000;
pub const DEFAULT_BYTE_THRESHOLD: u64 = 10 * 1024 * 1024; // 10MB

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_result_buffered() {
        assert_eq!(
            recommend_delivery(
                Some(100),
                Some(1024),
                DEFAULT_ROW_THRESHOLD,
                DEFAULT_BYTE_THRESHOLD
            ),
            DeliveryMode::Buffered
        );
    }

    #[test]
    fn test_large_rows_streaming() {
        assert_eq!(
            recommend_delivery(
                Some(50_000),
                None,
                DEFAULT_ROW_THRESHOLD,
                DEFAULT_BYTE_THRESHOLD
            ),
            DeliveryMode::Streaming
        );
    }

    #[test]
    fn test_large_bytes_streaming() {
        assert_eq!(
            recommend_delivery(
                None,
                Some(100 * 1024 * 1024),
                DEFAULT_ROW_THRESHOLD,
                DEFAULT_BYTE_THRESHOLD
            ),
            DeliveryMode::Streaming
        );
    }

    #[test]
    fn test_unknown_size_buffered() {
        assert_eq!(
            recommend_delivery(None, None, DEFAULT_ROW_THRESHOLD, DEFAULT_BYTE_THRESHOLD),
            DeliveryMode::Buffered
        );
    }
}
