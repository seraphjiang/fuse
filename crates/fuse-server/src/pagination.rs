// SPDX-License-Identifier: Apache-2.0
//! Pagination metadata for query results.

use serde::Serialize;

/// Pagination info included in query responses.
#[derive(Debug, Clone, Serialize)]
pub struct PaginationMeta {
    pub page_size: usize,
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_number: Option<u32>,
}

impl PaginationMeta {
    pub fn single_page(row_count: usize) -> Self {
        Self {
            page_size: row_count,
            has_more: false,
            next_cursor: None,
            total_rows: Some(row_count as u64),
            page_number: Some(1),
        }
    }

    pub fn with_cursor(page_size: usize, cursor: String, total: Option<u64>) -> Self {
        Self {
            page_size,
            has_more: true,
            next_cursor: Some(cursor),
            total_rows: total,
            page_number: None,
        }
    }

    pub fn last_page(page_size: usize, total: u64) -> Self {
        Self {
            page_size,
            has_more: false,
            next_cursor: None,
            total_rows: Some(total),
            page_number: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_page() {
        let p = PaginationMeta::single_page(42);
        assert!(!p.has_more);
        assert_eq!(p.total_rows, Some(42));
        assert!(p.next_cursor.is_none());
    }

    #[test]
    fn test_with_cursor() {
        let p = PaginationMeta::with_cursor(20, "abc".into(), Some(100));
        assert!(p.has_more);
        assert_eq!(p.next_cursor.as_deref(), Some("abc"));
    }

    #[test]
    fn test_last_page() {
        let p = PaginationMeta::last_page(15, 95);
        assert!(!p.has_more);
        assert_eq!(p.total_rows, Some(95));
    }

    #[test]
    fn test_serialization() {
        let p = PaginationMeta::single_page(10);
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"has_more\":false"));
        assert!(!json.contains("next_cursor")); // skip_serializing_if
    }
}
