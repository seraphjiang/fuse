// SPDX-License-Identifier: Apache-2.0
//! Offset-based pagination — LIMIT/OFFSET for result sets.

use serde_json::Value;

/// Apply LIMIT and OFFSET to rows.
pub fn paginate(rows: &[Vec<Value>], offset: usize, limit: usize) -> Vec<Vec<Value>> {
    rows.iter().skip(offset).take(limit).cloned().collect()
}

/// Calculate pagination metadata.
pub fn page_info(total: usize, offset: usize, limit: usize) -> PageInfo {
    let page = if limit > 0 { offset / limit + 1 } else { 1 };
    let total_pages = if limit > 0 { (total + limit - 1) / limit } else { 1 };
    PageInfo {
        page,
        total_pages,
        total_rows: total,
        has_next: offset + limit < total,
        has_prev: offset > 0,
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PageInfo {
    pub page: usize,
    pub total_pages: usize,
    pub total_rows: usize,
    pub has_next: bool,
    pub has_prev: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows(n: usize) -> Vec<Vec<Value>> {
        (0..n).map(|i| vec![json!(i)]).collect()
    }

    #[test]
    fn test_paginate_first_page() {
        let r = paginate(&rows(20), 0, 5);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][0], json!(0));
    }

    #[test]
    fn test_paginate_second_page() {
        let r = paginate(&rows(20), 5, 5);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][0], json!(5));
    }

    #[test]
    fn test_paginate_last_page() {
        let r = paginate(&rows(12), 10, 5);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn test_page_info() {
        let info = page_info(100, 0, 10);
        assert_eq!(info.page, 1);
        assert_eq!(info.total_pages, 10);
        assert!(info.has_next);
        assert!(!info.has_prev);
    }

    #[test]
    fn test_page_info_last() {
        let info = page_info(100, 90, 10);
        assert_eq!(info.page, 10);
        assert!(!info.has_next);
        assert!(info.has_prev);
    }
}
