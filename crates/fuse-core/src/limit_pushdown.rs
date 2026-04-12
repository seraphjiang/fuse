// SPDX-License-Identifier: Apache-2.0
//! Limit pushdown estimator — calculate per-datasource fetch limits.

/// For a UNION ALL with LIMIT, estimate how many rows to fetch per source.
/// Strategy: fetch LIMIT rows from each source, then merge and truncate.
pub fn union_fetch_limit(global_limit: u64, _source_count: usize) -> u64 {
    // Each source needs to return at least global_limit rows
    // (worst case: all qualifying rows come from one source)
    global_limit
}

/// For a JOIN with LIMIT, estimate build/probe side limits.
/// Build side: fetch all (needed for hash table). Probe side: fetch more than limit.
pub fn join_fetch_limits(global_limit: u64, selectivity: f64) -> (u64, u64) {
    let probe_limit = ((global_limit as f64 / selectivity.max(0.01)) as u64).max(global_limit);
    (u64::MAX, probe_limit) // build=all, probe=estimated
}

/// For a single source with LIMIT + OFFSET, calculate pushdown limit.
pub fn offset_fetch_limit(limit: u64, offset: u64) -> u64 {
    limit.saturating_add(offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_union_fetch() {
        assert_eq!(union_fetch_limit(100, 3), 100);
    }

    #[test]
    fn test_join_fetch() {
        let (build, probe) = join_fetch_limits(100, 0.1);
        assert_eq!(build, u64::MAX);
        assert!(probe >= 100);
    }

    #[test]
    fn test_offset_fetch() {
        assert_eq!(offset_fetch_limit(10, 20), 30);
    }

    #[test]
    fn test_offset_overflow() {
        assert_eq!(offset_fetch_limit(u64::MAX, 1), u64::MAX);
    }

    #[test]
    fn test_join_low_selectivity() {
        let (_, probe) = join_fetch_limits(10, 0.001);
        assert!(probe >= 1000);
    }
}
