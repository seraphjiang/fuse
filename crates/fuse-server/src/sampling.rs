// SPDX-License-Identifier: Apache-2.0
//! Query result sampling — return a random subset of rows.

use serde_json::Value;

/// Sample `n` rows from a result set using reservoir sampling.
pub fn sample_rows(rows: &[Vec<Value>], n: usize) -> Vec<Vec<Value>> {
    if rows.len() <= n {
        return rows.to_vec();
    }
    // Deterministic reservoir sampling (seeded by row count for reproducibility)
    let mut reservoir: Vec<Vec<Value>> = rows[..n].to_vec();
    let mut seed: u64 = rows.len() as u64;
    for (i, row) in rows.iter().enumerate().skip(n) {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (seed % (i as u64 + 1)) as usize;
        if j < n {
            reservoir[j] = row.clone();
        }
    }
    reservoir
}

/// Take first and last `n` rows (head + tail preview).
pub fn head_tail(rows: &[Vec<Value>], n: usize) -> (Vec<Vec<Value>>, Vec<Vec<Value>>) {
    let head = rows.iter().take(n).cloned().collect();
    let tail = if rows.len() > n {
        rows.iter().rev().take(n).rev().cloned().collect()
    } else {
        vec![]
    };
    (head, tail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_rows(n: usize) -> Vec<Vec<Value>> {
        (0..n).map(|i| vec![json!(i)]).collect()
    }

    #[test]
    fn test_sample_small() {
        let rows = make_rows(5);
        let sampled = sample_rows(&rows, 10);
        assert_eq!(sampled.len(), 5);
    }

    #[test]
    fn test_sample_exact() {
        let rows = make_rows(100);
        let sampled = sample_rows(&rows, 10);
        assert_eq!(sampled.len(), 10);
    }

    #[test]
    fn test_sample_deterministic() {
        let rows = make_rows(100);
        let s1 = sample_rows(&rows, 5);
        let s2 = sample_rows(&rows, 5);
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_head_tail() {
        let rows = make_rows(10);
        let (head, tail) = head_tail(&rows, 3);
        assert_eq!(head.len(), 3);
        assert_eq!(tail.len(), 3);
        assert_eq!(head[0][0], json!(0));
        assert_eq!(tail[2][0], json!(9));
    }

    #[test]
    fn test_head_tail_small() {
        let rows = make_rows(2);
        let (head, tail) = head_tail(&rows, 5);
        assert_eq!(head.len(), 2);
        assert!(tail.is_empty());
    }
}
