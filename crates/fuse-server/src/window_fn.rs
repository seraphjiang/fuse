// SPDX-License-Identifier: Apache-2.0
//! Post-execution window functions — ROW_NUMBER, RANK.

use serde_json::Value;

/// Add ROW_NUMBER column to rows (1-based sequential).
pub fn add_row_number(rows: &[Vec<Value>], col_name: &str) -> (Vec<Value>, Vec<Vec<Value>>) {
    let header = Value::String(col_name.to_string());
    let numbered: Vec<Vec<Value>> = rows.iter().enumerate().map(|(i, row)| {
        let mut new_row = vec![Value::Number((i as u64 + 1).into())];
        new_row.extend(row.iter().cloned());
        new_row
    }).collect();
    (vec![header], numbered)
}

/// Add RANK column based on a sort column (same values get same rank).
pub fn add_rank(rows: &[Vec<Value>], sort_col: usize) -> Vec<Vec<Value>> {
    if rows.is_empty() { return vec![]; }
    let mut result = Vec::with_capacity(rows.len());
    let mut rank = 1u64;
    let mut prev: Option<String> = None;
    for (i, row) in rows.iter().enumerate() {
        let val = row.get(sort_col).map(|v| v.to_string());
        if i > 0 && val != prev {
            rank = i as u64 + 1;
        }
        let mut new_row = vec![Value::Number(rank.into())];
        new_row.extend(row.iter().cloned());
        result.push(new_row);
        prev = val;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_row_number() {
        let rows = vec![vec![json!("a")], vec![json!("b")], vec![json!("c")]];
        let (_, numbered) = add_row_number(&rows, "rn");
        assert_eq!(numbered[0][0], json!(1));
        assert_eq!(numbered[2][0], json!(3));
        assert_eq!(numbered[0][1], json!("a"));
    }

    #[test]
    fn test_rank_distinct() {
        let rows = vec![vec![json!(100)], vec![json!(90)], vec![json!(80)]];
        let ranked = add_rank(&rows, 0);
        assert_eq!(ranked[0][0], json!(1));
        assert_eq!(ranked[1][0], json!(2));
        assert_eq!(ranked[2][0], json!(3));
    }

    #[test]
    fn test_rank_ties() {
        let rows = vec![vec![json!(100)], vec![json!(100)], vec![json!(80)]];
        let ranked = add_rank(&rows, 0);
        assert_eq!(ranked[0][0], json!(1));
        assert_eq!(ranked[1][0], json!(1)); // tie
        assert_eq!(ranked[2][0], json!(3)); // skip rank 2
    }

    #[test]
    fn test_empty() {
        let (_, numbered) = add_row_number(&[], "rn");
        assert!(numbered.is_empty());
        assert!(add_rank(&[], 0).is_empty());
    }

    #[test]
    fn test_row_number_preserves_columns() {
        let rows = vec![
            vec![json!("alice"), json!(100)],
            vec![json!("bob"), json!(200)],
        ];
        let (header, numbered) = add_row_number(&rows, "row_num");
        assert_eq!(header[0], json!("row_num"));
        assert_eq!(numbered[0].len(), 3); // rn + 2 original cols
        assert_eq!(numbered[0][1], json!("alice"));
        assert_eq!(numbered[1][2], json!(200));
    }

    #[test]
    fn test_rank_all_ties() {
        let rows = vec![vec![json!(50)], vec![json!(50)], vec![json!(50)]];
        let ranked = add_rank(&rows, 0);
        assert_eq!(ranked[0][0], json!(1));
        assert_eq!(ranked[1][0], json!(1));
        assert_eq!(ranked[2][0], json!(1));
    }

    #[test]
    fn test_rank_single_row() {
        let rows = vec![vec![json!(42)]];
        let ranked = add_rank(&rows, 0);
        assert_eq!(ranked[0][0], json!(1));
        assert_eq!(ranked[0][1], json!(42));
    }

    #[test]
    fn test_rank_string_values() {
        let rows = vec![vec![json!("a")], vec![json!("a")], vec![json!("b")], vec![json!("c")]];
        let ranked = add_rank(&rows, 0);
        assert_eq!(ranked[0][0], json!(1));
        assert_eq!(ranked[1][0], json!(1)); // tie
        assert_eq!(ranked[2][0], json!(3)); // skip 2
        assert_eq!(ranked[3][0], json!(4));
    }

    #[test]
    fn test_row_number_large_dataset() {
        let rows: Vec<Vec<Value>> = (0..1000).map(|i| vec![json!(i)]).collect();
        let (_, numbered) = add_row_number(&rows, "rn");
        assert_eq!(numbered.len(), 1000);
        assert_eq!(numbered[0][0], json!(1));
        assert_eq!(numbered[999][0], json!(1000));
    }

    #[test]
    fn test_rank_alternating_ties() {
        // Pattern: a, a, b, b, c, c
        let rows = vec![
            vec![json!(1)], vec![json!(1)],
            vec![json!(2)], vec![json!(2)],
            vec![json!(3)], vec![json!(3)],
        ];
        let ranked = add_rank(&rows, 0);
        assert_eq!(ranked[0][0], json!(1));
        assert_eq!(ranked[1][0], json!(1));
        assert_eq!(ranked[2][0], json!(3));
        assert_eq!(ranked[3][0], json!(3));
        assert_eq!(ranked[4][0], json!(5));
        assert_eq!(ranked[5][0], json!(5));
    }
}
