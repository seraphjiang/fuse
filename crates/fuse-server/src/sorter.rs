// SPDX-License-Identifier: Apache-2.0
//! Result sorter — sort query results by column.

use serde_json::Value;

/// Sort rows by a column index. Nulls always sort last.
pub fn sort_by_column(rows: &mut [Vec<Value>], col_idx: usize, descending: bool) {
    rows.sort_by(|a, b| {
        let va = a.get(col_idx);
        let vb = b.get(col_idx);
        match (is_null(va), is_null(vb)) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            (false, false) => {
                let cmp = compare_values(va, vb);
                if descending { cmp.reverse() } else { cmp }
            }
        }
    });
}

fn is_null(v: Option<&Value>) -> bool {
    matches!(v, None | Some(Value::Null))
}

fn compare_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) | (Some(Value::Null), Some(Value::Null)) => std::cmp::Ordering::Equal,
        (None | Some(Value::Null), _) => std::cmp::Ordering::Greater, // nulls last
        (_, None | Some(Value::Null)) => std::cmp::Ordering::Less,
        (Some(Value::Number(a)), Some(Value::Number(b))) => {
            a.as_f64().unwrap_or(0.0).partial_cmp(&b.as_f64().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        (Some(a), Some(b)) => a.to_string().cmp(&b.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_sort_asc() {
        let mut rows = vec![vec![json!(3)], vec![json!(1)], vec![json!(2)]];
        sort_by_column(&mut rows, 0, false);
        assert_eq!(rows[0][0], json!(1));
        assert_eq!(rows[2][0], json!(3));
    }

    #[test]
    fn test_sort_desc() {
        let mut rows = vec![vec![json!(1)], vec![json!(3)], vec![json!(2)]];
        sort_by_column(&mut rows, 0, true);
        assert_eq!(rows[0][0], json!(3));
    }

    #[test]
    fn test_sort_strings() {
        let mut rows = vec![vec![json!("c")], vec![json!("a")], vec![json!("b")]];
        sort_by_column(&mut rows, 0, false);
        assert_eq!(rows[0][0], json!("a"));
    }

    #[test]
    fn test_nulls_last() {
        let mut rows = vec![vec![json!(null)], vec![json!(1)], vec![json!(2)]];
        sort_by_column(&mut rows, 0, false);
        assert_eq!(rows[0][0], json!(1));
        assert_eq!(rows[2][0], json!(null));
    }
}
