// SPDX-License-Identifier: Apache-2.0
//! Math functions — ABS, ROUND, CEIL, FLOOR, MOD for result columns.

use serde_json::Value;

/// Apply a math function to a numeric column.
pub fn apply_math(rows: &mut [Vec<Value>], col: usize, func: MathFn) {
    for row in rows.iter_mut() {
        if let Some(val) = row.get(col).and_then(|v| v.as_f64()) {
            let result = match func {
                MathFn::Abs => val.abs(),
                MathFn::Round => val.round(),
                MathFn::Ceil => val.ceil(),
                MathFn::Floor => val.floor(),
            };
            if let Some(n) = serde_json::Number::from_f64(result) {
                row[col] = Value::Number(n);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MathFn { Abs, Round, Ceil, Floor }

/// Modulo operation on a column.
pub fn modulo(rows: &[Vec<Value>], col: usize, divisor: f64) -> Vec<Value> {
    rows.iter().map(|row| {
        row.get(col).and_then(|v| v.as_f64())
            .and_then(|n| serde_json::Number::from_f64(n % divisor).map(Value::Number))
            .unwrap_or(Value::Null)
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_abs() {
        let mut rows = vec![vec![json!(-5.0)], vec![json!(3.0)]];
        apply_math(&mut rows, 0, MathFn::Abs);
        assert_eq!(rows[0][0], json!(5.0));
        assert_eq!(rows[1][0], json!(3.0));
    }

    #[test]
    fn test_round() {
        let mut rows = vec![vec![json!(3.7)], vec![json!(3.2)]];
        apply_math(&mut rows, 0, MathFn::Round);
        assert_eq!(rows[0][0], json!(4.0));
        assert_eq!(rows[1][0], json!(3.0));
    }

    #[test]
    fn test_ceil_floor() {
        let mut rows = vec![vec![json!(3.2)]];
        let mut rows2 = rows.clone();
        apply_math(&mut rows, 0, MathFn::Ceil);
        apply_math(&mut rows2, 0, MathFn::Floor);
        assert_eq!(rows[0][0], json!(4.0));
        assert_eq!(rows2[0][0], json!(3.0));
    }

    #[test]
    fn test_modulo() {
        let rows = vec![vec![json!(10)], vec![json!(7)]];
        let result = modulo(&rows, 0, 3.0);
        assert_eq!(result[0], json!(1.0));
        assert_eq!(result[1], json!(1.0));
    }

    #[test]
    fn test_null_passthrough() {
        let mut rows = vec![vec![json!(null)]];
        apply_math(&mut rows, 0, MathFn::Abs);
        assert_eq!(rows[0][0], json!(null));
    }
}
