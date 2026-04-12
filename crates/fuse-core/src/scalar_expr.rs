// SPDX-License-Identifier: Apache-2.0
//! Scalar expressions — typed expressions for query planning.

use serde::Serialize;

/// A scalar expression in a query plan.
#[derive(Debug, Clone, Serialize)]
pub enum ScalarExpr {
    Column(String),
    Literal(String),
    BinaryOp {
        left: Box<ScalarExpr>,
        op: String,
        right: Box<ScalarExpr>,
    },
    Function {
        name: String,
        args: Vec<ScalarExpr>,
    },
    Star,
}

impl ScalarExpr {
    pub fn col(name: &str) -> Self {
        Self::Column(name.into())
    }
    pub fn lit(val: &str) -> Self {
        Self::Literal(val.into())
    }
    pub fn star() -> Self {
        Self::Star
    }

    pub fn eq(self, other: ScalarExpr) -> Self {
        Self::BinaryOp {
            left: Box::new(self),
            op: "=".into(),
            right: Box::new(other),
        }
    }

    pub fn gt(self, other: ScalarExpr) -> Self {
        Self::BinaryOp {
            left: Box::new(self),
            op: ">".into(),
            right: Box::new(other),
        }
    }

    pub fn func(name: &str, args: Vec<ScalarExpr>) -> Self {
        Self::Function {
            name: name.into(),
            args,
        }
    }

    /// Convert to SQL string.
    pub fn to_sql(&self) -> String {
        match self {
            Self::Column(c) => c.clone(),
            Self::Literal(v) => format!("'{}'", v.replace('\'', "''")),
            Self::BinaryOp { left, op, right } => {
                format!("{} {} {}", left.to_sql(), op, right.to_sql())
            }
            Self::Function { name, args } => {
                let arg_sql: Vec<String> = args.iter().map(|a| a.to_sql()).collect();
                format!("{}({})", name, arg_sql.join(", "))
            }
            Self::Star => "*".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column() {
        assert_eq!(ScalarExpr::col("id").to_sql(), "id");
    }

    #[test]
    fn test_literal() {
        assert_eq!(ScalarExpr::lit("hello").to_sql(), "'hello'");
    }

    #[test]
    fn test_binary_op() {
        let expr = ScalarExpr::col("age").gt(ScalarExpr::lit("18"));
        assert_eq!(expr.to_sql(), "age > '18'");
    }

    #[test]
    fn test_function() {
        let expr = ScalarExpr::func("COUNT", vec![ScalarExpr::star()]);
        assert_eq!(expr.to_sql(), "COUNT(*)");
    }

    #[test]
    fn test_eq() {
        let expr = ScalarExpr::col("name").eq(ScalarExpr::lit("alice"));
        assert_eq!(expr.to_sql(), "name = 'alice'");
    }
}
