// SPDX-License-Identifier: Apache-2.0
//! Data type mapping — map between connector-specific and Arrow types.

use serde::Serialize;

/// Unified data type for cross-connector compatibility.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Timestamp,
    Date,
    Null,
    Unknown(String),
}

/// Map a type name string to a DataType.
pub fn from_type_name(name: &str) -> DataType {
    match name.to_lowercase().as_str() {
        "bool" | "boolean" => DataType::Boolean,
        "int" | "int32" | "integer" => DataType::Int32,
        "bigint" | "int64" | "long" => DataType::Int64,
        "float" | "float32" | "real" => DataType::Float32,
        "double" | "float64" | "numeric" | "decimal" => DataType::Float64,
        "text" | "varchar" | "string" | "utf8" | "keyword" => DataType::Utf8,
        "binary" | "bytea" | "blob" => DataType::Binary,
        "timestamp" | "datetime" | "timestamptz" => DataType::Timestamp,
        "date" => DataType::Date,
        "null" => DataType::Null,
        other => DataType::Unknown(other.to_string()),
    }
}

/// Check if two types are compatible for UNION.
pub fn types_compatible(a: &DataType, b: &DataType) -> bool {
    if a == b {
        return true;
    }
    matches!(
        (a, b),
        (DataType::Int32, DataType::Int64)
            | (DataType::Int64, DataType::Int32)
            | (DataType::Float32, DataType::Float64)
            | (DataType::Float64, DataType::Float32)
            | (
                DataType::Int32 | DataType::Int64,
                DataType::Float32 | DataType::Float64
            )
            | (
                DataType::Float32 | DataType::Float64,
                DataType::Int32 | DataType::Int64
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_type_name() {
        assert_eq!(from_type_name("varchar"), DataType::Utf8);
        assert_eq!(from_type_name("BIGINT"), DataType::Int64);
        assert_eq!(from_type_name("boolean"), DataType::Boolean);
        assert_eq!(from_type_name("timestamp"), DataType::Timestamp);
    }

    #[test]
    fn test_unknown_type() {
        assert!(matches!(from_type_name("geometry"), DataType::Unknown(_)));
    }

    #[test]
    fn test_compatible_same() {
        assert!(types_compatible(&DataType::Utf8, &DataType::Utf8));
    }

    #[test]
    fn test_compatible_numeric_widening() {
        assert!(types_compatible(&DataType::Int32, &DataType::Int64));
        assert!(types_compatible(&DataType::Float32, &DataType::Float64));
    }

    #[test]
    fn test_incompatible() {
        assert!(!types_compatible(&DataType::Utf8, &DataType::Int32));
        assert!(!types_compatible(&DataType::Boolean, &DataType::Float64));
    }
}
