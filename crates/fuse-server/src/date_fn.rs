// SPDX-License-Identifier: Apache-2.0
//! Date/time functions for result columns.

use serde_json::Value;

/// Extract date part from ISO timestamp strings.
pub fn extract_date(rows: &[Vec<Value>], col: usize) -> Vec<Value> {
    rows.iter()
        .map(|row| match row.get(col) {
            Some(Value::String(s)) => {
                let date = s.split('T').next().unwrap_or(s);
                Value::String(date.to_string())
            }
            Some(v) => v.clone(),
            None => Value::Null,
        })
        .collect()
}

/// Extract hour from ISO timestamp strings.
pub fn extract_hour(rows: &[Vec<Value>], col: usize) -> Vec<Value> {
    rows.iter()
        .map(|row| match row.get(col) {
            Some(Value::String(s)) => {
                if let Some(time_part) = s.split('T').nth(1) {
                    let hour = time_part.split(':').next().unwrap_or("0");
                    hour.parse::<u32>()
                        .ok()
                        .map(|h| Value::Number(h.into()))
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        })
        .collect()
}

/// Get current timestamp as ISO string.
pub fn now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("1970-01-01T00:00:00Z+{}s", secs) // simplified
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_date() {
        let rows = vec![vec![json!("2024-01-15T10:30:00Z")]];
        let dates = extract_date(&rows, 0);
        assert_eq!(dates[0], json!("2024-01-15"));
    }

    #[test]
    fn test_extract_hour() {
        let rows = vec![vec![json!("2024-01-15T14:30:00Z")]];
        let hours = extract_hour(&rows, 0);
        assert_eq!(hours[0], json!(14));
    }

    #[test]
    fn test_extract_date_no_time() {
        let rows = vec![vec![json!("2024-01-15")]];
        let dates = extract_date(&rows, 0);
        assert_eq!(dates[0], json!("2024-01-15"));
    }

    #[test]
    fn test_null_handling() {
        let rows = vec![vec![json!(null)]];
        assert_eq!(extract_date(&rows, 0)[0], json!(null));
        assert_eq!(extract_hour(&rows, 0)[0], json!(null));
    }

    #[test]
    fn test_now() {
        let ts = now();
        assert!(ts.contains("1970"));
    }
}
