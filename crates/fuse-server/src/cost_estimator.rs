// SPDX-License-Identifier: Apache-2.0

//! Query cost estimation with real dollar amounts (#1803).
//!
//! Estimate execution cost before running: Athena $/GB scanned,
//! DynamoDB $/RCU, S3 $/request, BigQuery $/TB processed, etc.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct CostEstimate {
    pub datasource: String,
    pub connector_type: String,
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    pub estimated_cost_usd: f64,
    pub cost_breakdown: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryCostEstimate {
    pub total_cost_usd: f64,
    pub per_datasource: Vec<CostEstimate>,
}

/// Per-connector cost model.
pub fn estimate_cost(
    connector_type: &str,
    datasource: &str,
    estimated_rows: u64,
    estimated_bytes: u64,
) -> CostEstimate {
    let (cost, breakdown) = match connector_type {
        "athena" => {
            let tb = estimated_bytes as f64 / (1024.0 * 1024.0 * 1024.0 * 1024.0);
            let cost = tb * 5.0; // $5/TB
            (cost, format!("{:.4} TB scanned × $5/TB", tb))
        }
        "bigquery" => {
            let tb = estimated_bytes as f64 / (1024.0 * 1024.0 * 1024.0 * 1024.0);
            let cost = tb * 6.25; // $6.25/TB on-demand
            (cost, format!("{:.4} TB processed × $6.25/TB", tb))
        }
        "dynamodb" => {
            let rcus = (estimated_rows as f64 / 4.0).ceil(); // 4KB items, eventually consistent
            let cost = rcus * 0.00000025; // $0.25 per million RCU
            (cost, format!("{:.0} RCUs × $0.25/M", rcus))
        }
        "s3" | "s3-o11y" => {
            let requests = (estimated_bytes as f64 / (64.0 * 1024.0 * 1024.0))
                .ceil()
                .max(1.0);
            let cost = requests * 0.0004; // $0.0004/GET
            (cost, format!("{:.0} GET requests × $0.0004", requests))
        }
        "cloudwatch" => {
            let gb = estimated_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            let cost = gb * 0.005; // $0.005/GB scanned
            (cost, format!("{:.2} GB scanned × $0.005/GB", gb))
        }
        "timestream" => {
            let gb = estimated_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            let cost = gb * 0.01; // $10/TB = $0.01/GB
            (cost, format!("{:.2} GB scanned × $0.01/GB", gb))
        }
        "snowflake" => {
            let credits = (estimated_rows as f64 / 100_000.0).ceil().max(1.0) * 0.01;
            let cost = credits * 3.0; // ~$3/credit
            (cost, format!("{:.2} credits × $3/credit", credits))
        }
        _ => (0.0, "no cost model (self-hosted)".into()),
    };

    CostEstimate {
        datasource: datasource.into(),
        connector_type: connector_type.into(),
        estimated_rows,
        estimated_bytes,
        estimated_cost_usd: cost,
        cost_breakdown: breakdown,
    }
}

/// Estimate total cost for a multi-datasource query.
pub fn estimate_query_cost(datasources: &[(&str, &str, u64, u64)]) -> QueryCostEstimate {
    let per_datasource: Vec<CostEstimate> = datasources
        .iter()
        .map(|(ds, ct, rows, bytes)| estimate_cost(ct, ds, *rows, *bytes))
        .collect();
    let total = per_datasource.iter().map(|e| e.estimated_cost_usd).sum();
    QueryCostEstimate {
        total_cost_usd: total,
        per_datasource,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_athena_cost() {
        let e = estimate_cost("athena", "my_athena", 1000, 1_073_741_824); // 1GB
                                                                           // $5/TB = $0.005/GB → 1GB ≈ $0.005
        assert!((e.estimated_cost_usd - 0.005).abs() < 0.001);
    }

    #[test]
    fn test_athena_cost_1gb() {
        let e = estimate_cost("athena", "a", 0, 1_073_741_824);
        // $5/TB ≈ $0.00488/GB. 1GB ≈ $0.00488
        assert!(e.estimated_cost_usd > 0.0);
        assert!(e.cost_breakdown.contains("TB scanned"));
    }

    #[test]
    fn test_dynamodb_cost() {
        let e = estimate_cost("dynamodb", "ddb", 4000, 0);
        assert!(e.estimated_cost_usd > 0.0);
        assert!(e.cost_breakdown.contains("RCU"));
    }

    #[test]
    fn test_s3_cost() {
        let e = estimate_cost("s3", "my_s3", 0, 128 * 1024 * 1024); // 128MB
        assert!(e.estimated_cost_usd > 0.0);
        assert!(e.cost_breakdown.contains("GET"));
    }

    #[test]
    fn test_unknown_connector_free() {
        let e = estimate_cost("postgres", "pg", 1000, 1000);
        assert_eq!(e.estimated_cost_usd, 0.0);
        assert!(e.cost_breakdown.contains("self-hosted"));
    }

    #[test]
    fn test_multi_datasource_total() {
        let est = estimate_query_cost(&[
            ("athena_ds", "athena", 1000, 1_073_741_824),
            ("ddb_ds", "dynamodb", 500, 0),
            ("pg_ds", "postgres", 1000, 0),
        ]);
        assert_eq!(est.per_datasource.len(), 3);
        assert!(est.total_cost_usd > 0.0);
    }

    #[test]
    fn test_bigquery_cost() {
        let e = estimate_cost("bigquery", "bq", 0, 1_099_511_627_776); // 1TB
        assert!((e.estimated_cost_usd - 6.25).abs() < 0.01);
    }

    #[test]
    fn test_snowflake_cost() {
        let e = estimate_cost("snowflake", "sf", 100_000, 0);
        assert!(e.estimated_cost_usd > 0.0);
        assert!(e.cost_breakdown.contains("credit"));
    }

    #[test]
    fn test_athena_cost_is_5_per_tb_not_per_gb() {
        // 1TB scanned should cost exactly $5
        let e = estimate_cost("athena", "a", 0, 1_099_511_627_776); // 1TB
        assert!((e.estimated_cost_usd - 5.0).abs() < 0.1);
    }

    #[test]
    fn test_zero_bytes_zero_cost() {
        let e = estimate_cost("athena", "a", 0, 0);
        assert_eq!(e.estimated_cost_usd, 0.0);
    }

    #[test]
    fn test_cloudwatch_cost() {
        let e = estimate_cost("cloudwatch", "cw", 0, 1_073_741_824); // 1GB
        assert!((e.estimated_cost_usd - 0.005).abs() < 0.001);
        assert!(e.cost_breakdown.contains("GB scanned"));
    }

    #[test]
    fn test_timestream_cost() {
        let e = estimate_cost("timestream", "ts", 0, 1_073_741_824); // 1GB
        assert!((e.estimated_cost_usd - 0.01).abs() < 0.001);
    }

    #[test]
    fn test_query_cost_all_free_returns_zero() {
        let est =
            estimate_query_cost(&[("pg", "postgres", 1000, 1000), ("redis", "redis", 500, 500)]);
        assert_eq!(est.total_cost_usd, 0.0);
        assert_eq!(est.per_datasource.len(), 2);
    }

    #[test]
    fn test_cost_estimate_serializes() {
        let e = estimate_cost("athena", "a", 100, 1_000_000);
        let json = serde_json::to_value(&e).unwrap();
        assert!(json["estimated_cost_usd"].is_number());
        assert!(json["cost_breakdown"].is_string());
        assert_eq!(json["connector_type"], "athena");
    }

    #[test]
    fn test_query_cost_estimate_serializes() {
        let est = estimate_query_cost(&[("a", "athena", 100, 1_000_000)]);
        let json = serde_json::to_value(&est).unwrap();
        assert!(json["total_cost_usd"].is_number());
        assert!(json["per_datasource"].is_array());
    }
}
