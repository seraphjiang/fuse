// SPDX-License-Identifier: Apache-2.0

//! Anomaly detection primitives for time-series data.
//!
//! Post-compute functions that operate on Arrow RecordBatches:
//! - Moving average (sliding window)
//! - Standard deviation (sliding window)
//! - Z-score (how many stddevs from the moving average)
//!
//! Usage: query time-bucketed data, then apply these functions to detect
//! spikes, drops, and statistical outliers.

use std::sync::Arc;

use arrow::array::{Float64Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

/// Compute moving average over a f64 column with given window size.
pub fn moving_average(values: &[f64], window: usize) -> Vec<Option<f64>> {
    if window == 0 { return vec![None; values.len()]; }
    values.iter().enumerate().map(|(i, _)| {
        if i + 1 < window { return None; }
        let start = i + 1 - window;
        let sum: f64 = values[start..=i].iter().sum();
        Some(sum / window as f64)
    }).collect()
}

/// Compute moving standard deviation over a f64 column.
pub fn moving_stddev(values: &[f64], window: usize) -> Vec<Option<f64>> {
    if window < 2 { return vec![None; values.len()]; }
    values.iter().enumerate().map(|(i, _)| {
        if i + 1 < window { return None; }
        let start = i + 1 - window;
        let slice = &values[start..=i];
        let mean = slice.iter().sum::<f64>() / slice.len() as f64;
        let variance = slice.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (slice.len() - 1) as f64;
        Some(variance.sqrt())
    }).collect()
}

/// Compute z-score: (value - moving_avg) / moving_stddev.
/// Values beyond ±threshold are anomalies.
pub fn z_scores(values: &[f64], window: usize) -> Vec<Option<f64>> {
    let avgs = moving_average(values, window);
    let stds = moving_stddev(values, window);
    values.iter().enumerate().map(|(i, v)| {
        match (avgs[i], stds[i]) {
            (Some(avg), Some(std)) if std > 0.0 => Some((v - avg) / std),
            _ => None,
        }
    }).collect()
}

/// Detect anomalies: returns indices where |z-score| > threshold.
pub fn detect_anomalies(values: &[f64], window: usize, threshold: f64) -> Vec<usize> {
    z_scores(values, window).iter().enumerate()
        .filter_map(|(i, z)| match z {
            Some(z) if z.abs() > threshold => Some(i),
            _ => None,
        })
        .collect()
}

/// Add anomaly columns to a RecordBatch.
/// Appends: _moving_avg, _moving_stddev, _z_score, _is_anomaly columns.
pub fn annotate_batch(
    batch: &RecordBatch,
    value_col: &str,
    window: usize,
    threshold: f64,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = batch.schema();
    let col_idx = schema.index_of(value_col)
        .map_err(|_| arrow::error::ArrowError::SchemaError(format!("column '{}' not found", value_col)))?;

    // Extract f64 values
    let col = batch.column(col_idx);
    let values: Vec<f64> = (0..col.len()).map(|i| {
        if col.is_null(i) { 0.0 }
        else {
            arrow::util::display::array_value_to_string(col, i)
                .unwrap_or_default()
                .parse::<f64>()
                .unwrap_or(0.0)
        }
    }).collect();

    let avgs = moving_average(&values, window);
    let stds = moving_stddev(&values, window);
    let zs = z_scores(&values, window);

    // Build new columns
    let avg_arr: ArrayRef = Arc::new(Float64Array::from(avgs));
    let std_arr: ArrayRef = Arc::new(Float64Array::from(stds));
    let z_arr: ArrayRef = Arc::new(Float64Array::from(zs.clone()));
    let anomaly_arr: ArrayRef = Arc::new(arrow::array::BooleanArray::from(
        zs.iter().map(|z| z.map(|v| v.abs() > threshold)).collect::<Vec<_>>()
    ));

    // Extend schema
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("_moving_avg", DataType::Float64, true));
    fields.push(Field::new("_moving_stddev", DataType::Float64, true));
    fields.push(Field::new("_z_score", DataType::Float64, true));
    fields.push(Field::new("_is_anomaly", DataType::Boolean, true));

    let mut columns: Vec<ArrayRef> = (0..batch.num_columns()).map(|i| batch.column(i).clone()).collect();
    columns.extend([avg_arr, std_arr, z_arr, anomaly_arr]);

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

// ── Trend detection ──────────────────────────────────────────────────

/// Linear regression slope over a sliding window.
/// Positive = upward trend, negative = downward trend.
pub fn trend_slope(values: &[f64], window: usize) -> Vec<Option<f64>> {
    if window < 2 { return vec![None; values.len()]; }
    values.iter().enumerate().map(|(i, _)| {
        if i + 1 < window { return None; }
        let start = i + 1 - window;
        let n = window as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean: f64 = values[start..=i].iter().sum::<f64>() / n;
        let mut num = 0.0;
        let mut den = 0.0;
        for (j, &v) in values[start..=i].iter().enumerate() {
            let xd = j as f64 - x_mean;
            num += xd * (v - y_mean);
            den += xd * xd;
        }
        if den.abs() < f64::EPSILON { Some(0.0) } else { Some(num / den) }
    }).collect()
}

/// Detect trend direction: "rising", "falling", or "stable".
pub fn trend_direction(slope: f64, threshold: f64) -> &'static str {
    if slope > threshold { "rising" }
    else if slope < -threshold { "falling" }
    else { "stable" }
}

// ── Seasonal pattern detection ───────────────────────────────────────

/// Detect seasonal (periodic) patterns by computing autocorrelation at
/// candidate periods. Returns (best_period, correlation) or None if no
/// significant periodicity found.
pub fn detect_seasonality(
    values: &[f64],
    min_period: usize,
    max_period: usize,
    min_correlation: f64,
) -> Option<(usize, f64)> {
    if values.len() < max_period * 2 { return None; }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance: f64 = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>();
    if variance.abs() < f64::EPSILON { return None; }

    let mut best = None;
    for period in min_period..=max_period {
        let mut corr = 0.0;
        let n = values.len() - period;
        for i in 0..n {
            corr += (values[i] - mean) * (values[i + period] - mean);
        }
        corr /= variance;
        if corr >= min_correlation {
            match best {
                None => best = Some((period, corr)),
                Some((_, bc)) if corr > bc => best = Some((period, corr)),
                _ => {}
            }
        }
    }
    best
}

/// Remove seasonal component: subtract the average value at each position
/// within the detected period. Returns deseasonalized values.
pub fn deseasonalize(values: &[f64], period: usize) -> Vec<f64> {
    if period == 0 { return values.to_vec(); }
    // Compute seasonal averages per position
    let mut sums = vec![0.0; period];
    let mut counts = vec![0usize; period];
    for (i, &v) in values.iter().enumerate() {
        sums[i % period] += v;
        counts[i % period] += 1;
    }
    let seasonal: Vec<f64> = sums.iter().zip(&counts)
        .map(|(s, &c)| if c > 0 { s / c as f64 } else { 0.0 })
        .collect();
    values.iter().enumerate()
        .map(|(i, &v)| v - seasonal[i % period])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_moving_average_basic() {
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let avgs = moving_average(&vals, 3);
        assert_eq!(avgs[0], None);
        assert_eq!(avgs[1], None);
        assert!((avgs[2].unwrap() - 2.0).abs() < 1e-10); // (1+2+3)/3
        assert!((avgs[3].unwrap() - 3.0).abs() < 1e-10); // (2+3+4)/3
        assert!((avgs[4].unwrap() - 4.0).abs() < 1e-10); // (3+4+5)/3
    }

    #[test]
    fn test_moving_average_window_1() {
        let vals = vec![10.0, 20.0, 30.0];
        let avgs = moving_average(&vals, 1);
        assert_eq!(avgs[0], Some(10.0));
        assert_eq!(avgs[1], Some(20.0));
    }

    #[test]
    fn test_moving_average_window_zero() {
        let vals = vec![1.0, 2.0];
        let avgs = moving_average(&vals, 0);
        assert!(avgs.iter().all(|v| v.is_none()));
    }

    #[test]
    fn test_moving_stddev_basic() {
        let vals = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let stds = moving_stddev(&vals, 4);
        assert!(stds[0].is_none());
        assert!(stds[2].is_none());
        assert!(stds[3].is_some());
        assert!(stds[3].unwrap() > 0.0);
    }

    #[test]
    fn test_moving_stddev_window_1() {
        let vals = vec![1.0, 2.0, 3.0];
        let stds = moving_stddev(&vals, 1);
        assert!(stds.iter().all(|v| v.is_none()));
    }

    #[test]
    fn test_z_scores_spike() {
        let mut vals = vec![10.0; 20];
        vals[15] = 200.0; // large spike
        let zs = z_scores(&vals, 5);
        let spike_z = zs[15].unwrap();
        assert!(spike_z > 1.5, "spike z-score should be > 1.5, got {}", spike_z);
    }

    #[test]
    fn test_z_scores_steady() {
        let vals = vec![5.0; 10];
        let zs = z_scores(&vals, 3);
        // Steady values: z-scores should be 0 or None (stddev=0)
        for z in &zs[2..] {
            assert!(z.is_none() || z.unwrap().abs() < 1e-10);
        }
    }

    #[test]
    fn test_detect_anomalies() {
        let mut vals = vec![10.0; 20];
        vals[15] = 200.0;
        let anomalies = detect_anomalies(&vals, 5, 1.5);
        assert!(anomalies.contains(&15), "should detect spike at index 15");
    }

    #[test]
    fn test_detect_anomalies_none() {
        let vals = vec![10.0; 20];
        let anomalies = detect_anomalies(&vals, 5, 2.0);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn test_annotate_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let mut vals = vec![10.0; 10];
        vals[7] = 50.0; // spike
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(arrow::array::StringArray::from((0..10).map(|i| format!("t{}", i)).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(vals)),
        ]).unwrap();

        let result = annotate_batch(&batch, "value", 3, 2.0).unwrap();
        assert_eq!(result.num_columns(), 6); // 2 original + 4 anomaly
        assert_eq!(result.schema().field(2).name(), "_moving_avg");
        assert_eq!(result.schema().field(3).name(), "_moving_stddev");
        assert_eq!(result.schema().field(4).name(), "_z_score");
        assert_eq!(result.schema().field(5).name(), "_is_anomaly");
    }

    #[test]
    fn test_annotate_batch_missing_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Float64Array::from(vec![1.0])),
        ]).unwrap();
        assert!(annotate_batch(&batch, "nonexistent", 3, 2.0).is_err());
    }

    // ── Trend detection tests ──

    #[test]
    fn test_trend_slope_rising() {
        let vals: Vec<f64> = (0..10).map(|i| i as f64 * 2.0).collect();
        let slopes = trend_slope(&vals, 5);
        // Linear increase of 2.0 per step
        assert!((slopes[4].unwrap() - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_trend_slope_falling() {
        let vals: Vec<f64> = (0..10).map(|i| 100.0 - i as f64 * 3.0).collect();
        let slopes = trend_slope(&vals, 5);
        assert!(slopes[4].unwrap() < -2.0);
    }

    #[test]
    fn test_trend_slope_flat() {
        let vals = vec![5.0; 10];
        let slopes = trend_slope(&vals, 5);
        assert!((slopes[4].unwrap()).abs() < 0.01);
    }

    #[test]
    fn test_trend_direction() {
        assert_eq!(trend_direction(5.0, 1.0), "rising");
        assert_eq!(trend_direction(-5.0, 1.0), "falling");
        assert_eq!(trend_direction(0.5, 1.0), "stable");
    }

    // ── Seasonal detection tests ──

    #[test]
    fn test_detect_seasonality_periodic() {
        // Create a signal with period 7 (weekly pattern)
        let vals: Vec<f64> = (0..70).map(|i| {
            let base = 100.0;
            let seasonal = [10.0, 5.0, 3.0, 2.0, 3.0, 5.0, 15.0]; // weekly
            base + seasonal[i % 7]
        }).collect();
        let result = detect_seasonality(&vals, 2, 14, 0.5);
        assert!(result.is_some());
        let (period, corr) = result.unwrap();
        assert_eq!(period, 7);
        assert!(corr > 0.5);
    }

    #[test]
    fn test_detect_seasonality_none() {
        // Random-ish data with no periodicity
        let vals: Vec<f64> = (0..100).map(|i| (i as f64 * 1.7).sin() * 10.0 + (i as f64 * 0.3).cos() * 5.0).collect();
        let result = detect_seasonality(&vals, 2, 20, 0.9);
        // High threshold should reject weak correlations
        assert!(result.is_none());
    }

    #[test]
    fn test_deseasonalize() {
        let vals: Vec<f64> = (0..12).map(|i| {
            100.0 + [10.0, -5.0, 0.0][i % 3]
        }).collect();
        let deseasoned = deseasonalize(&vals, 3);
        // After removing seasonal component, values should be near 0
        for v in &deseasoned {
            assert!(v.abs() < 0.01, "expected ~0, got {}", v);
        }
    }

    #[test]
    fn test_deseasonalize_period_zero() {
        let vals = vec![1.0, 2.0, 3.0];
        assert_eq!(deseasonalize(&vals, 0), vals);
    }

}
