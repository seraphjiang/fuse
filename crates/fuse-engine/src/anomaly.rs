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
}
