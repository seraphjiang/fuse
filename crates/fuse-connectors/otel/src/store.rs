// SPDX-License-Identifier: Apache-2.0
//! In-memory ring-buffer store for OTel spans, metrics, and logs.
//!
//! Each signal type has a bounded VecDeque that evicts oldest entries
//! when capacity is reached. Data is stored as flat rows and converted
//! to Arrow RecordBatches on query.

use std::collections::VecDeque;
use std::sync::Mutex;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

/// Maximum default capacity per signal.
pub const DEFAULT_MAX_ENTRIES: usize = 100_000;

// ── Schemas ──────────────────────────────────────────────────

pub fn spans_schema() -> Schema {
    Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("operation", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("start_time_ns", DataType::Int64, false),
        Field::new("end_time_ns", DataType::Int64, false),
        Field::new("duration_ns", DataType::Int64, false),
        Field::new("attributes", DataType::Utf8, true),
    ])
}

pub fn metrics_schema() -> Schema {
    Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("unit", DataType::Utf8, true),
        Field::new("metric_type", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("labels", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
    ])
}

pub fn logs_schema() -> Schema {
    Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
    ])
}

// ── Row types ────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct SpanRow {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    service_name: String,
    operation: String,
    status: String,
    start_time_ns: i64,
    end_time_ns: i64,
    duration_ns: i64,
    attributes: Option<String>,
}

#[derive(Debug, Clone)]
struct MetricRow {
    name: String,
    description: Option<String>,
    unit: Option<String>,
    metric_type: String,
    value: String,
    timestamp_ns: i64,
    labels: Option<String>,
    service_name: Option<String>,
}

#[derive(Debug, Clone)]
struct LogRow {
    timestamp_ns: i64,
    severity: String,
    body: String,
    service_name: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    attributes: Option<String>,
}

// ── Filter ───────────────────────────────────────────────────

/// Optional time-range and service filter pushed down from SQL WHERE clauses.
#[derive(Debug, Clone, Default)]
pub struct OtelFilter {
    pub service_name: Option<String>,
    pub min_time_ns: Option<i64>,
    pub max_time_ns: Option<i64>,
}

// ── Store ────────────────────────────────────────────────────

/// Thread-safe bounded store for OTel data.
#[derive(Debug)]
pub struct OtelStore {
    spans: Mutex<VecDeque<SpanRow>>,
    metrics: Mutex<VecDeque<MetricRow>>,
    logs: Mutex<VecDeque<LogRow>>,
    max_spans: usize,
    max_metrics: usize,
    max_logs: usize,
}

impl OtelStore {
    pub fn new(max_spans: usize, max_metrics: usize, max_logs: usize) -> Self {
        Self {
            spans: Mutex::new(VecDeque::new()),
            metrics: Mutex::new(VecDeque::new()),
            logs: Mutex::new(VecDeque::new()),
            max_spans,
            max_metrics,
            max_logs,
        }
    }

    /// Returns (span_count, metric_count, log_count).
    pub fn counts(&self) -> (usize, usize, usize) {
        let s = self.spans.lock().unwrap().len();
        let m = self.metrics.lock().unwrap().len();
        let l = self.logs.lock().unwrap().len();
        (s, m, l)
    }

    // ── Ingestion ────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub fn ingest_span(
        &self, trace_id: &str, span_id: &str, parent_span_id: Option<&str>,
        service_name: &str, operation: &str, status: &str,
        start_time_ns: i64, end_time_ns: i64, attributes: &str,
    ) {
        let duration_ns = end_time_ns.saturating_sub(start_time_ns);
        let mut spans = self.spans.lock().unwrap();
        if spans.len() >= self.max_spans {
            spans.pop_front();
        }
        spans.push_back(SpanRow {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            parent_span_id: parent_span_id.map(Into::into),
            service_name: service_name.into(),
            operation: operation.into(),
            status: status.into(),
            start_time_ns,
            end_time_ns,
            duration_ns,
            attributes: if attributes.is_empty() { None } else { Some(attributes.into()) },
        });
    }

    pub fn ingest_metric(
        &self, name: &str, description: Option<&str>, unit: Option<&str>,
        metric_type: &str, value: &str, timestamp_ns: i64,
        labels: Option<&str>, service_name: Option<&str>,
    ) {
        let mut metrics = self.metrics.lock().unwrap();
        if metrics.len() >= self.max_metrics {
            metrics.pop_front();
        }
        metrics.push_back(MetricRow {
            name: name.into(),
            description: description.map(Into::into),
            unit: unit.map(Into::into),
            metric_type: metric_type.into(),
            value: value.into(),
            timestamp_ns,
            labels: labels.map(Into::into),
            service_name: service_name.map(Into::into),
        });
    }

    pub fn ingest_log(
        &self, timestamp_ns: i64, severity: &str, body: &str,
        service_name: Option<&str>, trace_id: Option<&str>,
        span_id: Option<&str>, attributes: Option<&str>,
    ) {
        let mut logs = self.logs.lock().unwrap();
        if logs.len() >= self.max_logs {
            logs.pop_front();
        }
        logs.push_back(LogRow {
            timestamp_ns,
            severity: severity.into(),
            body: body.into(),
            service_name: service_name.map(Into::into),
            trace_id: trace_id.map(Into::into),
            span_id: span_id.map(Into::into),
            attributes: attributes.map(Into::into),
        });
    }

    // ── Query ────────────────────────────────────────────────

    pub fn query_spans(&self, limit: Option<u64>) -> Option<RecordBatch> {
        self.query_spans_filtered(limit, &OtelFilter::default())
    }

    pub fn query_spans_filtered(&self, limit: Option<u64>, filter: &OtelFilter) -> Option<RecordBatch> {
        let spans = self.spans.lock().unwrap();
        if spans.is_empty() { return None; }
        let filtered: Vec<&SpanRow> = spans.iter().rev().filter(|r| {
            if let Some(ref svc) = filter.service_name {
                if r.service_name != *svc { return false; }
            }
            if let Some(min) = filter.min_time_ns {
                if r.end_time_ns < min { return false; }
            }
            if let Some(max) = filter.max_time_ns {
                if r.start_time_ns > max { return false; }
            }
            true
        }).collect();
        if filtered.is_empty() { return None; }
        let n = limit.map(|l| filtered.len().min(l as usize)).unwrap_or(filtered.len());
        let rows = &filtered[..n];
        let schema = std::sync::Arc::new(spans_schema());
        RecordBatch::try_new(schema, vec![
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.trace_id.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.span_id.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.parent_span_id.as_deref()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.service_name.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.operation.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.status.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(Int64Array::from(rows.iter().map(|r| r.start_time_ns).collect::<Vec<_>>())),
            std::sync::Arc::new(Int64Array::from(rows.iter().map(|r| r.end_time_ns).collect::<Vec<_>>())),
            std::sync::Arc::new(Int64Array::from(rows.iter().map(|r| r.duration_ns).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.attributes.as_deref()).collect::<Vec<_>>())),
        ]).ok()
    }

    pub fn query_metrics(&self, limit: Option<u64>) -> Option<RecordBatch> {
        self.query_metrics_filtered(limit, &OtelFilter::default())
    }

    pub fn query_metrics_filtered(&self, limit: Option<u64>, filter: &OtelFilter) -> Option<RecordBatch> {
        let metrics = self.metrics.lock().unwrap();
        if metrics.is_empty() { return None; }
        let filtered: Vec<&MetricRow> = metrics.iter().rev().filter(|r| {
            if let Some(ref svc) = filter.service_name {
                if r.service_name.as_deref() != Some(svc.as_str()) { return false; }
            }
            if let Some(min) = filter.min_time_ns {
                if r.timestamp_ns < min { return false; }
            }
            if let Some(max) = filter.max_time_ns {
                if r.timestamp_ns > max { return false; }
            }
            true
        }).collect();
        if filtered.is_empty() { return None; }
        let n = limit.map(|l| filtered.len().min(l as usize)).unwrap_or(filtered.len());
        let rows = &filtered[..n];
        let schema = std::sync::Arc::new(metrics_schema());
        RecordBatch::try_new(schema, vec![
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.name.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.description.as_deref()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.unit.as_deref()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.metric_type.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.value.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(Int64Array::from(rows.iter().map(|r| r.timestamp_ns).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.labels.as_deref()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.service_name.as_deref()).collect::<Vec<_>>())),
        ]).ok()
    }

    pub fn query_logs(&self, limit: Option<u64>) -> Option<RecordBatch> {
        self.query_logs_filtered(limit, &OtelFilter::default())
    }

    pub fn query_logs_filtered(&self, limit: Option<u64>, filter: &OtelFilter) -> Option<RecordBatch> {
        let logs = self.logs.lock().unwrap();
        if logs.is_empty() { return None; }
        let filtered: Vec<&LogRow> = logs.iter().rev().filter(|r| {
            if let Some(ref svc) = filter.service_name {
                if r.service_name.as_deref() != Some(svc.as_str()) { return false; }
            }
            if let Some(min) = filter.min_time_ns {
                if r.timestamp_ns < min { return false; }
            }
            if let Some(max) = filter.max_time_ns {
                if r.timestamp_ns > max { return false; }
            }
            true
        }).collect();
        if filtered.is_empty() { return None; }
        let n = limit.map(|l| filtered.len().min(l as usize)).unwrap_or(filtered.len());
        let rows = &filtered[..n];
        let schema = std::sync::Arc::new(logs_schema());
        RecordBatch::try_new(schema, vec![
            std::sync::Arc::new(Int64Array::from(rows.iter().map(|r| r.timestamp_ns).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.severity.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.body.as_str()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.service_name.as_deref()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.trace_id.as_deref()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.span_id.as_deref()).collect::<Vec<_>>())),
            std::sync::Arc::new(StringArray::from(rows.iter().map(|r| r.attributes.as_deref()).collect::<Vec<_>>())),
        ]).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_store() {
        let store = OtelStore::new(100, 100, 100);
        assert_eq!(store.counts(), (0, 0, 0));
        assert!(store.query_spans(None).is_none());
        assert!(store.query_metrics(None).is_none());
        assert!(store.query_logs(None).is_none());
    }

    #[test]
    fn test_ingest_and_query_span() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_span("t1", "s1", None, "svc-a", "GET /", "OK", 1000, 2000, "");
        assert_eq!(store.counts().0, 1);
        let batch = store.query_spans(None).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().fields().len(), 10);
    }

    #[test]
    fn test_span_duration_computed() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_span("t1", "s1", None, "svc", "op", "OK", 1_000_000, 5_000_000, "");
        let batch = store.query_spans(None).unwrap();
        let dur = batch.column(8).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(dur.value(0), 4_000_000);
    }

    #[test]
    fn test_ingest_and_query_metric() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_metric("http.duration", Some("Request duration"), Some("ms"),
            "histogram", "42.5", 1000, Some(r#"{"method":"GET"}"#), Some("svc-a"));
        assert_eq!(store.counts().1, 1);
        let batch = store.query_metrics(None).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_ingest_and_query_log() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_log(1000, "INFO", "Server started", Some("svc-a"), None, None, None);
        assert_eq!(store.counts().2, 1);
        let batch = store.query_logs(None).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let store = OtelStore::new(3, 3, 3);
        for i in 0..5 {
            store.ingest_span(&format!("t{i}"), &format!("s{i}"), None,
                "svc", "op", "OK", i, i + 1, "");
        }
        assert_eq!(store.counts().0, 3);
        let batch = store.query_spans(None).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_query_with_limit() {
        let store = OtelStore::new(100, 100, 100);
        for i in 0..10 {
            store.ingest_log(i, "INFO", &format!("msg{i}"), None, None, None, None);
        }
        let batch = store.query_logs(Some(3)).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_span_with_parent() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_span("t1", "s1", None, "svc", "root", "OK", 1, 10, "");
        store.ingest_span("t1", "s2", Some("s1"), "svc", "child", "OK", 2, 8, "");
        let batch = store.query_spans(None).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_metric_eviction() {
        let store = OtelStore::new(100, 2, 100);
        for i in 0..5 {
            store.ingest_metric(&format!("m{i}"), None, None, "gauge",
                &format!("{i}"), i, None, None);
        }
        assert_eq!(store.counts().1, 2);
    }

    #[test]
    fn test_log_eviction() {
        let store = OtelStore::new(100, 100, 2);
        for i in 0..5 {
            store.ingest_log(i, "WARN", &format!("log{i}"), None, None, None, None);
        }
        assert_eq!(store.counts().2, 2);
    }

    #[test]
    fn test_schemas_valid() {
        let s = spans_schema();
        assert_eq!(s.fields().len(), 10);
        assert!(s.field_with_name("trace_id").is_ok());
        assert!(s.field_with_name("duration_ns").is_ok());

        let m = metrics_schema();
        assert_eq!(m.fields().len(), 8);
        assert!(m.field_with_name("name").is_ok());

        let l = logs_schema();
        assert_eq!(l.fields().len(), 7);
        assert!(l.field_with_name("body").is_ok());
    }

    #[test]
    fn test_filter_spans_by_service() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_span("t1", "s1", None, "svc-a", "op1", "OK", 100, 200, "");
        store.ingest_span("t2", "s2", None, "svc-b", "op2", "OK", 100, 200, "");
        let filter = OtelFilter { service_name: Some("svc-a".into()), ..Default::default() };
        let batch = store.query_spans_filtered(None, &filter).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_filter_spans_by_time_range() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_span("t1", "s1", None, "svc", "op", "OK", 100, 200, "");
        store.ingest_span("t2", "s2", None, "svc", "op", "OK", 500, 600, "");
        let filter = OtelFilter { min_time_ns: Some(300), ..Default::default() };
        let batch = store.query_spans_filtered(None, &filter).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_filter_metrics_by_service() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_metric("m1", None, None, "gauge", "1", 100, None, Some("svc-a"));
        store.ingest_metric("m2", None, None, "gauge", "2", 100, None, Some("svc-b"));
        let filter = OtelFilter { service_name: Some("svc-b".into()), ..Default::default() };
        let batch = store.query_metrics_filtered(None, &filter).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_filter_logs_by_time_range() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_log(100, "INFO", "early", None, None, None, None);
        store.ingest_log(500, "WARN", "late", None, None, None, None);
        let filter = OtelFilter { max_time_ns: Some(300), ..Default::default() };
        let batch = store.query_logs_filtered(None, &filter).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_filter_returns_none_when_no_match() {
        let store = OtelStore::new(100, 100, 100);
        store.ingest_span("t1", "s1", None, "svc-a", "op", "OK", 100, 200, "");
        let filter = OtelFilter { service_name: Some("nonexistent".into()), ..Default::default() };
        assert!(store.query_spans_filtered(None, &filter).is_none());
    }
}
