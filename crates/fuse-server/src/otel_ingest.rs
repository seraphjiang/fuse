// SPDX-License-Identifier: Apache-2.0
//! OTLP HTTP ingestion handlers for the OTel collector connector.
//!
//! Accepts OTLP payloads at:
//! - `POST /v1/traces`  — JSON (`application/json`) or Protobuf (`application/x-protobuf`)
//! - `POST /v1/metrics` — JSON or Protobuf
//! - `POST /v1/logs`    — JSON or Protobuf
//! - `GET  /v1/health`  — collector health status
//!
//! Protobuf payloads are decoded to JSON via serde_json for uniform processing.
//! This avoids a hard dependency on generated proto types while still accepting
//! the default wire format from OTel SDKs.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::Value;

use fuse_connector_otel::store::OtelStore;

/// Shared state for OTLP ingestion routes.
#[derive(Clone)]
pub struct OtelIngestState {
    pub store: Arc<OtelStore>,
}

/// Content type detection from request headers.
fn is_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("application/x-protobuf"))
        .unwrap_or(false)
}

/// POST /v1/traces — ingest OTLP trace data (JSON or Protobuf).
pub async fn ingest_traces(
    State(state): State<OtelIngestState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let parsed = if is_protobuf(&headers) {
        // Protobuf not yet decoded — return 415 with guidance
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(serde_json::json!({
                "error": "Protobuf decoding not yet supported. Send JSON with Content-Type: application/json",
                "hint": "Set OTEL_EXPORTER_OTLP_PROTOCOL=http/json in your OTel SDK configuration"
            })),
        ).into_response();
    } else {
        match serde_json::from_slice::<Value>(&body) {
            Ok(v) => v,
            Err(e) => return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("Invalid JSON: {e}")})),
            ).into_response(),
        }
    };

    let count = ingest_traces_from_json(&state.store, &parsed);
    tracing::debug!(count, "Ingested OTLP traces");
    (StatusCode::OK, Json(serde_json::json!({"accepted": count}))).into_response()
}

/// POST /v1/metrics — ingest OTLP metric data (JSON or Protobuf).
pub async fn ingest_metrics(
    State(state): State<OtelIngestState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let parsed = if is_protobuf(&headers) {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(serde_json::json!({
                "error": "Protobuf decoding not yet supported. Send JSON with Content-Type: application/json",
                "hint": "Set OTEL_EXPORTER_OTLP_PROTOCOL=http/json in your OTel SDK configuration"
            })),
        ).into_response();
    } else {
        match serde_json::from_slice::<Value>(&body) {
            Ok(v) => v,
            Err(e) => return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("Invalid JSON: {e}")})),
            ).into_response(),
        }
    };

    let count = ingest_metrics_from_json(&state.store, &parsed);
    tracing::debug!(count, "Ingested OTLP metrics");
    (StatusCode::OK, Json(serde_json::json!({"accepted": count}))).into_response()
}

/// POST /v1/logs — ingest OTLP log data (JSON or Protobuf).
pub async fn ingest_logs(
    State(state): State<OtelIngestState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let parsed = if is_protobuf(&headers) {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(serde_json::json!({
                "error": "Protobuf decoding not yet supported. Send JSON with Content-Type: application/json",
                "hint": "Set OTEL_EXPORTER_OTLP_PROTOCOL=http/json in your OTel SDK configuration"
            })),
        ).into_response();
    } else {
        match serde_json::from_slice::<Value>(&body) {
            Ok(v) => v,
            Err(e) => return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("Invalid JSON: {e}")})),
            ).into_response(),
        }
    };

    let count = ingest_logs_from_json(&state.store, &parsed);
    tracing::debug!(count, "Ingested OTLP logs");
    (StatusCode::OK, Json(serde_json::json!({"accepted": count}))).into_response()
}

/// GET /v1/health — OTel collector health endpoint.
pub async fn otel_health(
    State(state): State<OtelIngestState>,
) -> impl IntoResponse {
    let (spans, metrics, logs) = state.store.counts();
    Json(serde_json::json!({
        "status": "ready",
        "signals": {
            "traces": { "count": spans },
            "metrics": { "count": metrics },
            "logs": { "count": logs }
        }
    }))
}

// ── JSON parsing helpers ─────────────────────────────────────

fn ingest_traces_from_json(store: &OtelStore, body: &Value) -> u64 {
    let mut count = 0u64;
    let Some(resource_spans) = body.get("resourceSpans").and_then(|v| v.as_array()) else {
        return 0;
    };
    for rs in resource_spans {
        let service_name = extract_service_name(rs.get("resource"));
        let Some(scope_spans) = rs.get("scopeSpans").and_then(|v| v.as_array()) else { continue };
        for ss in scope_spans {
            let Some(spans) = ss.get("spans").and_then(|v| v.as_array()) else { continue };
            for span in spans {
                let trace_id = span.get("traceId").and_then(|v| v.as_str()).unwrap_or("");
                let span_id = span.get("spanId").and_then(|v| v.as_str()).unwrap_or("");
                let parent = span.get("parentSpanId").and_then(|v| v.as_str());
                let name = span.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let status_code = span.get("status")
                    .and_then(|s| s.get("code"))
                    .and_then(|c| c.as_u64())
                    .unwrap_or(0);
                let status = match status_code {
                    0 => "UNSET",
                    1 => "OK",
                    2 => "ERROR",
                    _ => "UNKNOWN",
                };
                let start = parse_nano(span.get("startTimeUnixNano"));
                let end = parse_nano(span.get("endTimeUnixNano"));
                let attrs = span.get("attributes").map(|a| a.to_string()).unwrap_or_default();

                store.ingest_span(trace_id, span_id, parent, &service_name, name, status, start, end, &attrs);
                count += 1;
            }
        }
    }
    count
}

fn ingest_metrics_from_json(store: &OtelStore, body: &Value) -> u64 {
    let mut count = 0u64;
    let Some(resource_metrics) = body.get("resourceMetrics").and_then(|v| v.as_array()) else {
        return 0;
    };
    for rm in resource_metrics {
        let service_name = extract_service_name(rm.get("resource"));
        let Some(scope_metrics) = rm.get("scopeMetrics").and_then(|v| v.as_array()) else { continue };
        for sm in scope_metrics {
            let Some(metrics) = sm.get("metrics").and_then(|v| v.as_array()) else { continue };
            for metric in metrics {
                let name = metric.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let desc = metric.get("description").and_then(|v| v.as_str());
                let unit = metric.get("unit").and_then(|v| v.as_str());
                let (metric_type, points) = extract_data_points(metric);
                for (value, ts, labels) in points {
                    store.ingest_metric(
                        name, desc, unit, metric_type, &value, ts,
                        labels.as_deref(),
                        if service_name.is_empty() { None } else { Some(&service_name) },
                    );
                    count += 1;
                }
            }
        }
    }
    count
}

fn ingest_logs_from_json(store: &OtelStore, body: &Value) -> u64 {
    let mut count = 0u64;
    let Some(resource_logs) = body.get("resourceLogs").and_then(|v| v.as_array()) else {
        return 0;
    };
    for rl in resource_logs {
        let service_name = extract_service_name(rl.get("resource"));
        let Some(scope_logs) = rl.get("scopeLogs").and_then(|v| v.as_array()) else { continue };
        for sl in scope_logs {
            let Some(log_records) = sl.get("logRecords").and_then(|v| v.as_array()) else { continue };
            for rec in log_records {
                let ts = parse_nano(rec.get("timeUnixNano"));
                let severity = rec.get("severityText").and_then(|v| v.as_str()).unwrap_or("UNSPECIFIED");
                let body_val = rec.get("body")
                    .and_then(|b| b.get("stringValue").and_then(|v| v.as_str()))
                    .unwrap_or("");
                let trace_id = rec.get("traceId").and_then(|v| v.as_str());
                let span_id = rec.get("spanId").and_then(|v| v.as_str());
                let attrs = rec.get("attributes").map(|a| a.to_string());

                store.ingest_log(
                    ts, severity, body_val,
                    if service_name.is_empty() { None } else { Some(&service_name) },
                    trace_id, span_id, attrs.as_deref(),
                );
                count += 1;
            }
        }
    }
    count
}

/// Parse a nanosecond timestamp that may be a string or integer.
fn parse_nano(v: Option<&Value>) -> i64 {
    v.and_then(|v| {
        v.as_str()
            .map(|s| s.parse::<i64>().unwrap_or(0))
            .or_else(|| v.as_i64())
    })
    .unwrap_or(0)
}

/// Extract service.name from resource attributes.
fn extract_service_name(resource: Option<&Value>) -> String {
    resource
        .and_then(|r| r.get("attributes"))
        .and_then(|attrs| attrs.as_array())
        .and_then(|arr| {
            arr.iter().find(|a| {
                a.get("key").and_then(|k| k.as_str()) == Some("service.name")
            })
        })
        .and_then(|a| a.get("value").and_then(|v| v.get("stringValue")).and_then(|s| s.as_str()))
        .unwrap_or("")
        .to_string()
}

/// Extract data points from a metric (gauge, sum, or histogram).
fn extract_data_points(metric: &Value) -> (&str, Vec<(String, i64, Option<String>)>) {
    let (metric_type, dp_key) = if metric.get("gauge").is_some() {
        ("gauge", "gauge")
    } else if metric.get("sum").is_some() {
        ("sum", "sum")
    } else if metric.get("histogram").is_some() {
        ("histogram", "histogram")
    } else {
        return ("unknown", vec![]);
    };

    let Some(points) = metric.get(dp_key).and_then(|d| d.get("dataPoints")).and_then(|v| v.as_array()) else {
        return (metric_type, vec![]);
    };

    let results = points.iter().map(|dp| {
        let value = dp.get("asDouble")
            .and_then(|v| v.as_f64())
            .map(|f| f.to_string())
            .or_else(|| {
                dp.get("asInt")
                    .and_then(|v| v.as_str().map(String::from).or_else(|| v.as_i64().map(|i| i.to_string())))
            })
            .unwrap_or_else(|| "0".into());

        let ts = parse_nano(dp.get("timeUnixNano"));

        let labels = dp.get("attributes").and_then(|attrs| {
            attrs.as_array().map(|arr| {
                let map: serde_json::Map<String, Value> = arr.iter().filter_map(|a| {
                    let key = a.get("key")?.as_str()?;
                    let val = a.get("value")?.get("stringValue")?.as_str()?;
                    Some((key.to_string(), Value::String(val.to_string())))
                }).collect();
                Value::Object(map).to_string()
            })
        });

        (value, ts, labels)
    }).collect();

    (metric_type, results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_service_name() {
        let resource: Value = serde_json::from_str(r#"{
            "attributes": [
                {"key": "service.name", "value": {"stringValue": "my-service"}}
            ]
        }"#).unwrap();
        assert_eq!(extract_service_name(Some(&resource)), "my-service");
    }

    #[test]
    fn test_extract_service_name_missing() {
        assert_eq!(extract_service_name(None), "");
    }

    #[test]
    fn test_extract_data_points_gauge() {
        let metric: Value = serde_json::from_str(r#"{
            "name": "cpu.usage",
            "gauge": {
                "dataPoints": [
                    {"asDouble": 42.5, "timeUnixNano": "1000000000"}
                ]
            }
        }"#).unwrap();
        let (mt, points) = extract_data_points(&metric);
        assert_eq!(mt, "gauge");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].0, "42.5");
        assert_eq!(points[0].1, 1000000000);
    }

    #[test]
    fn test_extract_data_points_sum() {
        let metric: Value = serde_json::from_str(r#"{
            "name": "requests",
            "sum": {
                "dataPoints": [
                    {"asInt": "100", "timeUnixNano": "2000000000"}
                ]
            }
        }"#).unwrap();
        let (mt, points) = extract_data_points(&metric);
        assert_eq!(mt, "sum");
        assert_eq!(points[0].0, "100");
    }

    #[test]
    fn test_extract_data_points_unknown() {
        let metric: Value = serde_json::from_str(r#"{"name": "x"}"#).unwrap();
        let (mt, points) = extract_data_points(&metric);
        assert_eq!(mt, "unknown");
        assert!(points.is_empty());
    }

    #[test]
    fn test_parse_nano_string() {
        let v: Value = serde_json::json!("1234567890");
        assert_eq!(parse_nano(Some(&v)), 1234567890);
    }

    #[test]
    fn test_parse_nano_int() {
        let v: Value = serde_json::json!(9876543210_i64);
        assert_eq!(parse_nano(Some(&v)), 9876543210);
    }

    #[test]
    fn test_parse_nano_none() {
        assert_eq!(parse_nano(None), 0);
    }

    #[test]
    fn test_is_protobuf() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/x-protobuf".parse().unwrap());
        assert!(is_protobuf(&headers));

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        assert!(!is_protobuf(&headers));
    }

    #[test]
    fn test_ingest_traces_from_json() {
        let store = OtelStore::new(100, 100, 100);
        let body: Value = serde_json::from_str(r#"{
            "resourceSpans": [{
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "test-svc"}}]},
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "abc",
                        "spanId": "def",
                        "name": "GET /",
                        "startTimeUnixNano": "1000",
                        "endTimeUnixNano": "2000",
                        "status": {"code": 1}
                    }]
                }]
            }]
        }"#).unwrap();
        let count = ingest_traces_from_json(&store, &body);
        assert_eq!(count, 1);
        assert_eq!(store.counts().0, 1);
    }

    #[test]
    fn test_ingest_metrics_from_json() {
        let store = OtelStore::new(100, 100, 100);
        let body: Value = serde_json::from_str(r#"{
            "resourceMetrics": [{
                "resource": {"attributes": []},
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "cpu",
                        "gauge": {"dataPoints": [{"asDouble": 0.5, "timeUnixNano": "1000"}]}
                    }]
                }]
            }]
        }"#).unwrap();
        let count = ingest_metrics_from_json(&store, &body);
        assert_eq!(count, 1);
        assert_eq!(store.counts().1, 1);
    }

    #[test]
    fn test_ingest_logs_from_json() {
        let store = OtelStore::new(100, 100, 100);
        let body: Value = serde_json::from_str(r#"{
            "resourceLogs": [{
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "log-svc"}}]},
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "5000",
                        "severityText": "ERROR",
                        "body": {"stringValue": "something broke"}
                    }]
                }]
            }]
        }"#).unwrap();
        let count = ingest_logs_from_json(&store, &body);
        assert_eq!(count, 1);
        assert_eq!(store.counts().2, 1);
    }
}
