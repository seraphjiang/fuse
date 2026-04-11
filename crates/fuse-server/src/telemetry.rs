// SPDX-License-Identifier: Apache-2.0

//! OpenTelemetry distributed tracing integration for Fuse.
//!
//! Wires the OTel SDK into the existing `tracing` subscriber so spans
//! are exported via OTLP gRPC to any compatible collector (Jaeger, Tempo,
//! AWS X-Ray via ADOT, etc.). Works alongside `tracing_ctx.rs` which
//! handles lightweight W3C traceparent propagation.
//!
//! # Configuration (fuse.toml)
//!
//! ```toml
//! [engine.telemetry]
//! enabled = true
//! otlp_endpoint = "http://localhost:4317"
//! service_name = "fuse"
//! ```

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Telemetry configuration parsed from `[engine.telemetry]` in fuse.toml.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub otlp_endpoint: String,
    pub service_name: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: "http://localhost:4317".into(),
            service_name: "fuse".into(),
        }
    }
}

impl TelemetryConfig {
    pub fn from_toml(table: Option<&toml::Value>) -> Self {
        let Some(t) = table.and_then(|v| v.as_table()) else {
            return Self::default();
        };
        Self {
            enabled: t.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false),
            otlp_endpoint: t.get("otlp_endpoint").and_then(|v| v.as_str())
                .unwrap_or("http://localhost:4317").into(),
            service_name: t.get("service_name").and_then(|v| v.as_str())
                .unwrap_or("fuse").into(),
        }
    }
}

/// Initialize tracing with optional OTLP export.
///
/// Returns the provider guard — drop on shutdown to flush pending spans.
pub fn init_tracing(config: &TelemetryConfig) -> Option<SdkTracerProvider> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    if config.enabled {
        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&config.otlp_endpoint)
            .build()
            .expect("failed to create OTLP span exporter");

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name(config.service_name.clone())
                    .build(),
            )
            .build();

        let tracer = provider.tracer("fuse");
        let otel_layer = OpenTelemetryLayer::new(tracer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .with(otel_layer)
            .init();

        Some(provider)
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = TelemetryConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.otlp_endpoint, "http://localhost:4317");
        assert_eq!(cfg.service_name, "fuse");
    }

    #[test]
    fn test_config_from_toml_none() {
        let cfg = TelemetryConfig::from_toml(None);
        assert!(!cfg.enabled);
    }

    #[test]
    fn test_config_from_toml_enabled() {
        let val: toml::Value = toml::from_str(r#"
            enabled = true
            otlp_endpoint = "http://collector:4317"
            service_name = "fuse-prod"
        "#).unwrap();
        let cfg = TelemetryConfig::from_toml(Some(&val));
        assert!(cfg.enabled);
        assert_eq!(cfg.otlp_endpoint, "http://collector:4317");
        assert_eq!(cfg.service_name, "fuse-prod");
    }

    #[test]
    fn test_config_from_toml_partial() {
        let val: toml::Value = toml::from_str(r#"
            enabled = true
        "#).unwrap();
        let cfg = TelemetryConfig::from_toml(Some(&val));
        assert!(cfg.enabled);
        assert_eq!(cfg.otlp_endpoint, "http://localhost:4317");
        assert_eq!(cfg.service_name, "fuse");
    }

    #[test]
    fn test_config_from_toml_disabled_explicit() {
        let val: toml::Value = toml::from_str(r#"
            enabled = false
            otlp_endpoint = "http://custom:4317"
        "#).unwrap();
        let cfg = TelemetryConfig::from_toml(Some(&val));
        assert!(!cfg.enabled);
        assert_eq!(cfg.otlp_endpoint, "http://custom:4317");
    }
}
