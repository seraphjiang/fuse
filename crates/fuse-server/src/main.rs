// SPDX-License-Identifier: Apache-2.0

mod api;
mod health;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use tracing::info;

use fuse_core::config::FuseConfig;
use fuse_core::registry::{ConnectorFactory, ConnectorRegistry};
use fuse_connector_opensearch::OpenSearchConnectorFactory;

use api::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,fuse_server=debug".into()),
        )
        .init();

    // Load config
    let config_path = std::env::var("FUSE_CONFIG").unwrap_or_else(|_| "fuse.toml".to_string());
    let config = FuseConfig::from_file(&config_path).unwrap_or_else(|e| {
        info!("No config loaded ({}), starting with empty registry", e);
        FuseConfig {
            engine: fuse_core::config::EngineConfig {
                bind: "0.0.0.0:9400".to_string(),
                max_concurrent_queries: 64,
                default_timeout: "30s".to_string(),
            },
            connector: vec![],
        }
    });

    // Build connector registry from config
    let registry = ConnectorRegistry::new();
    let factories: Vec<Box<dyn ConnectorFactory>> =
        vec![Box::new(OpenSearchConnectorFactory)];

    for cc in &config.connector {
        let factory = factories
            .iter()
            .find(|f| f.connector_type() == cc.connector_type);

        match factory {
            Some(f) => match f.create(cc) {
                Ok(connector) => {
                    registry.register(connector)?;
                    info!(id = %cc.id, r#type = %cc.connector_type, "Registered connector");
                }
                Err(e) => {
                    tracing::warn!(id = %cc.id, error = %e, "Failed to create connector, skipping");
                }
            },
            None => {
                tracing::warn!(
                    id = %cc.id,
                    r#type = %cc.connector_type,
                    "Unknown connector type, skipping"
                );
            }
        }
    }

    let state = Arc::new(AppState {
        registry: Arc::new(registry),
    });

    // Build router
    let app = Router::new()
        .route("/api/fuse/query", post(api::query_handler))
        .route("/api/fuse/datasources", get(api::list_datasources))
        .route(
            "/api/fuse/datasources/{id}/schemas",
            get(api::get_schemas),
        )
        .route(
            "/api/fuse/datasources/{id}/schemas/{table}/fields",
            get(api::get_fields),
        )
        .route("/api/fuse/query/explain", post(api::explain_handler))
        .route("/api/fuse/query/validate", post(api::validate_handler))
        .route("/api/fuse/health", get(api::health_handler))
        .with_state(state);

    let bind = &config.engine.bind;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!(bind = %bind, "Fuse server starting");
    axum::serve(listener, app).await?;

    Ok(())
}
