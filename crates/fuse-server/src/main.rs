// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tracing::info;

use fuse_core::config::FuseConfig;
use fuse_core::registry::{ConnectorFactory, ConnectorRegistry};
use fuse_connector_opensearch::OpenSearchConnectorFactory;
use fuse_connector_s3_o11y::S3O11yConnectorFactory;
use fuse_connector_dynamodb::DynamoDbConnectorFactory;
use fuse_connector_postgres::{PostgresConnectorFactory, MysqlConnectorFactory, RedshiftConnectorFactory};
use fuse_connector_elasticsearch::ElasticsearchConnectorFactory;
// use fuse_connector_mongodb::MongoDbConnectorFactory;  // blocked: compile error in mongodb connector
use fuse_connector_influxdb::InfluxDbConnectorFactory;
use fuse_connector_clickhouse::ClickHouseConnectorFactory;
use fuse_connector_cloudwatch::CloudWatchConnectorFactory;
use fuse_connector_csv_json::CsvJsonConnectorFactory;
// use fuse_connector_redis::RedisConnectorFactory;  // blocked: compile error

use fuse_server::api::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,fuse_server=debug".into());

    let log_format = std::env::var("FUSE_LOG_FORMAT").unwrap_or_default();
    if log_format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .with_target(true)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .init();
    }

    // Load config
    let config_path = std::env::var("FUSE_CONFIG").unwrap_or_else(|_| "fuse.toml".to_string());
    let config = FuseConfig::from_file(&config_path).unwrap_or_else(|e| {
        info!("No config loaded ({}), starting with empty registry", e);
        FuseConfig {
            engine: fuse_core::config::EngineConfig {
                bind: "0.0.0.0:9400".to_string(),
                max_concurrent_queries: 64,
                default_timeout: "30s".to_string(),
                rate_limit_global: 1000,
                rate_limit_per_ip: 100,
            },
            connector: vec![],
        }
    });

    // Build connector registry from config
    let registry = ConnectorRegistry::new();
    let factories: Vec<Box<dyn ConnectorFactory>> = vec![
        Box::new(OpenSearchConnectorFactory),
        Box::new(S3O11yConnectorFactory),
        Box::new(DynamoDbConnectorFactory),
        Box::new(PostgresConnectorFactory),
        Box::new(MysqlConnectorFactory),
        Box::new(RedshiftConnectorFactory),
        Box::new(ElasticsearchConnectorFactory),
        // Box::new(MongoDbConnectorFactory),  // blocked: compile error
        Box::new(InfluxDbConnectorFactory),
        Box::new(ClickHouseConnectorFactory),
        Box::new(CloudWatchConnectorFactory),
        Box::new(CsvJsonConnectorFactory),
        // Box::new(RedisConnectorFactory),  // blocked
    ];

    for cc in &config.connector {
        let factory = factories
            .iter()
            .find(|f| f.connector_type() == cc.connector_type);

        match factory {
            Some(f) => match f.create(cc).await {
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
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(fuse_server::api::RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });

    // Initialize metrics
    let metrics_handle = fuse_server::metrics::init();
    fuse_server::metrics::set_handle(metrics_handle);

    // Build router with rate limits from config
    let rl = fuse_server::rate_limit::RateLimitState::new(
        config.engine.rate_limit_global,
        config.engine.rate_limit_per_ip,
    );
    let running_queries = state.running_queries.clone();
    let app = fuse_server::build_router_with_limits(state, rl);

    let bind = &config.engine.bind;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!(bind = %bind, "Fuse server starting");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(running_queries))
        .await?;

    info!("Fuse server stopped");
    Ok(())
}

async fn shutdown_signal(running_queries: Arc<fuse_server::api::RunningQueries>) {
    let ctrl_c = async { tokio::signal::ctrl_c().await.unwrap() };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .unwrap()
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received SIGINT"),
        _ = terminate => info!("Received SIGTERM"),
    }

    info!(in_flight = running_queries.count(), "Shutting down — draining in-flight queries");

    // Give in-flight queries up to 10 seconds to complete
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while running_queries.count() > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    if running_queries.count() > 0 {
        info!(remaining = running_queries.count(), "Grace period expired — cancelling remaining queries");
        running_queries.cancel_all();
    }

    info!("Shutdown complete");
}
