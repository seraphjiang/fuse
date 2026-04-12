// SPDX-License-Identifier: Apache-2.0
//! #543 SQL injection fuzz tests across all connector pushdown paths.

use fuse_core::connector::*;

fn make_query(payload: &str) -> SubQuery {
    SubQuery {
        table: "test".into(),
        projections: vec![],
        filter: Some(FilterExpr::Comparison {
            field: "name".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8(payload.to_string()),
        }),
        aggregations: vec![],
        group_by: vec![],
        having: None,
        sort: vec![],
        limit: None,
        passthrough: None,
        offset: None,
    }
}

const PAYLOADS: &[&str] = &[
    "'; DROP TABLE users; --",
    "\" OR 1=1 --",
    "' UNION SELECT * FROM secrets --",
    "Robert'); DROP TABLE students;--",
    "' OR ''='",
    "'); DROP TABLE logs; --",
    "' AND 1=CONVERT(int,(SELECT TOP 1 name FROM sys.tables))--",
    "' OR 1=1#",
    "admin'--",
    "' WAITFOR DELAY '0:0:5'--",
    "' UNION ALL SELECT NULL,NULL--",
    "' ; SELECT * FROM information_schema.tables --",
    "",
];

// ── PostgreSQL: quote escaping ──

#[test]
fn test_postgres_escapes_all_payloads() {
    use fuse_connector_postgres::sql::subquery_to_sql;
    for payload in PAYLOADS {
        let sql = subquery_to_sql(&make_query(payload));
        if payload.contains('\'') {
            let escaped = payload.replace('\'', "''");
            assert!(sql.contains(&escaped),
                "PG unescaped: {}\nSQL: {}", payload, sql);
        }
    }
}

// ── ClickHouse: quote escaping ──

#[test]
fn test_clickhouse_escapes_all_payloads() {
    use fuse_connector_clickhouse::sql::subquery_to_sql;
    for payload in PAYLOADS {
        let sql = subquery_to_sql(&make_query(payload));
        if payload.contains('\'') {
            let escaped = payload.replace('\'', "''");
            assert!(sql.contains(&escaped),
                "CH unescaped: {}\nSQL: {}", payload, sql);
        }
    }
}

// ── OpenSearch: JSON-safe (structured, no interpolation) ──

#[test]
fn test_opensearch_preserves_payloads_in_json() {
    use fuse_connector_opensearch::pushdown::translate_to_query_dsl;
    for payload in PAYLOADS {
        let dsl = translate_to_query_dsl(&make_query(payload));
        let val = dsl["query"]["term"]["name"].as_str().unwrap_or("");
        assert_eq!(val, *payload, "OS corrupted: {}", payload);
    }
}

// ── Elasticsearch: JSON-safe ──

#[test]
fn test_elasticsearch_preserves_payloads_in_json() {
    use fuse_connector_elasticsearch::pushdown::translate_to_query_dsl;
    for payload in PAYLOADS {
        let dsl = translate_to_query_dsl(&make_query(payload));
        let val = dsl["query"]["term"]["name"].as_str().unwrap_or("");
        assert_eq!(val, *payload, "ES corrupted: {}", payload);
    }
}

// ── Server-level: injection via query handler (no panic, no 5xx crash) ──

use axum::http::StatusCode;

async fn post_query(app: axum::Router, query: &str, format: &str) -> (StatusCode, serde_json::Value) {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;
    let body = serde_json::json!({"query": query, "format": format});
    let req = Request::builder()
        .method("POST")
        .uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    (status, json)
}

fn build_test_app() -> axum::Router {
    use std::sync::Arc;
    use fuse_core::registry::ConnectorRegistry;
    use fuse_server::api::{AppState, RunningQueries};

    #[derive(Debug)]
    struct MockConn;
    #[async_trait::async_trait]
    impl FederatedConnector for MockConn {
        fn id(&self) -> &str { "testds" }
        fn connector_type(&self) -> &str { "mock" }
        fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
        async fn health_check(&self) -> ConnectorHealth {
            ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None }
        }
        async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, fuse_core::error::ConnectorError> {
            Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Table, estimated_row_count: Some(2) }])
        }
        async fn get_schema(&self, _: &str) -> Result<arrow::datatypes::Schema, fuse_core::error::ConnectorError> {
            use arrow::datatypes::{DataType, Field};
            Ok(arrow::datatypes::Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
            ]))
        }
        async fn execute(&self, _: &SubQuery) -> Result<Vec<arrow::record_batch::RecordBatch>, fuse_core::error::ConnectorError> {
            use arrow::array::StringArray;
            use std::sync::Arc as A;
            let schema = A::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            ]));
            Ok(vec![arrow::record_batch::RecordBatch::try_new(schema, vec![
                A::new(StringArray::from(vec!["alice", "bob"])) as arrow::array::ArrayRef,
            ]).unwrap()])
        }
        async fn execute_streaming(&self, q: &SubQuery, tx: tokio::sync::mpsc::Sender<Result<arrow::record_batch::RecordBatch, fuse_core::error::ConnectorError>>) -> Result<(), fuse_core::error::ConnectorError> {
            for b in self.execute(q).await? { tx.send(Ok(b)).await.map_err(|_| fuse_core::error::ConnectorError::ChannelClosed)?; }
            Ok(())
        }
    }

    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(MockConn)).unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
        audit_log: Arc::new(fuse_server::audit::AuditLog::new(100)),
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::new(vec![])),
        result_cache: Arc::new(fuse_server::plan_cache::ResultCache::new(60, 500)),
        prepared_statements: fuse_server::prepared::new_store(),
        adaptive_timeout: Arc::new(fuse_server::adaptive_timeout::AdaptiveTimeout::new()),
        shared_saved_queries: fuse_server::shared_state::SharedSavedQueries::from_env(),
        shared_history: fuse_server::shared_state::SharedQueryHistory::from_env(),
        shared_audit_log: fuse_server::shared_state::SharedAuditLog::from_env(), transactions: std::sync::Arc::new(fuse_server::transaction::TransactionStore::new()), max_result_bytes: 0, datasource_limiter: std::sync::Arc::new(fuse_server::rate_limit::DatasourceLimiter::new()), adaptive_parallelism: std::sync::Arc::new(fuse_server::adaptive_parallelism::AdaptiveParallelism::new()), otel_store: None, query_recorder: std::sync::Arc::new(fuse_server::query_replay::QueryRecorder::new(100)), compilation_cache: std::sync::Arc::new(fuse_server::query_compilation::CompilationCache::new(300, 5000)),
    });
    fuse_server::build_router(state)
}

#[tokio::test]
async fn test_server_handles_injection_in_where_clause() {
    for payload in PAYLOADS {
        let query = format!("SELECT * FROM testds.logs WHERE name = '{}'", payload);
        let (status, _) = post_query(build_test_app(), &query, "sql").await;
        // Must not panic or return 5xx server error
        assert_ne!(status, StatusCode::INTERNAL_SERVER_ERROR,
            "Server crashed on payload: {}", payload);
    }
}

#[tokio::test]
async fn test_server_handles_injection_in_table_name() {
    let payloads = &[
        "testds.logs; DROP TABLE users",
        "testds.logs UNION SELECT * FROM secrets",
        "testds.logs' --",
    ];
    for payload in payloads {
        let query = format!("SELECT * FROM {}", payload);
        let (status, _) = post_query(build_test_app(), &query, "sql").await;
        assert_ne!(status, StatusCode::INTERNAL_SERVER_ERROR,
            "Server crashed on table injection: {}", payload);
    }
}
