// SPDX-License-Identifier: Apache-2.0
//! GraphQL API for Fuse — alternative to REST (#1810).
//! Exposes datasources, schemas, fields, query execution, saved queries,
//! views, history, and health via `/api/fuse/graphql`.

use std::sync::Arc;

use async_graphql::{Context, EmptySubscription, InputObject, Object, Schema, SimpleObject};
use axum::response::IntoResponse;

use crate::api::AppState;

// ---------------------------------------------------------------------------
// Output types
// ---------------------------------------------------------------------------

#[derive(SimpleObject)]
pub struct Datasource {
    pub id: String,
    pub connector_type: String,
}

#[derive(SimpleObject)]
pub struct TableSchema {
    pub name: String,
}

#[derive(SimpleObject)]
pub struct Field {
    pub name: String,
    pub field_type: String,
    pub nullable: bool,
}

#[derive(SimpleObject)]
pub struct SavedQuery {
    pub name: String,
    pub query: String,
    pub format: String,
}

#[derive(SimpleObject)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<async_graphql::Json<Vec<serde_json::Value>>>,
    pub row_count: u64,
    pub latency_ms: u64,
}

#[derive(SimpleObject)]
pub struct HistoryEntry {
    pub query: String,
    pub format: String,
    pub timestamp: u64,
    pub latency_ms: u64,
    pub row_count: u64,
    pub error: Option<String>,
}

#[derive(SimpleObject)]
pub struct ViewInfo {
    pub name: String,
    pub query: String,
    pub stale: bool,
}

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

#[derive(InputObject)]
pub struct SaveQueryInput {
    pub name: String,
    pub query: String,
    #[graphql(default_with = "\"sql\".to_string()")]
    pub format: String,
}

#[derive(InputObject)]
pub struct ExecuteQueryInput {
    pub query: String,
    #[graphql(default_with = "\"sql\".to_string()")]
    pub format: String,
}

#[derive(InputObject)]
pub struct CreateViewInput {
    pub name: String,
    pub query: String,
}

// ---------------------------------------------------------------------------
// Query root
// ---------------------------------------------------------------------------

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// List all registered datasources.
    async fn datasources(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<Datasource>> {
        let state = ctx.data::<Arc<AppState>>()?;
        Ok(state
            .registry
            .connectors()
            .into_iter()
            .map(|(id, c)| Datasource {
                id,
                connector_type: c.connector_type().to_string(),
            })
            .collect())
    }

    /// List tables for a datasource.
    async fn schemas(
        &self,
        ctx: &Context<'_>,
        datasource_id: String,
    ) -> async_graphql::Result<Vec<TableSchema>> {
        let state = ctx.data::<Arc<AppState>>()?;
        let connector = state
            .registry
            .get(&datasource_id)
            .ok_or_else(|| {
                async_graphql::Error::new(format!("datasource '{}' not found", datasource_id))
            })?;
        let tables = connector
            .discover_schemas()
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(tables
            .into_iter()
            .map(|si| TableSchema { name: si.name })
            .collect())
    }

    /// Get fields for a table in a datasource.
    async fn fields(
        &self,
        ctx: &Context<'_>,
        datasource_id: String,
        table: String,
    ) -> async_graphql::Result<Vec<Field>> {
        let state = ctx.data::<Arc<AppState>>()?;
        let connector = state
            .registry
            .get(&datasource_id)
            .ok_or_else(|| {
                async_graphql::Error::new(format!("datasource '{}' not found", datasource_id))
            })?;
        let schema = connector
            .get_schema(&table)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(schema
            .fields()
            .iter()
            .map(|f| Field {
                name: f.name().to_string(),
                field_type: f.data_type().to_string(),
                nullable: f.is_nullable(),
            })
            .collect())
    }

    /// List saved queries.
    async fn saved_queries(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<SavedQuery>> {
        let state = ctx.data::<Arc<AppState>>()?;
        Ok(state
            .saved_queries
            .list()
            .into_iter()
            .map(|sq| SavedQuery {
                name: sq.name,
                query: sq.query,
                format: sq.format,
            })
            .collect())
    }

    /// Recent query history.
    async fn history(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 50)] limit: usize,
    ) -> async_graphql::Result<Vec<HistoryEntry>> {
        let state = ctx.data::<Arc<AppState>>()?;
        Ok(state
            .history
            .recent(limit)
            .into_iter()
            .map(|h| HistoryEntry {
                query: h.query,
                format: h.format,
                timestamp: h.timestamp,
                latency_ms: h.latency_ms,
                row_count: h.row_count,
                error: h.error,
            })
            .collect())
    }

    /// List materialized views.
    async fn views(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<ViewInfo>> {
        let state = ctx.data::<Arc<AppState>>()?;
        let names = state.view_registry.list();
        Ok(names
            .into_iter()
            .filter_map(|name| {
                let view_arc = state.view_registry.get(&name)?;
                let view = view_arc.read().unwrap();
                Some(ViewInfo {
                    name,
                    query: view.def.query.clone(),
                    stale: view.needs_refresh(),
                })
            })
            .collect())
    }

    /// Health check.
    async fn health(&self) -> &str {
        "ok"
    }
}

// ---------------------------------------------------------------------------
// Mutation root
// ---------------------------------------------------------------------------

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Execute a SQL or PPL query and return results.
    async fn execute_query(
        &self,
        ctx: &Context<'_>,
        input: ExecuteQueryInput,
    ) -> async_graphql::Result<QueryResult> {
        let state = ctx.data::<Arc<AppState>>()?;
        let t0 = std::time::Instant::now();
        let format = input.format.to_lowercase();
        let query = crate::api::rewrite_contains(&input.query);

        let refs = match format.as_str() {
            "ppl" => crate::api::parse_ppl_sources(&query),
            _ => crate::api::parse_sql_sources(&query),
        }
        .map_err(|e| async_graphql::Error::new(e))?;

        if refs.is_empty() {
            return Err(async_graphql::Error::new(
                "no datasource.table references found",
            ));
        }

        let mut all_batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
        for (ds_id, table) in &refs {
            let connector = state.registry.get(ds_id).ok_or_else(|| {
                async_graphql::Error::new(format!("datasource '{}' not found", ds_id))
            })?;
            let sq = crate::api::build_sub_query(&query, &format, table)
                .map_err(|e| async_graphql::Error::new(e))?;
            let batches = connector
                .execute(&sq)
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
            all_batches.extend(batches);
        }

        let (columns, rows) = crate::api::batches_to_json(&all_batches);
        let row_count = rows.len() as u64;
        let latency_ms = t0.elapsed().as_millis() as u64;

        state.history.push(crate::history::HistoryEntry {
            query: input.query,
            format: input.format,
            timestamp: crate::history::now_secs(),
            latency_ms,
            row_count,
            error: None,
        });

        Ok(QueryResult {
            columns,
            rows: rows.into_iter().map(async_graphql::Json).collect(),
            row_count,
            latency_ms,
        })
    }

    /// Save a query.
    async fn save_query(
        &self,
        ctx: &Context<'_>,
        input: SaveQueryInput,
    ) -> async_graphql::Result<SavedQuery> {
        let state = ctx.data::<Arc<AppState>>()?;
        state.saved_queries.save(crate::saved_queries::SavedQuery {
            name: input.name.clone(),
            query: input.query.clone(),
            format: input.format.clone(),
            description: String::new(),
        });
        Ok(SavedQuery {
            name: input.name,
            query: input.query,
            format: input.format,
        })
    }

    /// Delete a saved query.
    async fn delete_saved_query(
        &self,
        ctx: &Context<'_>,
        name: String,
    ) -> async_graphql::Result<bool> {
        let state = ctx.data::<Arc<AppState>>()?;
        Ok(state.saved_queries.delete(&name))
    }

    /// Create a materialized view.
    async fn create_view(
        &self,
        ctx: &Context<'_>,
        input: CreateViewInput,
    ) -> async_graphql::Result<ViewInfo> {
        let state = ctx.data::<Arc<AppState>>()?;
        state.view_registry.register(
            fuse_engine::materialized::MaterializedViewDef {
                name: input.name.clone(),
                query: input.query.clone(),
                refresh_interval: std::time::Duration::from_secs(300),
            },
        );
        Ok(ViewInfo {
            name: input.name,
            query: input.query,
            stale: true,
        })
    }

    /// Delete a materialized view.
    async fn delete_view(
        &self,
        ctx: &Context<'_>,
        name: String,
    ) -> async_graphql::Result<bool> {
        let state = ctx.data::<Arc<AppState>>()?;
        Ok(state.view_registry.remove(&name))
    }
}

// ---------------------------------------------------------------------------
// Schema + handlers
// ---------------------------------------------------------------------------

pub type FuseSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

pub fn build_schema(state: Arc<AppState>) -> FuseSchema {
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(state)
        .finish()
}

/// POST /api/fuse/graphql
pub async fn graphql_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    req: async_graphql_axum::GraphQLRequest,
) -> impl IntoResponse {
    let schema = build_schema(state);
    async_graphql_axum::GraphQLResponse::from(schema.execute(req.into_inner()).await)
}

/// GET /api/fuse/graphql — GraphiQL playground
pub async fn graphiql_handler() -> impl IntoResponse {
    axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint("/api/fuse/graphql")
            .finish(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql::Value;
    use crate::api::AppState;
    use fuse_core::registry::ConnectorRegistry;

    fn test_state() -> Arc<AppState> {
        Arc::new(AppState {
            registry: Arc::new(ConnectorRegistry::new()),
            alert_rules: vec![],
            view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
            history: Arc::new(crate::history::QueryHistory::new()),
            running_queries: Arc::new(crate::api::RunningQueries::new()),
            saved_queries: Arc::new(crate::saved_queries::SavedQueryRegistry::new()),
            plan_cache: Arc::new(crate::plan_cache::PlanCache::new(300, 1000)),
            result_cache: Arc::new(crate::plan_cache::ResultCache::new(60, 500)),
            tenant_registry: Arc::new(crate::tenant::TenantRegistry::disabled()),
            audit_log: Arc::new(crate::audit::AuditLog::new(10000)),
            prepared_statements: crate::prepared::new_store(),
            adaptive_timeout: Arc::new(crate::adaptive_timeout::AdaptiveTimeout::new()),
            shared_saved_queries: crate::shared_state::SharedSavedQueries::InMemory(Arc::new(crate::saved_queries::SavedQueryRegistry::new())),
            shared_history: crate::shared_state::SharedQueryHistory::InMemory(Arc::new(crate::history::QueryHistory::new())),
            shared_audit_log: crate::shared_state::SharedAuditLog::InMemory(Arc::new(crate::audit::AuditLog::new(1000))),
            transactions: Arc::new(crate::transaction::TransactionStore::new()),
            max_result_bytes: 0,
            datasource_limiter: Arc::new(crate::rate_limit::DatasourceLimiter::new()),
            otel_store: None,
            adaptive_parallelism: Arc::new(crate::adaptive_parallelism::AdaptiveParallelism::new()),
            compilation_cache: Arc::new(crate::query_compilation::CompilationCache::new(300, 5000)),
        })
    }

    #[tokio::test]
    async fn test_graphql_health() {
        let schema = build_schema(test_state());
        let res = schema.execute("{ health }").await;
        assert!(res.errors.is_empty());
        assert_eq!(res.data, Value::from_json(serde_json::json!({"health": "ok"})).unwrap());
    }

    #[tokio::test]
    async fn test_graphql_datasources_empty() {
        let schema = build_schema(test_state());
        let res = schema.execute("{ datasources { id connectorType } }").await;
        assert!(res.errors.is_empty());
        assert_eq!(res.data, Value::from_json(serde_json::json!({"datasources": []})).unwrap());
    }

    #[tokio::test]
    async fn test_graphql_saved_queries_crud() {
        let schema = build_schema(test_state());

        let res = schema
            .execute(r#"mutation { saveQuery(input: { name: "q1", query: "SELECT 1", format: "sql" }) { name query } }"#)
            .await;
        assert!(res.errors.is_empty());

        let res = schema.execute("{ savedQueries { name query format } }").await;
        assert!(res.errors.is_empty());
        let data = res.data.into_json().unwrap();
        assert_eq!(data["savedQueries"][0]["name"], "q1");

        let res = schema.execute(r#"mutation { deleteSavedQuery(name: "q1") }"#).await;
        assert!(res.errors.is_empty());
    }

    #[tokio::test]
    async fn test_graphql_introspection() {
        let schema = build_schema(test_state());
        let res = schema
            .execute("{ __schema { queryType { name } mutationType { name } } }")
            .await;
        assert!(res.errors.is_empty());
    }

    #[tokio::test]
    async fn test_graphql_history_empty() {
        let schema = build_schema(test_state());
        let res = schema.execute("{ history { query format latencyMs } }").await;
        assert!(res.errors.is_empty());
        assert_eq!(res.data, Value::from_json(serde_json::json!({"history": []})).unwrap());
    }

    #[tokio::test]
    async fn test_graphql_views_empty() {
        let schema = build_schema(test_state());
        let res = schema.execute("{ views { name query stale } }").await;
        assert!(res.errors.is_empty());
        assert_eq!(res.data, Value::from_json(serde_json::json!({"views": []})).unwrap());
    }

    #[tokio::test]
    async fn test_graphql_view_crud() {
        let schema = build_schema(test_state());

        let res = schema
            .execute(r#"mutation { createView(input: { name: "v1", query: "SELECT 1" }) { name query stale } }"#)
            .await;
        assert!(res.errors.is_empty());
        let data = res.data.into_json().unwrap();
        assert_eq!(data["createView"]["name"], "v1");
        assert_eq!(data["createView"]["stale"], true);

        let res = schema.execute("{ views { name } }").await;
        assert!(res.errors.is_empty());
        let data = res.data.into_json().unwrap();
        assert_eq!(data["views"][0]["name"], "v1");

        let res = schema.execute(r#"mutation { deleteView(name: "v1") }"#).await;
        assert!(res.errors.is_empty());
    }

    #[tokio::test]
    async fn test_graphql_mutation_fields_present() {
        let schema = build_schema(test_state());
        let res = schema
            .execute(r#"{ __type(name: "MutationRoot") { fields { name } } }"#)
            .await;
        assert!(res.errors.is_empty());
        let data = res.data.into_json().unwrap();
        let fields: Vec<String> = data["__type"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap().to_string())
            .collect();
        assert!(fields.contains(&"executeQuery".to_string()));
        assert!(fields.contains(&"saveQuery".to_string()));
        assert!(fields.contains(&"deleteSavedQuery".to_string()));
        assert!(fields.contains(&"createView".to_string()));
        assert!(fields.contains(&"deleteView".to_string()));
    }

    #[tokio::test]
    async fn test_graphql_query_fields_present() {
        let schema = build_schema(test_state());
        let res = schema
            .execute(r#"{ __type(name: "QueryRoot") { fields { name } } }"#)
            .await;
        assert!(res.errors.is_empty());
        let data = res.data.into_json().unwrap();
        let fields: Vec<String> = data["__type"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap().to_string())
            .collect();
        assert!(fields.contains(&"datasources".to_string()));
        assert!(fields.contains(&"schemas".to_string()));
        assert!(fields.contains(&"fields".to_string()));
        assert!(fields.contains(&"savedQueries".to_string()));
        assert!(fields.contains(&"history".to_string()));
        assert!(fields.contains(&"views".to_string()));
        assert!(fields.contains(&"health".to_string()));
    }
}
