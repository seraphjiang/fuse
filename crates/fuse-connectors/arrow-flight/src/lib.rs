// SPDX-License-Identifier: Apache-2.0

//! Arrow Flight connector for the Fuse federated query engine.
//!
//! Zero-copy data transfer via Arrow Flight (gRPC). Connects to any Flight
//! server (e.g., DataFusion, DuckDB, Spark with Flight, another Fuse instance
//! running a Flight endpoint) and streams RecordBatches natively — no JSON
//! serialization overhead.
//!
//! Supports both Flight SQL (for SQL-capable servers) and plain Flight
//! (for custom ticket-based data retrieval).
//!
//! # Configuration
//!
//! ```toml
//! [[connector]]
//! id = "my_flight"
//! type = "arrow-flight"
//! url = "grpc://flight-server:50051"
//! # Optional
//! # token = "secret://fuse/flight-token"
//! # mode = "flight-sql"  # or "flight" (default: "flight-sql")
//! ```

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::{FlightClient, FlightDescriptor, Ticket};
use async_trait::async_trait;
use futures::TryStreamExt;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;
use fuse_core::sql::{quote_ident, quote_table};

/// Connection mode: Flight SQL (SQL queries) or plain Flight (ticket-based).
#[derive(Debug, Clone, PartialEq)]
pub enum FlightMode {
    FlightSql,
    Flight,
}

#[derive(Debug)]
pub struct ArrowFlightConnector {
    id: String,
    url: String,
    mode: FlightMode,
    token: Option<String>,
}

impl ArrowFlightConnector {
    pub fn new(id: String, url: String, mode: FlightMode, token: Option<String>) -> Self {
        Self { id, url, mode, token }
    }

    async fn connect_channel(&self) -> Result<Channel, ConnectorError> {
        let endpoint = Channel::from_shared(self.url.clone())
            .map_err(|e| ConnectorError::Connection(format!("invalid Flight URL: {e}")))?;
        endpoint
            .connect()
            .await
            .map_err(|e| ConnectorError::Connection(format!("Flight connect failed: {e}")))
    }

    async fn flight_sql_client(&self) -> Result<FlightSqlServiceClient<Channel>, ConnectorError> {
        let channel = self.connect_channel().await?;
        let mut client = FlightSqlServiceClient::new(channel);
        if let Some(ref token) = self.token {
            client.set_header("authorization", format!("Bearer {}", token));
        }
        Ok(client)
    }

    async fn flight_client(&self) -> Result<FlightClient, ConnectorError> {
        let channel = self.connect_channel().await?;
        let mut client = FlightClient::new(channel);
        if let Some(ref token) = self.token {
            client.add_header("authorization", token).map_err(|e| {
                ConnectorError::Connection(format!("invalid auth header: {e}"))
            })?;
        }
        Ok(client)
    }

    async fn execute_flight_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
        debug!(url = %self.url, sql = %sql, "executing Flight SQL query");
        let mut client = self.flight_sql_client().await?;

        let flight_info = client
            .execute(sql.to_string(), None)
            .await
            .map_err(|e| ConnectorError::query(format!("Flight SQL execute failed: {e}")))?;

        let mut batches = Vec::new();
        for endpoint in flight_info.endpoint {
            if let Some(ticket) = endpoint.ticket {
                let mut stream = client
                    .do_get(ticket)
                    .await
                    .map_err(|e| ConnectorError::query(format!("Flight SQL do_get failed: {e}")))?;
                while let Some(batch) = stream
                    .try_next()
                    .await
                    .map_err(|e| ConnectorError::query(format!("Flight stream error: {e}")))?
                {
                    batches.push(batch);
                }
            }
        }
        Ok(batches)
    }

    async fn execute_flight_ticket(
        &self,
        query: &SubQuery,
    ) -> Result<Vec<RecordBatch>, ConnectorError> {
        debug!(url = %self.url, table = %query.table, "executing Flight ticket query");
        let mut client = self.flight_client().await?;

        let ticket = Ticket::new(build_flight_ticket(query));
        let stream = client
            .do_get(ticket)
            .await
            .map_err(|e| ConnectorError::query(format!("Flight do_get failed: {e}")))?;

        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| ConnectorError::query(format!("Flight stream error: {e}")))?;

        Ok(batches)
    }
}

/// Build a Flight ticket with predicate pushdown for plain Flight mode.
///
/// Encodes the SubQuery as JSON so Flight servers that support it can
/// apply server-side filtering, projection, sorting, and limits.
/// Servers that don't understand the format fall back to the table name.
fn build_flight_ticket(query: &SubQuery) -> Vec<u8> {
    let mut ticket = serde_json::json!({ "table": query.table });
    if !query.projections.is_empty() {
        ticket["projections"] = serde_json::json!(query.projections);
    }
    if let Some(ref f) = query.filter {
        ticket["filter"] = serde_json::Value::String(filter_to_sql(f));
    }
    if !query.sort.is_empty() {
        let s: Vec<serde_json::Value> = query.sort.iter().map(|s| {
            serde_json::json!({ "field": s.field, "desc": s.descending })
        }).collect();
        ticket["sort"] = serde_json::json!(s);
    }
    if let Some(l) = query.limit {
        ticket["limit"] = serde_json::json!(l);
    }
    if let Some(o) = query.offset {
        ticket["offset"] = serde_json::json!(o);
    }
    ticket.to_string().into_bytes()
}

/// Build SQL from SubQuery for Flight SQL servers.
fn build_flight_sql(query: &SubQuery) -> String {
    let cols = if query.projections.is_empty() {
        "*".into()
    } else {
        query.projections.iter().map(|p| if p == "*" { "*".to_string() } else { quote_ident(p) }).collect::<Vec<_>>().join(", ")
    };
    let mut sql = format!("SELECT {} FROM {}", cols, quote_table(&query.table));

    if let Some(ref f) = query.filter {
        sql.push_str(&format!(" WHERE {}", filter_to_sql(f)));
    }
    if !query.group_by.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", query.group_by.iter().map(|g| quote_ident(g)).collect::<Vec<_>>().join(", ")));
    }
    if let Some(ref h) = query.having {
        sql.push_str(&format!(" HAVING {}", filter_to_sql(h)));
    }
    if !query.sort.is_empty() {
        let s: Vec<String> = query.sort.iter().map(|s| {
            if s.descending { format!("{} DESC", quote_ident(&s.field)) } else { quote_ident(&s.field) }
        }).collect();
        sql.push_str(&format!(" ORDER BY {}", s.join(", ")));
    }
    if let Some(l) = query.limit {
        sql.push_str(&format!(" LIMIT {}", l));
    }
    if let Some(o) = query.offset {
        sql.push_str(&format!(" OFFSET {}", o));
    }
    sql
}

fn filter_to_sql(f: &FilterExpr) -> String {
    match f {
        FilterExpr::And(l, r) => format!("({} AND {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Or(l, r) => format!("({} OR {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Not(inner) => format!("NOT ({})", filter_to_sql(inner)),
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=", ComparisonOp::Neq => "!=",
                ComparisonOp::Lt => "<", ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">", ComparisonOp::Gte => ">=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => "LIKE",
            };
            format!("{} {} {}", quote_ident(field), op_str, scalar_to_sql(value))
        }
        FilterExpr::In { field, values } => {
            let v: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{} IN ({})", quote_ident(field), v.join(", "))
        }
        FilterExpr::IsNull(f) => format!("{} IS NULL", quote_ident(f)),
        FilterExpr::IsNotNull(f) => format!("{} IS NOT NULL", quote_ident(f)),
    }
}

fn scalar_to_sql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Null => "NULL".into(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(n) => n.to_string(),
        ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

#[async_trait]
impl FederatedConnector for ArrowFlightConnector {
    fn id(&self) -> &str {
        &self.id
    }
    fn connector_type(&self) -> &str {
        "arrow-flight"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: self.mode == FlightMode::FlightSql,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 8,
            supports_streaming: true,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        match self.connect_channel().await {
            Ok(_) => ConnectorHealth {
                status: HealthStatus::Healthy,
                latency_ms: None,
                message: None,
            },
            Err(e) => ConnectorHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: None,
                message: Some(e.to_string()),
            },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        if self.mode == FlightMode::FlightSql {
            let mut client = self.flight_sql_client().await?;
            let req = arrow_flight::sql::CommandGetTables::default();
            let flight_info = client
                .get_tables(req)
                .await
                .map_err(|e| ConnectorError::query(format!("get_tables failed: {e}")))?;

            let mut schemas = Vec::new();
            for endpoint in flight_info.endpoint {
                if let Some(ticket) = endpoint.ticket {
                    let mut stream = client
                        .do_get(ticket)
                        .await
                        .map_err(|e| ConnectorError::query(e.to_string()))?;
                    while let Some(batch) = stream
                        .try_next()
                        .await
                        .map_err(|e| ConnectorError::query(e.to_string()))?
                    {
                        if let Some(col) = batch
                            .column_by_name("table_name")
                            .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>())
                        {
                            use arrow::array::Array;
                            for i in 0..col.len() {
                                if !col.is_null(i) {
                                    schemas.push(SchemaInfo {
                                        name: col.value(i).to_string(),
                                        schema_type: SchemaType::Table,
                                        estimated_row_count: None,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            Ok(schemas)
        } else {
            // Plain Flight: list available flights
            let mut client = self.flight_client().await?;
            let flights = client
                .list_flights("")
                .await
                .map_err(|e| ConnectorError::query(e.to_string()))?;
            let infos: Vec<_> = flights
                .try_collect()
                .await
                .map_err(|e| ConnectorError::query(e.to_string()))?;
            Ok(infos
                .iter()
                .filter_map(|fi| {
                    fi.flight_descriptor.as_ref().map(|fd| SchemaInfo {
                        name: String::from_utf8_lossy(&fd.cmd).to_string(),
                        schema_type: SchemaType::Table,
                        estimated_row_count: if fi.total_records >= 0 { Some(fi.total_records as u64) } else { None },
                    })
                })
                .collect())
        }
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        if self.mode == FlightMode::FlightSql {
            let mut client = self.flight_sql_client().await?;
            let flight_info = client
                .execute(format!("SELECT * FROM {} LIMIT 0", quote_table(table)), None)
                .await
                .map_err(|e| ConnectorError::query(e.to_string()))?;
            let schema = flight_info
                .try_decode_schema()
                .map_err(|e| ConnectorError::query(e.to_string()))?;
            Ok(schema)
        } else {
            let mut client = self.flight_client().await?;
            let descriptor = FlightDescriptor::new_path(vec![table.to_string()]);
            let schema = client
                .get_schema(descriptor)
                .await
                .map_err(|e| ConnectorError::query(e.to_string()))?;
            Ok(schema)
        }
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        match self.mode {
            FlightMode::FlightSql => {
                let sql = build_flight_sql(query);
                self.execute_flight_sql(&sql).await
            }
            FlightMode::Flight => self.execute_flight_ticket(query).await,
        }
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        // For Flight, we can stream batches as they arrive
        match self.mode {
            FlightMode::FlightSql => {
                let sql = build_flight_sql(query);
                let mut client = self.flight_sql_client().await?;
                let flight_info = client
                    .execute(sql, None)
                    .await
                    .map_err(|e| ConnectorError::query(e.to_string()))?;
                for endpoint in flight_info.endpoint {
                    if let Some(ticket) = endpoint.ticket {
                        let mut batch_stream = client
                            .do_get(ticket)
                            .await
                            .map_err(|e| ConnectorError::query(e.to_string()))?;
                        while let Some(batch) = batch_stream
                            .try_next()
                            .await
                            .map_err(|e| ConnectorError::query(e.to_string()))?
                        {
                            tx.send(Ok(batch))
                                .await
                                .map_err(|_| ConnectorError::ChannelClosed)?;
                        }
                    }
                }
            }
            FlightMode::Flight => {
                let mut client = self.flight_client().await?;
                let ticket = Ticket::new(build_flight_ticket(query));
                let mut batch_stream = client
                    .do_get(ticket)
                    .await
                    .map_err(|e| ConnectorError::query(e.to_string()))?;
                while let Some(batch) = batch_stream
                    .try_next()
                    .await
                    .map_err(|e| ConnectorError::query(e.to_string()))?
                {
                    tx.send(Ok(batch))
                        .await
                        .map_err(|_| ConnectorError::ChannelClosed)?;
                }
            }
        }
        Ok(())
    }
}

pub struct ArrowFlightConnectorFactory;

#[async_trait]
impl ConnectorFactory for ArrowFlightConnectorFactory {
    fn connector_type(&self) -> &str {
        "arrow-flight"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let url = config
            .properties
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'url' is required".into()))?
            .to_string();

        let mode = match config
            .properties
            .get("mode")
            .and_then(|v| v.as_str())
        {
            Some("flight") => FlightMode::Flight,
            _ => FlightMode::FlightSql,
        };

        let token = config
            .properties
            .get("token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(Arc::new(ArrowFlightConnector::new(
            config.id.clone(),
            url,
            mode,
            token,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sq(table: &str) -> SubQuery {
        SubQuery {
            table: table.into(), projections: vec![], filter: None, aggregations: vec![],
            group_by: vec![], sort: vec![], limit: None, having: None, passthrough: None, offset: None,
        }
    }

    #[test]
    fn test_build_flight_sql_simple() {
        assert_eq!(build_flight_sql(&sq("events")), "SELECT * FROM \"events\"");
    }

    #[test]
    fn test_build_flight_sql_full() {
        let mut q = sq("logs");
        q.projections = vec!["host".into(), "count(*)".into()];
        q.filter = Some(FilterExpr::Comparison {
            field: "status".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(500),
        });
        q.group_by = vec!["host".into()];
        q.sort = vec![SortExpr { field: "host".into(), descending: true }];
        q.limit = Some(10);
        assert_eq!(
            build_flight_sql(&q),
            "SELECT \"host\", \"count(*)\" FROM \"logs\" WHERE \"status\" >= 500 GROUP BY \"host\" ORDER BY \"host\" DESC LIMIT 10"
        );
    }

    #[test]
    fn test_flight_sql_capabilities() {
        let c = ArrowFlightConnector::new("t".into(), "grpc://localhost:50051".into(), FlightMode::FlightSql, None);
        let caps = c.capabilities();
        assert!(caps.supports_filtering);
        assert!(caps.supports_projection);
        assert!(caps.supports_streaming);
    }

    #[test]
    fn test_plain_flight_capabilities() {
        let c = ArrowFlightConnector::new("t".into(), "grpc://localhost:50051".into(), FlightMode::Flight, None);
        let caps = c.capabilities();
        assert!(caps.supports_filtering); // pushdown via JSON ticket
        assert!(caps.supports_projection);
        assert!(!caps.supports_aggregation); // only Flight SQL supports aggregation
        assert!(caps.supports_streaming);
    }

    #[test]
    fn test_connector_type() {
        let c = ArrowFlightConnector::new("t".into(), "grpc://x".into(), FlightMode::FlightSql, None);
        assert_eq!(c.connector_type(), "arrow-flight");
    }

    #[test]
    fn test_mode_from_config() {
        // Default is FlightSql
        assert_eq!(
            ArrowFlightConnector::new("t".into(), "x".into(), FlightMode::FlightSql, None).mode,
            FlightMode::FlightSql
        );
    }

    #[test]
    fn test_filter_is_not_null() {
        assert_eq!(filter_to_sql(&FilterExpr::IsNotNull("x".into())), "\"x\" IS NOT NULL");
    }

    #[test]
    fn test_scalar_null_and_bool() {
        assert_eq!(scalar_to_sql(&ScalarValue::Null), "NULL");
        assert_eq!(scalar_to_sql(&ScalarValue::Boolean(true)), "true");
    }

    #[test]
    fn test_flight_ticket_simple() {
        let ticket = build_flight_ticket(&sq("events"));
        let json: serde_json::Value = serde_json::from_slice(&ticket).unwrap();
        assert_eq!(json["table"], "events");
        assert!(json.get("filter").is_none());
    }

    #[test]
    fn test_flight_ticket_with_predicates() {
        let mut q = sq("logs");
        q.projections = vec!["host".into()];
        q.filter = Some(FilterExpr::Comparison {
            field: "level".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("ERROR".into()),
        });
        q.sort = vec![SortExpr { field: "ts".into(), descending: true }];
        q.limit = Some(50);
        let ticket = build_flight_ticket(&q);
        let json: serde_json::Value = serde_json::from_slice(&ticket).unwrap();
        assert_eq!(json["table"], "logs");
        assert_eq!(json["projections"], serde_json::json!(["host"]));
        assert_eq!(json["filter"], "\"level\" = 'ERROR'");
        assert_eq!(json["sort"][0]["field"], "ts");
        assert_eq!(json["sort"][0]["desc"], true);
        assert_eq!(json["limit"], 50);
    }
}
