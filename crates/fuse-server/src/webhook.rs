// SPDX-License-Identifier: Apache-2.0
//! Webhook Subscriptions (#1811).
//!
//! Register a callback URL + query + condition. When evaluated (on schedule or
//! via API), the query runs and the webhook fires if the condition is met.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

/// Condition that triggers the webhook.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WebhookCondition {
    /// Fire when query returns any rows.
    RowsReturned,
    /// Fire when a column value exceeds a threshold.
    Threshold { column: String, operator: ThresholdOp, value: f64 },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThresholdOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
}

impl ThresholdOp {
    pub fn evaluate(&self, actual: f64, threshold: f64) -> bool {
        match self {
            Self::Gt => actual > threshold,
            Self::Gte => actual >= threshold,
            Self::Lt => actual < threshold,
            Self::Lte => actual <= threshold,
            Self::Eq => (actual - threshold).abs() < f64::EPSILON,
        }
    }
}

/// A registered webhook subscription.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookSubscription {
    pub id: String,
    pub name: String,
    /// SQL or PPL query to evaluate.
    pub query: String,
    #[serde(default = "default_format")]
    pub format: String,
    /// Condition that triggers the callback.
    pub condition: WebhookCondition,
    /// URL to POST when condition is met.
    pub callback_url: String,
    pub enabled: bool,
    /// Number of times this webhook has fired.
    #[serde(default)]
    pub fire_count: u64,
    /// Last evaluation timestamp (unix secs).
    #[serde(default)]
    pub last_evaluated: Option<u64>,
    /// Last error from evaluation or delivery.
    #[serde(default)]
    pub last_error: Option<String>,
}

fn default_format() -> String {
    "sql".to_string()
}

/// Payload sent to the callback URL.
#[derive(Debug, Clone, Serialize)]
pub struct WebhookPayload {
    pub subscription_id: String,
    pub subscription_name: String,
    pub query: String,
    pub row_count: u64,
    pub columns: Vec<String>,
    pub sample_rows: Vec<Vec<serde_json::Value>>,
    pub fired_at: u64,
}

/// In-memory registry of webhook subscriptions.
pub struct WebhookRegistry {
    subs: RwLock<HashMap<String, WebhookSubscription>>,
    next_id: std::sync::atomic::AtomicU64,
}

impl WebhookRegistry {
    pub fn new() -> Self {
        Self {
            subs: RwLock::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn register(&self, mut sub: WebhookSubscription) -> String {
        let id = format!(
            "wh-{}",
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        sub.id = id.clone();
        self.subs.write().unwrap().insert(id.clone(), sub);
        id
    }

    pub fn get(&self, id: &str) -> Option<WebhookSubscription> {
        self.subs.read().unwrap().get(id).cloned()
    }

    pub fn list(&self) -> Vec<WebhookSubscription> {
        self.subs.read().unwrap().values().cloned().collect()
    }

    pub fn delete(&self, id: &str) -> bool {
        self.subs.write().unwrap().remove(id).is_some()
    }

    pub fn update_after_fire(&self, id: &str, timestamp: u64, error: Option<String>) {
        if let Some(sub) = self.subs.write().unwrap().get_mut(id) {
            sub.fire_count += 1;
            sub.last_evaluated = Some(timestamp);
            sub.last_error = error;
        }
    }

    pub fn update_after_eval(&self, id: &str, timestamp: u64, error: Option<String>) {
        if let Some(sub) = self.subs.write().unwrap().get_mut(id) {
            sub.last_evaluated = Some(timestamp);
            sub.last_error = error;
        }
    }

    pub fn enabled_subscriptions(&self) -> Vec<WebhookSubscription> {
        self.subs
            .read()
            .unwrap()
            .values()
            .filter(|s| s.enabled)
            .cloned()
            .collect()
    }
}

/// Check whether the condition is met given query results.
pub fn evaluate_condition(
    condition: &WebhookCondition,
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
) -> bool {
    match condition {
        WebhookCondition::RowsReturned => !rows.is_empty(),
        WebhookCondition::Threshold {
            column,
            operator,
            value,
        } => {
            let col_idx = columns.iter().position(|c| c == column);
            let col_idx = match col_idx {
                Some(i) => i,
                None => return false,
            };
            // Check first row's value
            rows.first()
                .and_then(|row| row.get(col_idx))
                .and_then(|v| v.as_f64())
                .map(|actual| operator.evaluate(actual, *value))
                .unwrap_or(false)
        }
    }
}

/// Deliver a webhook payload to the callback URL.
pub async fn deliver_webhook(url: &str, payload: &WebhookPayload) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| format!("client build failed: {e}"))?;
    let resp = client
        .post(url)
        .header("Content-Type", "application/json")
        .header("X-Fuse-Event", "webhook.fired")
        .json(payload)
        .send()
        .await
        .map_err(|e| format!("webhook delivery failed: {e}"))?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!("webhook returned HTTP {}", resp.status()))
    }
}

/// Build REST routes for webhook subscriptions.
pub fn webhook_routes() -> axum::Router<Arc<crate::api::AppState>> {
    use axum::routing::{delete, get, post};
    axum::Router::new()
        .route("/", get(list_webhooks).post(create_webhook))
        .route("/{id}", get(get_webhook).delete(delete_webhook))
        .route("/{id}/test", post(test_webhook))
}

// --- Handlers ---

async fn list_webhooks(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    axum::Json(state.webhook_registry.list()).into_response()
}

#[derive(Deserialize)]
pub struct CreateWebhookRequest {
    pub name: String,
    pub query: String,
    #[serde(default = "default_format")]
    pub format: String,
    pub condition: WebhookCondition,
    pub callback_url: String,
}

async fn create_webhook(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    axum::Json(req): axum::Json<CreateWebhookRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let sub = WebhookSubscription {
        id: String::new(),
        name: req.name,
        query: req.query,
        format: req.format,
        condition: req.condition,
        callback_url: req.callback_url,
        enabled: true,
        fire_count: 0,
        last_evaluated: None,
        last_error: None,
    };
    let id = state.webhook_registry.register(sub);
    (
        axum::http::StatusCode::CREATED,
        axum::Json(serde_json::json!({ "id": id })),
    )
        .into_response()
}

async fn get_webhook(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match state.webhook_registry.get(&id) {
        Some(sub) => axum::Json(sub).into_response(),
        None => (
            axum::http::StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error": "not found"})),
        )
            .into_response(),
    }
}

async fn delete_webhook(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if state.webhook_registry.delete(&id) {
        axum::Json(serde_json::json!({"deleted": true})).into_response()
    } else {
        (
            axum::http::StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error": "not found"})),
        )
            .into_response()
    }
}

/// Test-fire a webhook: run the query, evaluate condition, deliver if met.
async fn test_webhook(
    axum::extract::State(state): axum::extract::State<Arc<crate::api::AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let sub = match state.webhook_registry.get(&id) {
        Some(s) => s,
        None => {
            return (
                axum::http::StatusCode::NOT_FOUND,
                axum::Json(serde_json::json!({"error": "not found"})),
            )
                .into_response()
        }
    };

    // Execute the query
    let refs = match sub.format.as_str() {
        "ppl" => crate::api::parse_ppl_sources(&sub.query),
        _ => crate::api::parse_sql_sources(&sub.query),
    };
    let refs = match refs {
        Ok(r) if !r.is_empty() => r,
        _ => {
            return axum::Json(serde_json::json!({
                "fired": false,
                "error": "failed to parse query sources"
            }))
            .into_response()
        }
    };

    let (ds_id, table) = &refs[0];
    let connector = match state.registry.get(ds_id) {
        Some(c) => c,
        None => {
            return axum::Json(serde_json::json!({
                "fired": false,
                "error": format!("datasource '{}' not found", ds_id)
            }))
            .into_response()
        }
    };

    let sq = match crate::api::build_sub_query(&sub.query, &sub.format, table) {
        Ok(sq) => sq,
        Err(e) => {
            return axum::Json(serde_json::json!({"fired": false, "error": e})).into_response()
        }
    };

    let batches = match connector.execute(&sq).await {
        Ok(b) => b,
        Err(e) => {
            let now = crate::history::now_secs();
            state
                .webhook_registry
                .update_after_eval(&id, now, Some(e.to_string()));
            return axum::Json(serde_json::json!({
                "fired": false,
                "error": e.to_string()
            }))
            .into_response();
        }
    };

    let (columns, rows) = crate::api::batches_to_json(&batches);
    let now = crate::history::now_secs();

    if evaluate_condition(&sub.condition, &columns, &rows) {
        let sample_rows: Vec<Vec<serde_json::Value>> = rows.iter().take(10).cloned().collect();
        let payload = WebhookPayload {
            subscription_id: sub.id.clone(),
            subscription_name: sub.name.clone(),
            query: sub.query.clone(),
            row_count: rows.len() as u64,
            columns: columns.clone(),
            sample_rows,
            fired_at: now,
        };
        let delivery = deliver_webhook(&sub.callback_url, &payload).await;
        let error = delivery.err();
        state
            .webhook_registry
            .update_after_fire(&id, now, error.clone());
        axum::Json(serde_json::json!({
            "fired": true,
            "row_count": rows.len(),
            "delivery_error": error,
        }))
        .into_response()
    } else {
        state.webhook_registry.update_after_eval(&id, now, None);
        axum::Json(serde_json::json!({
            "fired": false,
            "reason": "condition not met",
            "row_count": rows.len(),
        }))
        .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_threshold_operators() {
        assert!(ThresholdOp::Gt.evaluate(10.0, 5.0));
        assert!(!ThresholdOp::Gt.evaluate(5.0, 10.0));
        assert!(ThresholdOp::Gte.evaluate(5.0, 5.0));
        assert!(ThresholdOp::Lt.evaluate(3.0, 5.0));
        assert!(ThresholdOp::Lte.evaluate(5.0, 5.0));
        assert!(ThresholdOp::Eq.evaluate(5.0, 5.0));
        assert!(!ThresholdOp::Eq.evaluate(5.1, 5.0));
    }

    #[test]
    fn test_evaluate_condition_rows_returned() {
        let cond = WebhookCondition::RowsReturned;
        let cols = vec!["a".to_string()];
        let rows = vec![vec![serde_json::json!(1)]];
        assert!(evaluate_condition(&cond, &cols, &rows));
        assert!(!evaluate_condition(&cond, &cols, &[]));
    }

    #[test]
    fn test_evaluate_condition_threshold() {
        let cond = WebhookCondition::Threshold {
            column: "count".to_string(),
            operator: ThresholdOp::Gt,
            value: 100.0,
        };
        let cols = vec!["name".to_string(), "count".to_string()];
        let rows = vec![vec![serde_json::json!("x"), serde_json::json!(150)]];
        assert!(evaluate_condition(&cond, &cols, &rows));

        let rows_low = vec![vec![serde_json::json!("x"), serde_json::json!(50)]];
        assert!(!evaluate_condition(&cond, &cols, &rows_low));
    }

    #[test]
    fn test_evaluate_condition_missing_column() {
        let cond = WebhookCondition::Threshold {
            column: "missing".to_string(),
            operator: ThresholdOp::Gt,
            value: 0.0,
        };
        let cols = vec!["a".to_string()];
        let rows = vec![vec![serde_json::json!(1)]];
        assert!(!evaluate_condition(&cond, &cols, &rows));
    }

    #[test]
    fn test_registry_crud() {
        let reg = WebhookRegistry::new();
        let sub = WebhookSubscription {
            id: String::new(),
            name: "test".into(),
            query: "SELECT 1".into(),
            format: "sql".into(),
            condition: WebhookCondition::RowsReturned,
            callback_url: "http://example.com/hook".into(),
            enabled: true,
            fire_count: 0,
            last_evaluated: None,
            last_error: None,
        };
        let id = reg.register(sub);
        assert!(id.starts_with("wh-"));
        assert!(reg.get(&id).is_some());
        assert_eq!(reg.list().len(), 1);
        assert_eq!(reg.enabled_subscriptions().len(), 1);
        assert!(reg.delete(&id));
        assert!(reg.get(&id).is_none());
    }

    #[test]
    fn test_registry_update_after_fire() {
        let reg = WebhookRegistry::new();
        let sub = WebhookSubscription {
            id: String::new(),
            name: "test".into(),
            query: "SELECT 1".into(),
            format: "sql".into(),
            condition: WebhookCondition::RowsReturned,
            callback_url: "http://example.com/hook".into(),
            enabled: true,
            fire_count: 0,
            last_evaluated: None,
            last_error: None,
        };
        let id = reg.register(sub);
        reg.update_after_fire(&id, 1000, None);
        let updated = reg.get(&id).unwrap();
        assert_eq!(updated.fire_count, 1);
        assert_eq!(updated.last_evaluated, Some(1000));
        assert!(updated.last_error.is_none());
    }

    #[test]
    fn test_webhook_payload_serialization() {
        let payload = WebhookPayload {
            subscription_id: "wh-1".into(),
            subscription_name: "test".into(),
            query: "SELECT 1".into(),
            row_count: 1,
            columns: vec!["a".into()],
            sample_rows: vec![vec![serde_json::json!(1)]],
            fired_at: 1000,
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("wh-1"));
        assert!(json.contains("webhook.fired").not());
        // Just verify it serializes without error
        assert!(!json.is_empty());
    }

    // Helper — .not() doesn't exist on bool, use this pattern
    trait Not {
        fn not(&self) -> bool;
    }
    impl Not for bool {
        fn not(&self) -> bool {
            !self
        }
    }
}
