// SPDX-License-Identifier: Apache-2.0
//! Query notifications — notify when queries complete.

use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::mpsc;

/// Notification for a completed query.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryNotification {
    pub query_id: String,
    pub success: bool,
    pub duration_ms: u64,
    pub row_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Manages notification subscriptions.
pub struct NotificationHub {
    subscribers: Mutex<HashMap<String, mpsc::Sender<QueryNotification>>>,
}

impl Default for NotificationHub {
    fn default() -> Self {
        Self::new()
    }
}

impl NotificationHub {
    pub fn new() -> Self {
        Self {
            subscribers: Mutex::new(HashMap::new()),
        }
    }

    /// Subscribe to notifications for a query.
    pub fn subscribe(&self, query_id: &str) -> mpsc::Receiver<QueryNotification> {
        let (tx, rx) = mpsc::channel(1);
        self.subscribers
            .lock()
            .unwrap()
            .insert(query_id.to_string(), tx);
        rx
    }

    /// Notify subscribers that a query completed.
    pub fn notify(&self, notification: QueryNotification) {
        let mut subs = self.subscribers.lock().unwrap();
        if let Some(tx) = subs.remove(&notification.query_id) {
            let _ = tx.try_send(notification);
        }
    }

    pub fn pending_count(&self) -> usize {
        self.subscribers.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscribe_and_notify() {
        let hub = NotificationHub::new();
        let mut rx = hub.subscribe("q-1");
        hub.notify(QueryNotification {
            query_id: "q-1".into(),
            success: true,
            duration_ms: 50,
            row_count: 10,
            error: None,
        });
        let n = rx.recv().await.unwrap();
        assert!(n.success);
        assert_eq!(n.row_count, 10);
    }

    #[test]
    fn test_pending_count() {
        let hub = NotificationHub::new();
        let _rx1 = hub.subscribe("q-1");
        let _rx2 = hub.subscribe("q-2");
        assert_eq!(hub.pending_count(), 2);
    }

    #[tokio::test]
    async fn test_notify_removes_subscriber() {
        let hub = NotificationHub::new();
        let _rx = hub.subscribe("q-1");
        hub.notify(QueryNotification {
            query_id: "q-1".into(),
            success: false,
            duration_ms: 0,
            row_count: 0,
            error: Some("timeout".into()),
        });
        assert_eq!(hub.pending_count(), 0);
    }
}
