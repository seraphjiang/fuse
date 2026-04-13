// SPDX-License-Identifier: Apache-2.0
//! Feedback system — submit, list, reply, status tracking.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Feedback {
    pub id: String,
    pub submitter: String,
    #[serde(rename = "type")]
    pub feedback_type: String,
    pub title: String,
    pub description: String,
    pub status: FeedbackStatus,
    /// Base64 data URI screenshot, if provided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub screenshot: Option<String>,
    pub page: String,
    pub url: String,
    pub user_agent: String,
    pub page_context: String,
    #[serde(default)]
    pub console_errors: Vec<String>,
    pub created_at: u64,
    pub updated_at: u64,
    #[serde(default)]
    pub replies: Vec<Reply>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackStatus {
    Pending,
    Reviewed,
    Replied,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    pub author: String,
    pub message: String,
    pub created_at: u64,
}

#[derive(Debug, Deserialize)]
pub struct SubmitRequest {
    #[serde(rename = "type", default = "default_type")]
    pub feedback_type: String,
    pub title: String,
    pub description: String,
    pub screenshot: Option<String>,
    #[serde(default)]
    pub page: String,
    #[serde(default)]
    pub url: String,
    #[serde(default, rename = "userAgent")]
    pub user_agent: String,
    #[serde(default, rename = "pageContext")]
    pub page_context: String,
    #[serde(default, rename = "consoleErrors")]
    pub console_errors: Vec<serde_json::Value>,
}

fn default_type() -> String { "general".into() }

#[derive(Debug, Deserialize)]
pub struct ReplyRequest {
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct StatusRequest {
    pub status: FeedbackStatus,
}

pub struct FeedbackStore {
    entries: RwLock<HashMap<String, Feedback>>,
    max_entries: usize,
    counter: std::sync::atomic::AtomicU64,
}

impl FeedbackStore {
    pub fn new(max_entries: usize) -> Self {
        Self { entries: RwLock::new(HashMap::new()), max_entries, counter: std::sync::atomic::AtomicU64::new(0) }
    }

    pub fn submit(&self, submitter: &str, req: SubmitRequest) -> Feedback {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let seq = self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = format!("fb-{}-{}", now, seq);
        let fb = Feedback {
            id: id.clone(),
            submitter: submitter.to_string(),
            feedback_type: req.feedback_type,
            title: req.title,
            description: req.description,
            status: FeedbackStatus::Pending,
            screenshot: req.screenshot,
            page: req.page,
            url: req.url,
            user_agent: req.user_agent,
            page_context: req.page_context,
            console_errors: req.console_errors.into_iter().map(|e| e.to_string()).collect(),
            created_at: now,
            updated_at: now,
            replies: vec![],
        };
        let mut entries = self.entries.write().unwrap();
        // Evict oldest if at capacity
        if entries.len() >= self.max_entries {
            if let Some(oldest_id) = entries.values()
                .min_by_key(|f| f.created_at)
                .map(|f| f.id.clone())
            {
                entries.remove(&oldest_id);
            }
        }
        entries.insert(id, fb.clone());
        fb
    }

    /// List feedback for a specific user (sorted newest first).
    pub fn list_by_user(&self, user: &str) -> Vec<Feedback> {
        let entries = self.entries.read().unwrap();
        let mut list: Vec<_> = entries.values()
            .filter(|f| f.submitter == user)
            .cloned()
            .collect();
        list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        list
    }

    /// List all feedback (admin only, sorted newest first).
    pub fn list_all(&self) -> Vec<Feedback> {
        let entries = self.entries.read().unwrap();
        let mut list: Vec<_> = entries.values().cloned().collect();
        list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        list
    }

    pub fn get(&self, id: &str) -> Option<Feedback> {
        self.entries.read().unwrap().get(id).cloned()
    }

    pub fn add_reply(&self, id: &str, author: &str, message: String) -> Option<Feedback> {
        let mut entries = self.entries.write().unwrap();
        let fb = entries.get_mut(id)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        fb.replies.push(Reply { author: author.to_string(), message, created_at: now });
        fb.updated_at = now;
        if fb.status == FeedbackStatus::Pending {
            fb.status = FeedbackStatus::Replied;
        }
        Some(fb.clone())
    }

    pub fn set_status(&self, id: &str, status: FeedbackStatus) -> Option<Feedback> {
        let mut entries = self.entries.write().unwrap();
        let fb = entries.get_mut(id)?;
        fb.status = status;
        fb.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Some(fb.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_submit_and_list() {
        let store = FeedbackStore::new(100);
        store.submit("alice", SubmitRequest {
            feedback_type: "bug".into(), title: "broken".into(), description: "it broke".into(),
            screenshot: Some("data:image/png;base64,abc".into()),
            page: "/".into(), url: "http://localhost".into(),
            user_agent: "test".into(), page_context: "".into(), console_errors: vec![],
        });
        store.submit("bob", SubmitRequest {
            feedback_type: "feature".into(), title: "want X".into(), description: "please".into(),
            screenshot: None, page: "/".into(), url: "http://localhost".into(),
            user_agent: "test".into(), page_context: "".into(), console_errors: vec![],
        });
        assert_eq!(store.list_by_user("alice").len(), 1);
        assert_eq!(store.list_by_user("bob").len(), 1);
        assert_eq!(store.list_all().len(), 2);
    }

    #[test]
    fn test_reply_and_status() {
        let store = FeedbackStore::new(100);
        let fb = store.submit("alice", SubmitRequest {
            feedback_type: "bug".into(), title: "t".into(), description: "d".into(),
            screenshot: None, page: "".into(), url: "".into(),
            user_agent: "".into(), page_context: "".into(), console_errors: vec![],
        });
        assert_eq!(fb.status, FeedbackStatus::Pending);
        let fb = store.add_reply(&fb.id, "admin", "looking into it".into()).unwrap();
        assert_eq!(fb.status, FeedbackStatus::Replied);
        assert_eq!(fb.replies.len(), 1);
        let fb = store.set_status(&fb.id, FeedbackStatus::Closed).unwrap();
        assert_eq!(fb.status, FeedbackStatus::Closed);
    }

    #[test]
    fn test_screenshot_preserved() {
        let store = FeedbackStore::new(100);
        let fb = store.submit("u", SubmitRequest {
            feedback_type: "bug".into(), title: "t".into(), description: "d".into(),
            screenshot: Some("data:image/jpeg;base64,/9j/4AAQ".into()),
            page: "".into(), url: "".into(),
            user_agent: "".into(), page_context: "".into(), console_errors: vec![],
        });
        let got = store.get(&fb.id).unwrap();
        assert!(got.screenshot.unwrap().starts_with("data:image"));
    }

    #[test]
    fn test_eviction() {
        let store = FeedbackStore::new(2);
        store.submit("a", SubmitRequest { feedback_type: "bug".into(), title: "1".into(), description: "".into(), screenshot: None, page: "".into(), url: "".into(), user_agent: "".into(), page_context: "".into(), console_errors: vec![] });
        store.submit("a", SubmitRequest { feedback_type: "bug".into(), title: "2".into(), description: "".into(), screenshot: None, page: "".into(), url: "".into(), user_agent: "".into(), page_context: "".into(), console_errors: vec![] });
        store.submit("a", SubmitRequest { feedback_type: "bug".into(), title: "3".into(), description: "".into(), screenshot: None, page: "".into(), url: "".into(), user_agent: "".into(), page_context: "".into(), console_errors: vec![] });
        assert_eq!(store.list_all().len(), 2);
    }
}
