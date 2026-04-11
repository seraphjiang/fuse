// SPDX-License-Identifier: Apache-2.0

//! Async query API — submit/poll pattern for long-running queries.
//!
//! - `POST /api/fuse/query/async` — submit query, returns `job_id` immediately
//! - `GET /api/fuse/query/async/{job_id}` — poll status and retrieve results
//! - `DELETE /api/fuse/query/async/{job_id}` — cancel/cleanup a job
//!
//! Jobs run in background tokio tasks. Results are held in memory with TTL.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Job status lifecycle: Pending → Running → Completed | Failed | Cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Async query job metadata and results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncJob {
    pub job_id: String,
    pub status: JobStatus,
    pub query: String,
    pub format: String,
    /// Identity of the user who submitted this job.
    #[serde(skip_serializing)]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub submitted_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
}

/// In-memory job store with TTL-based cleanup.
pub struct JobStore {
    jobs: Mutex<HashMap<String, AsyncJob>>,
    ttl: Duration,
    max_jobs: usize,
}

impl JobStore {
    pub fn new(ttl_secs: u64, max_jobs: usize) -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_secs),
            max_jobs,
        }
    }

    pub fn submit(&self, job_id: String, query: String, format: String) -> AsyncJob {
        let now = now_epoch();
        let job = AsyncJob {
            job_id: job_id.clone(),
            status: JobStatus::Pending,
            query,
            format,
            owner: None,
            result: None,
            error: None,
            submitted_at: now,
            completed_at: None,
        };
        let mut jobs = self.jobs.lock().unwrap();
        self.evict_expired(&mut jobs);
        if jobs.len() >= self.max_jobs {
            // Evict oldest completed job
            if let Some(key) = jobs.iter()
                .filter(|(_, j)| matches!(j.status, JobStatus::Completed | JobStatus::Failed))
                .min_by_key(|(_, j)| j.submitted_at)
                .map(|(k, _)| k.clone())
            {
                jobs.remove(&key);
            }
        }
        jobs.insert(job_id, job.clone());
        job
    }

    pub fn get(&self, job_id: &str) -> Option<AsyncJob> {
        self.jobs.lock().unwrap().get(job_id).cloned()
    }

    pub fn update_status(&self, job_id: &str, status: JobStatus) {
        if let Some(job) = self.jobs.lock().unwrap().get_mut(job_id) {
            job.status = status;
        }
    }

    pub fn complete(&self, job_id: &str, result: serde_json::Value) {
        if let Some(job) = self.jobs.lock().unwrap().get_mut(job_id) {
            job.status = JobStatus::Completed;
            job.result = Some(result);
            job.completed_at = Some(now_epoch());
        }
    }

    pub fn fail(&self, job_id: &str, error: String) {
        if let Some(job) = self.jobs.lock().unwrap().get_mut(job_id) {
            job.status = JobStatus::Failed;
            job.error = Some(error);
            job.completed_at = Some(now_epoch());
        }
    }

    pub fn cancel(&self, job_id: &str) -> bool {
        if let Some(job) = self.jobs.lock().unwrap().get_mut(job_id) {
            if matches!(job.status, JobStatus::Pending | JobStatus::Running) {
                job.status = JobStatus::Cancelled;
                job.completed_at = Some(now_epoch());
                return true;
            }
        }
        false
    }

    pub fn list(&self) -> Vec<AsyncJob> {
        self.jobs.lock().unwrap().values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.lock().unwrap().is_empty()
    }

    fn evict_expired(&self, jobs: &mut HashMap<String, AsyncJob>) {
        let cutoff = now_epoch().saturating_sub(self.ttl.as_secs());
        jobs.retain(|_, j| j.submitted_at > cutoff);
    }
}

fn now_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_job_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    // Mix timestamp + counter + pseudo-random bits to prevent enumeration
    let rand_bits = t.as_nanos() as u64 ^ (c.wrapping_mul(6364136223846793005));
    format!("job-{:016x}{:08x}", rand_bits, c as u32)
}

/// Submit request for async query.
#[derive(Debug, Deserialize)]
pub struct AsyncQueryRequest {
    pub query: String,
    #[serde(default = "default_format")]
    pub format: String,
}

fn default_format() -> String { "sql".into() }

/// Submit response with job_id for polling.
#[derive(Debug, Serialize)]
pub struct AsyncSubmitResponse {
    pub job_id: String,
    pub status: JobStatus,
    pub poll_url: String,
}

/// Create a new async job and return the submit response.
pub fn submit_async_query(store: &Arc<JobStore>, req: AsyncQueryRequest) -> AsyncSubmitResponse {
    let job_id = generate_job_id();
    let job = store.submit(job_id.clone(), req.query, req.format);
    AsyncSubmitResponse {
        job_id: job.job_id.clone(),
        status: job.status,
        poll_url: format!("/api/fuse/query/async/{}", job.job_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_submit_and_get() {
        let store = JobStore::new(300, 100);
        let job = store.submit("j1".into(), "SELECT 1".into(), "sql".into());
        assert_eq!(job.status, JobStatus::Pending);
        assert!(store.get("j1").is_some());
    }

    #[test]
    fn test_complete_job() {
        let store = JobStore::new(300, 100);
        store.submit("j1".into(), "SELECT 1".into(), "sql".into());
        store.update_status("j1", JobStatus::Running);
        store.complete("j1", serde_json::json!({"rows": []}));
        let job = store.get("j1").unwrap();
        assert_eq!(job.status, JobStatus::Completed);
        assert!(job.result.is_some());
        assert!(job.completed_at.is_some());
    }

    #[test]
    fn test_fail_job() {
        let store = JobStore::new(300, 100);
        store.submit("j1".into(), "bad query".into(), "sql".into());
        store.fail("j1", "parse error".into());
        let job = store.get("j1").unwrap();
        assert_eq!(job.status, JobStatus::Failed);
        assert_eq!(job.error.as_deref(), Some("parse error"));
    }

    #[test]
    fn test_cancel_job() {
        let store = JobStore::new(300, 100);
        store.submit("j1".into(), "SELECT 1".into(), "sql".into());
        assert!(store.cancel("j1"));
        assert_eq!(store.get("j1").unwrap().status, JobStatus::Cancelled);
    }

    #[test]
    fn test_cancel_completed_noop() {
        let store = JobStore::new(300, 100);
        store.submit("j1".into(), "SELECT 1".into(), "sql".into());
        store.complete("j1", serde_json::json!({}));
        assert!(!store.cancel("j1")); // can't cancel completed
        assert_eq!(store.get("j1").unwrap().status, JobStatus::Completed);
    }

    #[test]
    fn test_get_missing() {
        let store = JobStore::new(300, 100);
        assert!(store.get("nonexistent").is_none());
    }

    #[test]
    fn test_list_jobs() {
        let store = JobStore::new(300, 100);
        store.submit("j1".into(), "q1".into(), "sql".into());
        store.submit("j2".into(), "q2".into(), "ppl".into());
        assert_eq!(store.list().len(), 2);
    }

    #[test]
    fn test_eviction_at_capacity() {
        let store = JobStore::new(300, 2);
        store.submit("j1".into(), "q1".into(), "sql".into());
        store.complete("j1", serde_json::json!({}));
        store.submit("j2".into(), "q2".into(), "sql".into());
        store.submit("j3".into(), "q3".into(), "sql".into()); // should evict j1
        assert!(store.len() <= 2);
    }

    #[test]
    fn test_submit_async_query() {
        let store = Arc::new(JobStore::new(300, 100));
        let resp = submit_async_query(&store, AsyncQueryRequest {
            query: "SELECT * FROM t".into(),
            format: "sql".into(),
        });
        assert_eq!(resp.status, JobStatus::Pending);
        assert!(resp.job_id.starts_with("job-"));
        assert!(resp.poll_url.contains(&resp.job_id));
    }

    #[test]
    fn test_job_serialization() {
        let store = JobStore::new(300, 100);
        store.submit("j1".into(), "SELECT 1".into(), "sql".into());
        let job = store.get("j1").unwrap();
        let json = serde_json::to_string(&job).unwrap();
        assert!(json.contains("\"status\":\"pending\""));
        // result and error should be omitted when None
        assert!(!json.contains("\"result\""));
        assert!(!json.contains("\"error\""));
    }
}
