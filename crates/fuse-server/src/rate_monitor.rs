// SPDX-License-Identifier: Apache-2.0
//! Query rate monitor — sliding window QPS tracking.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Instant;

pub struct RateMonitor {
    timestamps: Mutex<VecDeque<Instant>>,
    window_secs: u64,
}

impl RateMonitor {
    pub fn new(window_secs: u64) -> Self {
        Self { timestamps: Mutex::new(VecDeque::new()), window_secs }
    }

    pub fn record(&self) {
        let now = Instant::now();
        let mut ts = self.timestamps.lock().unwrap();
        ts.push_back(now);
        let cutoff = std::time::Duration::from_secs(self.window_secs);
        while ts.front().map(|t| t.elapsed() > cutoff).unwrap_or(false) {
            ts.pop_front();
        }
    }

    /// Queries per second over the window.
    pub fn qps(&self) -> f64 {
        let ts = self.timestamps.lock().unwrap();
        if ts.is_empty() || self.window_secs == 0 { return 0.0; }
        let cutoff = std::time::Duration::from_secs(self.window_secs);
        let count = ts.iter().filter(|t| t.elapsed() <= cutoff).count();
        count as f64 / self.window_secs as f64
    }

    /// Total queries in the current window.
    pub fn count(&self) -> usize {
        self.timestamps.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let m = RateMonitor::new(60);
        assert_eq!(m.qps(), 0.0);
        assert_eq!(m.count(), 0);
    }

    #[test]
    fn test_record_and_count() {
        let m = RateMonitor::new(60);
        m.record();
        m.record();
        m.record();
        assert_eq!(m.count(), 3);
        assert!(m.qps() > 0.0);
    }

    #[test]
    fn test_qps_calculation() {
        let m = RateMonitor::new(10);
        for _ in 0..100 {
            m.record();
        }
        // 100 queries in 10s window = ~10 QPS
        assert!(m.qps() >= 9.0);
    }
}
